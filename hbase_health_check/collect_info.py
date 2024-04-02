from hbase_health_check.models import *
import requests
import boto3
import orjson
import gzip
import hbase_health_check.configuration as configuration

emr_client = boto3.client('emr')


def get_all_cluster_id():
    marker = None
    clusters = []
    while True:
        # Include the marker in the request if it exists.
        if marker:
            response = emr_client.list_clusters(ClusterStates=['RUNNING', 'WAITING'], Marker=marker)
        else:
            response = emr_client.list_clusters(ClusterStates=['RUNNING', 'WAITING'])

        # Process the current page of clusters.
        for cluster in response['Clusters']:
            clusters.append(cluster['Id'])

        # If there's no next marker, exit the loop.
        if 'Marker' not in response:
            break

        # Update the marker with the next marker.
        marker = response['Marker']
    return clusters


def get_jmx(dns_name, port):
    url = f'http://{dns_name}:{port}/jmx'
    print(f'Retrieving JMX from {url}')
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            print(f'Retrieved JMX from {url}')
            return response.json()['beans']
            # print(data)
        else:
            print(f"Failed to retrieve data: {response.status_code}")
    except requests.RequestException as e:
        print(f"An error occurred: {e}")


def get_clusters(clusters: str):
    hbase_clusters = []
    if clusters is None:
        clusters = get_all_cluster_id()
    else:
        clusters = clusters.split(',')
    for cluster_id in clusters:
        cluster_description = emr_client.describe_cluster(ClusterId=cluster_id)
        applications = cluster_description['Cluster']['Applications']
        hbase_installed = any(app['Name'] == 'HBase' for app in applications)
        if hbase_installed:
            dns_name = cluster_description['Cluster']['MasterPublicDnsName']
            jmx = get_jmx(dns_name, 16010)
            live_region_servers = []
            for bean in jmx:
                if bean['name'] == 'Hadoop:service=HBase,name=Master,sub=Server':
                    servers = bean['tag.liveRegionServers']
                    for server in servers.split(';'):
                        live_region_servers.append(server.split(',')[0])
            cluster = HBaseCluster(
                cluster_id=cluster_id,
                # cluster_description=cluster_description,
                master_dns_name=dns_name,
                configurations=cluster_description['Cluster']['Configurations'],
                region_servers=[],
                live_region_servers=live_region_servers
            )
            hbase_clusters.append(cluster)
    return hbase_clusters


def bytes_to_gb(size_bytes: int) -> float:
    gb = size_bytes / 1024 / 1024 / 1024
    # return f"{gb:.2f}GB"
    return gb


def get_regions(bean):
    regions = {}
    for key, value in bean.items():
        keys = key.split('_')
        if len(keys) != 8:
            continue
        table_name = f'{keys[1]}:{keys[3]}'
        region_name = keys[5]
        region = regions.get(region_name, Region(name=region_name, table_name=table_name))
        if keys[-1] == 'storeFileSize':
            region.store_file_size_gb = bytes_to_gb(value)
        regions[region_name] = region
    return list(regions.values())


def get_tables(bean):
    tables = {}
    for key, value in bean.items():
        keys = key.split('_')
        if len(keys) != 6:
            continue
        table_name = f'{keys[1]}:{keys[3]}'
        table = tables.get(table_name, Table(name=table_name))
        if keys[-1] == 'totalRequestCount':
            table.total_requests = value
        elif keys[-1] == 'storeCount':
            table.column_families = value
        tables[table_name] = table
    return list(tables.values())


def get_region_servers(cluster: HBaseCluster):
    region_servers = []
    paginator = emr_client.get_paginator('list_instances')
    response_iterator = paginator.paginate(
        ClusterId=cluster.cluster_id,
        # InstanceFleetType='CORE',
        InstanceStates=['RUNNING'],
        PaginationConfig={
            'MaxItems': 100
        }
    )
    for page in response_iterator:
        for instance in page['Instances']:
            if instance['PrivateDnsName'] not in cluster.live_region_servers:
                print(f'Instance {instance['Ec2InstanceId']} is not a live region server')
                continue
            dns_name = instance['PublicDnsName'] or instance['PrivateDnsName']
            jmx = get_jmx(dns_name, 16030)
            if jmx is None:
                continue
            server_bean = None
            table_bean = None
            region_bean = None
            jvm_bean = None
            for bean in jmx:
                if bean['name'] == 'Hadoop:service=HBase,name=RegionServer,sub=Regions':
                    region_bean = bean
                elif bean['name'] == 'Hadoop:service=HBase,name=RegionServer,sub=Server':
                    server_bean = bean
                elif bean['name'] == 'Hadoop:service=HBase,name=RegionServer,sub=Tables':
                    table_bean = bean
                elif bean['name'] == 'Hadoop:service=HBase,name=JvmMetrics':
                    jvm_bean = bean
            region_server = RegionServer(
                instance_id=instance['Ec2InstanceId'],
                instance_dns_name=instance['PublicDnsName'] or instance['PrivateDnsName'],
                region_num=server_bean['regionCount'],
                total_requests=server_bean['totalRequestCount'],
                store_files=server_bean['storeFileCount'],
                tables=get_tables(table_bean),
                regions=get_regions(region_bean),
                l1_hit_ratio=server_bean['l1CacheHitRatio'],
                l2_hit_ratio=server_bean['l2CacheHitRatio'],
                split_num=server_bean['splitRequestCount'],
                gc_num=jvm_bean['GcCount'],
                gc_time=f'{(jvm_bean['GcTimeMillis'] / 1000):2f}s'
            )
            region_servers.append(region_server)
    return region_servers


def collect(clusters: str):
    clusters = get_clusters(clusters)
    for cluster in clusters:
        cluster.region_servers = get_region_servers(cluster)
    clusters = [cluster for cluster in clusters if len(cluster.region_servers) > 0]

    output_data = orjson.dumps(clusters)
    # write to file
    with open(configuration.txt_file_path, 'w') as f:
        f.write(output_data.decode('utf8'))
    with gzip.open(configuration.gz_file_path, 'wb') as f:
        f.write(output_data)
