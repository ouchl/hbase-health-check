import click
from hbase_health_check.collect_info import collect
from hbase_health_check.report import report_health_check


@click.group()
def cli():
    pass


@click.command()
@click.option('--clusters', default=None, help='EMR clusters')
def collect_info(clusters):
    collect(clusters)


@click.command()
def report():
    report_health_check()


cli.add_command(collect_info, 'collect-info')
cli.add_command(report, 'report')

if __name__ == '__main__':
    cli()
