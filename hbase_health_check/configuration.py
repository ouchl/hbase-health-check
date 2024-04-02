from pathlib import Path

output_dir = 'output'
gz_file_path = Path(output_dir) / Path('clusters.json.gz')
txt_file_path = Path(output_dir) / Path('clusters.txt')
report_dir = Path('report')
LARGE_REGION_SIZE = 10
COLUMN_FAMILY_THRESHOLD = 3
SKEWNESS_THRESHOLD = 2
CONFIGURATIONS = {'hbase.server.scanner.max.result.size': '',
                  'hbase.hregion.memstore.flush.size': '',
                  'hbase.regionserver.global.memstore.size': '0.7',
                  'hbase.regionserver.handler.count': '100',
                  'hbase.ipc.server.num.callqueue': '',
                  'hbase.hregion.max.filesize': '20GB',
                  'hbase.hstore.compaction.min': '5',
                  'hbase.hstro.blockingStoreFiles': '20',
                  'hbase.hstore.flusher.count': '4',
                  'hfile.block.cache.size': '0.1',
                  'HBASE_HEAPSIZE': ''}
