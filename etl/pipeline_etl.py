from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
import logging

spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
    .getOrCreate()

# Configuración básica del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def dynamic_read_batch(list_batch):
    dfs = {}

    for name_batch in list_batch:
        dfs[name_batch] = (spark.read
                           .option('header', True)
                           .option('delimiter', ',')
                           .option('inferSchema', True).csv(f'./data/{name_batch}.csv'))

    for name_batch, df in dfs.items():
        # Lista de expresiones para convertir a minúscula
        lowers_expr = [col(column).alias(column.lower()) for column in df.columns]
        # Aplicar las expresiones y asignar el resultado al DataFrame original.
        dfs[name_batch] = df.select(*lowers_expr)

    return dfs


dfs_batch = dynamic_read_batch(['2012-1', '2012-2', '2012-3', '2012-4', '2012-5'])

sumatoria = 0
for batch_name in dfs_batch.keys():
    sumatoria += dfs_batch[batch_name].count()
    print(f'Lote Batch: {batch_name} conteo {dfs_batch[batch_name].count()}')
print(f'TOTAL REGISTROS BATCH: {sumatoria}')
