from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.functions import unix_timestamp, from_unixtime
import logging

spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

# Configuración básica del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configura la información de la base de datos PostgreSQL
postgres_url = "jdbc:postgresql://localhost:5432/db_batch"
properties = {
    "user": "postgres",
    "password": "123456",
    "driver": "org.postgresql.Driver"
}


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


def transformations(dfs):
    for batch_name in dfs.keys():
        dfs[batch_name] = dfs[batch_name].withColumn("timestamp", to_date(col("timestamp"), "M/d/yyyy"))
    return dfs


def write_data(dfs):
    # Escribe el DataFrame en PostgreSQL
    for df_batch in dfs.values():
        df_batch.write.jdbc(url=postgres_url, table="my_user", mode="append", properties=properties)


dfs_batch = dynamic_read_batch(['2012-1', '2012-2', '2012-3', '2012-4', '2012-5'])
dfs_transformations = transformations(dfs_batch)

sumatoria = 0
for batch_name in dfs_transformations.keys():
    sumatoria += dfs_transformations[batch_name].count()
    print(f'Lote Batch: {batch_name} conteo {dfs_transformations[batch_name].count()}')
print(f'TOTAL REGISTROS BATCH: {sumatoria}')

dfs_batch['2012-1'].groupby('timestamp').count().orderBy('count', ascending=False).show(5, truncate=False)

write_data(dfs_transformations)
