from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, to_date, mean, min, max, lit
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
    """
    Función encargada leer los lotes batch, uno a uno
    :param list_batch: recibe como parámetro la lista de los lotes batch
    :return: un diccionario con la key el nombre del lote y el valor el df
    """
    dfs = {}

    for name_batch in list_batch:
        dfs[name_batch] = (spark.read
                           .option('header', True)
                           .option('delimiter', ',')
                           .option('inferSchema', True).csv(f'./data/{name_batch}.csv'))
        logger.info(f'Lectura completa del lote batch: {name_batch}')

    for name_batch, df in dfs.items():
        # Lista de expresiones para convertir a minúscula
        lowers_expr = [col(column).alias(column.lower()) for column in df.columns]
        # Aplicar las expresiones y asignar el resultado al DataFrame original.
        dfs[name_batch] = df.select(*lowers_expr)

    return dfs


def transformations(dfs):
    """
    Función encargada de castear la fecha timestamp a tipo date, ya que no utiliza un formato explícito
    :param dfs: recibe como parámetro el diccionario de la lectura dinámica
    :return: el diccionario con los campos timestamp convertidos a tipo date
    """
    for batch_name in dfs.keys():
        dfs[batch_name] = dfs[batch_name].withColumn("timestamp", to_date(col("timestamp"), "M/d/yyyy"))
    return dfs


def write_data(dfs):
    """
    Función que permite escribir los datos en la base de datos postgreSQL
    También calcula el valor medio, minimo y máximo, cantidad de registros
    :param dfs: recibe como parámetro el diccionario de las transformaciones
    :return:
    """
    # Escribe el DataFrame en PostgreSQL
    sumatoria = 0

    for batch_name, df_batch in dfs.items():
        if batch_name == 'validation':
            df_batch.write.jdbc(url=postgres_url, table="my_user_validation", mode="append", properties=properties)

        else:
            df_batch.write.jdbc(url=postgres_url, table="my_user", mode="append", properties=properties)

        count_df = df_batch.count()
        if batch_name != 'validation':
            sumatoria += count_df
        statistics = df_batch.agg(
            lit(batch_name).alias('nombre_lote_batch'),
            lit(count_df).alias('conteo_registros'),
            mean("price").alias("valor_medio"),
            min("price").alias("valor_minimo"),
            max("price").alias("valor_maximo")
        )
        statistics.show(truncate=False)
    print(f' TOTAL REGISTROS BATCH: {sumatoria}')


if __name__ == "__main__":
    # 1. paso
    dfs_batch = dynamic_read_batch(['2012-1', '2012-2', '2012-3', '2012-4', '2012-5', 'validation'])
    # 2. paso
    dfs_transformations = transformations(dfs_batch)
    # 3. paso
    write_data(dfs_transformations)
