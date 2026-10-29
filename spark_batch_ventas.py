"""
spark_batch_ventas.py

Lee un CSV de ventas, limpia los datos y genera resultados usando PySpark.

Ejecucion local:
    spark-submit spark_batch_ventas.py ventas.csv resultados_spark

Si el CSV esta en HDFS, se puede indicar su ruta:
    spark-submit spark_batch_ventas.py hdfs:///datos/ventas.csv hdfs:///resultados/ventas

Requisito:
    Tener Apache Spark instalado y PySpark disponible en el entorno.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    round as spark_round,
    sum as spark_sum,
    trim,
    to_date,
)


def main():
    archivo_entrada = sys.argv[1] if len(sys.argv) > 1 else "ventas.csv"
    carpeta_salida = sys.argv[2] if len(sys.argv) > 2 else "resultados_spark"

    spark = (
        SparkSession.builder
        .appName("ProcesamientoBatchVentas")
        .getOrCreate()
    )

    try:
        print("Leyendo archivo de ventas...")
        ventas = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(archivo_entrada)
        )

        print("Columnas recibidas:")
        ventas.printSchema()

        # Se limpian espacios y se convierten los campos numericos.
        ventas_limpias = (
            ventas
            .withColumn("producto", trim(col("producto")))
            .withColumn("categoria", trim(col("categoria")))
            .withColumn("ciudad", trim(col("ciudad")))
            .withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))
            .withColumn("cantidad", col("cantidad").cast("integer"))
            .withColumn("precio_unitario", col("precio_unitario").cast("double"))
            .withColumn("total", col("total").cast("double"))
            .filter(
                col("id_venta").isNotNull()
                & col("fecha").isNotNull()
                & col("producto").isNotNull()
                & col("cantidad").isNotNull()
                & col("precio_unitario").isNotNull()
                & col("total").isNotNull()
            )
            .filter(
                (col("cantidad") > 0)
                & (col("precio_unitario") >= 0)
                & (col("total") >= 0)
            )
            .dropDuplicates(["id_venta"])
        )

        # Se calcula nuevamente el total para validar el valor de cada venta.
        ventas_limpias = ventas_limpias.withColumn(
            "total_calculado",
            spark_round(col("cantidad") * col("precio_unitario"), 2)
        )

        print("Muestra de ventas limpias:")
        ventas_limpias.show(10, truncate=False)

        print("Resumen de ventas por producto:")
        resumen_producto = (
            ventas_limpias
            .groupBy("producto", "categoria")
            .agg(
                count("*").alias("numero_ventas"),
                spark_sum("cantidad").alias("unidades_vendidas"),
                spark_round(spark_sum("total_calculado"), 2).alias("total_vendido"),
            )
            .orderBy(col("total_vendido").desc())
        )
        resumen_producto.show(truncate=False)

        print("Resumen de ventas por ciudad:")
        resumen_ciudad = (
            ventas_limpias
            .groupBy("ciudad")
            .agg(
                count("*").alias("numero_ventas"),
                spark_round(spark_sum("total_calculado"), 2).alias("total_vendido"),
            )
            .orderBy(col("total_vendido").desc())
        )
        resumen_ciudad.show(truncate=False)

        # Se guardan los resultados en formato CSV.
        resumen_producto.write.mode("overwrite").option("header", True).csv(
            f"{carpeta_salida}/por_producto"
        )
        resumen_ciudad.write.mode("overwrite").option("header", True).csv(
            f"{carpeta_salida}/por_ciudad"
        )

        print(f"Proceso terminado. Resultados guardados en: {carpeta_salida}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
