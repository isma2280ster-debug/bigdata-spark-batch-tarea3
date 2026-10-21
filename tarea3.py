#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de Procesamiento Batch en Apache Spark - Tarea 3 Big Data (UNAD)
Descripción: Carga, exploración, filtrado y ordenamiento eficiente de un dataset 
             de TRM desde HDFS utilizando buenas prácticas de Spark, manejo de 
             excepciones, caché y documentación formal.
Autor: Estudiante UNAD
"""

import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    # 1. Inicialización de la SparkSession con configuración optimizada
    spark = SparkSession.builder \
        .appName("AnalisisBatch_TRM_Optimizado") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    # Reducir el nivel de logs a WARN para mantener la consola limpia y profesional
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 60)
    print(" === 1. INICIALIZACIÓN EXITOSA DE LA SESIÓN DE SPARK ===")
    print("=" * 60)

    # Definición de la ruta del archivo en HDFS
    file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'
    
    try:
        # 2. Carga eficiente del archivo CSV con inferencia automática de esquema y cabecera
        print(f"\n[INFO] Cargando el conjunto de datos desde HDFS: {file_path}")
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file_path)
        
        # OPTIMIZACIÓN CLAVE: Caché en memoria para acelerar múltiples consultas sobre el mismo DataFrame
        df.cache()
        
        # 3. Inspección estructural del esquema de datos
        print("\n" + "=" * 60)
        print(" === 2. ESQUEMA DEL DATAFRAME (Data Types) ===")
        print("=" * 60)
        df.printSchema()
        
        # 4. Visualización de los primeros registros
        print("\n" + "=" * 60)
        print(" === 3. MUESTRA DE DATOS (Primeros 20 registros) ===")
        print("=" * 60)
        df.show(20, truncate=False)
        
        # 5. Resumen estadístico descriptivo avanzado
        print("\n" + "=" * 60)
        print(" === 4. RESUMEN ESTADÍSTICO (Media, Min, Max, Cuartiles) ===")
        print("=" * 60)
        df.summary().show()
        
        # 6. Consulta analítica 1: Filtrado estructurado con funciones de PySpark (F.col)
        print("\n" + "=" * 60)
        print(" === 5. ANÁLISIS: DÍAS CON VALOR DE TRM SUPERIOR A 5000 ===")
        print("=" * 60)
        dias_filtrados = df.filter(F.col('VALOR') > 5000) \
                           .select('VALOR', 'VIGENCIADESDE', 'VIGENCIAHASTA')
        
        print(f"Total de registros encontrados con valor mayor a 5000: {dias_filtrados.count()}")
        dias_filtrados.show(10, truncate=False)
        
        # 7. Consulta analítica 2: Ordenamiento descendente para identificar los valores históricos más altos
        print("\n" + "=" * 60)
        print(" === 6. ANÁLISIS: TOP VALORES HISTÓRICOS DE TRM (Orden Descendente) ===")
        print("=" * 60)
        df.orderBy(F.col("VALOR").desc()) \
          .select('VALOR', 'VIGENCIADESDE', 'VIGENCIAHASTA') \
          .show(10, truncate=False)

    except Exception as e:
        print(f"\n[ERROR CRÍTICO] Ocurrió una excepción durante la ejecución en Spark: {str(e)}")
        sys.exit(1)
        
    finally:
        # 8. Cierre ordenado de la sesión de Spark para liberar recursos de memoria y CPU
        spark.stop()
        print("\n" + "=" * 60)
        print(" === 7. SESIÓN DE SPARK FINALIZADA Y RECURSOS LIBERADOS ===")
        print("=" * 60)

if __name__ == "__main__":
    main()
