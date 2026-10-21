# Tarea 3: Procesamiento de Datos en Lote (Batch) con PySpark y HDFS

**Curso:** Big Data (Código 202016911_60)  
**Institución:** Universidad Nacional Abierta y a Distancia (UNAD)  
**Escuela:** Escuela de Ciencias Básicas, Tecnología e Ingeniería (ECBTI)  
**Estudiante:** Ismael Antonio Ochoa Villalba  
**Tutor:** Javier Antonio Ballesteros Ricaurte  
**Fecha:** Octubre 2026  

---

## 📌 Descripción del Proyecto

Este repositorio contiene la solución técnica formal para el procesamiento analítico distribuido en lote (**Batch Processing**) utilizando **Apache Spark (PySpark)** e **Hadoop HDFS**. 

El objetivo principal es procesar el conjunto de datos de la **Tasa de Cambio Representativa del Mercado (TRM)** en Colombia, realizando transformaciones distribuidas en memoria para:
- Inferencia automática de esquemas relacionales (`StructType`).
- Optimización mediante persistencia/caching de DataFrames en memoria RAM.
- Generación de resúmenes estadísticos descriptivos (`summary()`).
- Filtrado condicional de umbrales financieros (días con TRM superior a **\$5,000 COP**).
- Ordenamiento descendente de máximos históricos.

---

## 🛠️ Tecnologías y Herramientas Utilizadas

- **Apache Spark v3.x:** Engine de procesamiento distribuido in-memory.
- **PySpark:** API de Python para la orquestación de Jobs en Spark.
- **Hadoop HDFS v3.3.6:** Sistema de archivos distribuido para almacenamiento masivo.
- **Ubuntu Server 22.04 LTS:** Entorno de ejecución sobre VirtualBox.
- **Python 3.10+ & Git/GitHub:** Lenguaje base y control de versiones.

---

## 📂 Estructura del Repositorio

```text
bigdata-spark-batch-tarea3/
├── README.md               <-- Documentación principal y guía de ejecución
├── tarea3.py               <-- Script principal de PySpark Batch
├── data/
│   └── rows_sample.csv     <-- Dataset base (Serie histórica TRM Colombia)
└── .gitignore              <-- Archivos excluidos del control de versiones
```

---

## ⚙️ Ingesta de Datos e Instrucciones de Ejecución

### 1. Iniciar el Clúster Hadoop y Verificar HDFS
Asegúrate de que los demonios principales de Hadoop estén activos:
```bash
start-all.sh
jps
```
*Se debe verificar en consola la presencia de `NameNode`, `DataNode`, `ResourceManager` y `NodeManager`.*

### 2. Cargar el Dataset en HDFS
Crea la estructura de directorios en el almacenamiento distribuido y sube el archivo de la TRM:
```bash
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put data/rows_sample.csv /Tarea3/
hdfs dfs -ls /Tarea3
```

### 3. Ejecutar el Script de PySpark
Ejecuta el procesamiento en lote conectando Spark con HDFS:
```bash
python3 tarea3.py
```
*O mediante el gestor de trabajos:*
```bash
spark-submit --master local[*] tarea3.py
```

---

## 📊 Monitoreo y Auditoría

Durante y después de la ejecución, se puede auditar el rendimiento del clúster a través de las interfaces web:
- **Spark Web UI:** `http://<IP_MÁQUINA_VIRTUAL>:4040` (Inspección de *Jobs*, *DAG Visualization*, *Stages* y *Executors*).
- **Hadoop NameNode UI:** `http://<IP_MÁQUINA_VIRTUAL>:9870` (Verificación de bloques de almacenamiento y réplicas en `/Tarea3`).

---

## 📄 Licencia y Uso Académico
Desarrollado con fines estrictamente académicos para la evaluación de la **Tarea 3** del curso de Big Data de la UNAD.
