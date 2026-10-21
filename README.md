# Procesamiento Batch con PySpark - Tarea 3
**Curso:** Big Data  
**Autor:** vboxuser  
**Fecha:** Octubre 2026  

---

## 📌 Descripción del Proyecto
Este repositorio contiene la solución formal para la **Tarea 3**, enfocada en el diseño e implementación de un script de procesamiento de datos en lote (Batch) utilizando **PySpark**. El objetivo principal es procesar y transformar un conjunto de datos histórico de la Tasa de Cambio Representativa del Mercado (TRM) para extraer métricas clave de rendimiento financiero.

## 🛠️ Tecnologías y Herramientas Utilizadas
* **Apache Spark v3.x** (Core de procesamiento distribuido)
* **PySpark** (API de Python para Spark)
* **Python 3.x** (Lenguaje de programación)
* **Git & GitHub** (Control de versiones y despliegue)

## 📂 Estructura del Repositorio
```text
bigdata-spark-batch-tarea3/
├── README.md               <-- Documentación principal de la solución
├── tarea3.py               <-- Código fuente optimizado de PySpark Batch
├── data/
│   └── rows_sample.csv     <-- Muestra del dataset (Histórico TRM)
└── .gitignore              <-- Archivos excluidos del control de versiones
```

## ⚙️ Requisitos Previos e Ingesta
Para ejecutar este script en tu entorno local o máquina virtual, asegúrate de contar con Apache Spark configurado y las dependencias de Python listas.

El flujo de procesamiento implementado en `tarea3.py` realiza la carga automatizada del archivo local `data/rows_sample.csv`, la limpieza de tipos de datos, ordenamiento y agregaciones analíticas batch requeridas por la guía.
