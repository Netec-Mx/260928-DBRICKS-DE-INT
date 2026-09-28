# Creación de workflow multi-tarea

## Metadatos

| Campo | Valor |
|---|---|
| Duración | 50 minutos |
| Complejidad | Media |
| Nivel de Bloom | Aplicar |

## Descripción General

En esta práctica se construirá un workflow reutilizable de Databricks Workflows para orquestar una carga de pedidos mediante cuatro tareas Notebook: ingesta, transformación, validación y publicación de métricas. El Job recibirá parámetros, aplicará dependencias explícitas mediante un DAG, transferirá métricas entre tareas mediante task values y registrará evidencia de la ejecución en una tabla Delta de auditoría.

La práctica se centra en ejecuciones manuales controladas. No se configurará una programación recurrente, aunque el Job resultante podrá reutilizarse posteriormente como base para una ejecución programada.

## Objetivos de Aprendizaje

- [ ] Crear cuatro notebooks modulares para ingesta, transformación, validación y auditoría.
- [ ] Crear un Job de Databricks con un DAG explícito: Ingesta → Transformación → Validación → Publicación.
- [ ] Configurar y consumir los parámetros `run_date` y `source_file`.
- [ ] Configurar timeout, reintento, notificación de fallo y cómputo de Job.
- [ ] Interpretar el grafo, los task values, los logs y las tablas generadas por una ejecución exitosa.

## Prerrequisitos

**Conocimientos requeridos**

- Uso básico de notebooks de Databricks con Python y SQL.
- Creación y consulta de tablas Delta con Unity Catalog.
- Conceptos de Job, Task, Run, dependencias y parámetros.
- Comprensión básica de archivos CSV y DataFrames de Spark.

**Accesos requeridos**

- Acceso al workspace de Databricks con Unity Catalog habilitado.
- Permisos `USE CATALOG` sobre `de_training`.
- Permisos `USE SCHEMA`, `CREATE TABLE` y `MODIFY` sobre `de_training.batch1`.
- Acceso de lectura y escritura al volumen `de_training.batch1.landing`.
- Permiso para crear, editar y ejecutar Jobs.
- Dirección de correo institucional del estudiante o equipo para notificaciones de fallo.
- Disponibilidad de Databricks Runtime 15.4 LTS para Job Compute.

## Entorno de Laboratorio

| Componente | Versión o configuración exacta | Fuente oficial |
|---|---|---|
| Databricks Runtime | 15.4 LTS, edición estándar con Photon opcional | <https://docs.databricks.com/en/release-notes/runtime/15.4lts.html> |
| Apache Spark | 3.5.0, incluido en Databricks Runtime 15.4 LTS | <https://spark.apache.org/releases/spark-release-3-5-0.html> |
| Python | 3.11.0, incluido en Databricks Runtime 15.4 LTS | <https://www.python.org/downloads/release/python-3110/> |
| Delta Lake | 3.2.0, incluido en Databricks Runtime 15.4 LTS | <https://github.com/delta-io/delta/releases/tag/v3.2.0> |
| Databricks Workflows | Servicio SaaS compatible con Databricks Runtime 15.4 LTS; sin versión independiente publicada | <https://docs.databricks.com/en/workflows/index.html> |
| Unity Catalog | Servicio SaaS compatible con Databricks Runtime 15.4 LTS; sin versión independiente publicada | <https://docs.databricks.com/en/data-governance/unity-catalog/index.html> |
| Cómputo del Job | Job Compute, 1 driver y 1–2 workers, modo de acceso Standard, mínimo recomendado: 4 vCPU y 16 GB RAM por nodo | <https://docs.databricks.com/en/compute/configure.html> |

Use las siguientes constantes durante toda la práctica:

```text
Catálogo:              de_training
Esquema:               batch1
Volumen:               de_training.batch1.landing
Ruta del volumen:      /Volumes/de_training/batch1/landing
Carpeta de notebooks:  /Workspace/Users/<usuario>/batch_1
Job:                   job_orders_orchestration_b2
Lote:                  batch_001
```

Ejecute esta celda SQL en un notebook de preparación para comprobar el acceso al catálogo, esquema y volumen:

```sql
USE CATALOG de_training;
USE SCHEMA batch1;

SHOW VOLUMES;

CREATE SCHEMA IF NOT EXISTS de_training.batch1;
```

**Resultado esperado:** el comando `SHOW VOLUMES` debe incluir el volumen `landing` o el instructor debe confirmar la ruta alternativa asignada.

> **Nota terminológica:** esta práctica no utiliza agentes de IA, prompts ni mensajes de sistema. Un parámetro de Job es una entrada de configuración de ejecución; no es un prompt. Un mensaje de sistema sería una instrucción persistente que define el comportamiento de un asistente, concepto que no aplica al Job de Databricks creado en esta actividad.

## Instrucciones Paso a Paso

### Paso 1: Crear la estructura de notebooks

**Objetivo**

Crear los cuatro notebooks modulares que serán ejecutados por las tareas del Job.

**Instrucciones**

1. En el workspace, navegue a la carpeta:

   ```text
   /Workspace/Users/<usuario>/batch_1
   ```

2. Cree la siguiente estructura de carpetas si aún no existe:

   ```text
   01_setup
   02_bronze
   03_silver
   04_gold
   05_validations
   06_merge
   07_time_travel
   ```

3. Dentro de `01_setup`, cree los siguientes notebooks en Python:

   ```text
   01_ingest_landing
   02_transform_orders
   03_validate_orders
   04_publish_run_metrics
   ```

4. Verifique que los nombres coincidan exactamente con los indicados. El Job utilizará estas rutas para localizar cada notebook.

5. En los cuatro notebooks, agregue una primera celda Markdown con el nombre funcional de la tarea. Ejemplo para el primer notebook:

   ```markdown
   # 01_ingest_landing
   Genera un archivo CSV determinista de pedidos en el volumen de entrada.
   ```

**Resultado esperado**

Existen cuatro notebooks Python en una ubicación conocida, estable y accesible para Databricks Workflows.

**Verificación**

Compruebe visualmente que la carpeta contiene los cuatro notebooks:

```text
01_ingest_landing
02_transform_orders
03_validate_orders
04_publish_run_metrics
```

---

### Paso 2: Implementar los notebooks de ingesta y transformación

**Objetivo**

Generar un archivo CSV determinista en el volumen de entrada y transformarlo en una tabla Delta administrada por Unity Catalog.

**Instrucciones**

1. Abra el notebook `01_ingest_landing`.

2. Agregue la siguiente celda Python. Esta celda define los widgets que recibirán los parámetros del Job, valida el nombre del archivo y genera datos reproducibles.

```python
from datetime import datetime
import re

dbutils.widgets.text("run_date", "2026-09-28")
dbutils.widgets.text("source_file", "orders_batch_001.csv")

run_date = dbutils.widgets.get("run_date").strip()
source_file = dbutils.widgets.get("source_file").strip()

if not re.fullmatch(r"[A-Za-z0-9_-]+\.csv", source_file):
    raise ValueError(
        "source_file debe ser un nombre CSV simple, por ejemplo: orders_batch_001.csv"
    )

landing_path = f"/Volumes/de_training/batch1/landing/{source_file}"

orders = [
    ("1001", "C001", "2026-09-28 08:15:00", "125.50", "COMPLETED"),
    ("1002", "C002", "2026-09-28 09:30:00", "80.00", "COMPLETED"),
    ("1003", "C003", "2026-09-28 10:45:00", "210.75", "COMPLETED"),
    ("1003", "C003", "2026-09-28 10:45:00", "210.75", "COMPLETED"),
    ("1004", "C004", "2026-09-28 11:20:00", "-15.00", "COMPLETED")
]

csv_lines = [
    "order_id,customer_id,order_ts,amount,status",
    *[",".join(row) for row in orders]
]

dbutils.fs.put(landing_path, "\n".join(csv_lines), overwrite=True)

print(f"Archivo generado: {landing_path}")
print(f"Fecha de proceso: {run_date}")
print(f"Registros escritos: {len(orders)}")
```

3. Ejecute el notebook manualmente una vez.

4. Confirme que se creó el archivo:

```python
display(dbutils.fs.ls("/Volumes/de_training/batch1/landing/"))
```

5. Abra el notebook `02_transform_orders`.

6. Agregue la siguiente celda Python. La transformación lee el CSV, estandariza los tipos de datos y escribe el resultado en la tabla Delta administrada `bronze_orders`.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
import re

dbutils.widgets.text("run_date", "2026-09-28")
dbutils.widgets.text("source_file", "orders_batch_001.csv")

run_date = dbutils.widgets.get("run_date").strip()
source_file = dbutils.widgets.get("source_file").strip()

if not re.fullmatch(r"[A-Za-z0-9_-]+\.csv", source_file):
    raise ValueError(
        "source_file debe ser un nombre CSV simple, por ejemplo: orders_batch_001.csv"
    )

landing_path = f"/Volumes/de_training/batch1/landing/{source_file}"

spark.sql("""
CREATE TABLE IF NOT EXISTS de_training.batch1.bronze_orders (
    order_id STRING,
    customer_id STRING,
    order_ts TIMESTAMP,
    amount DECIMAL(12,2),
    status STRING,
    run_date DATE,
    source_file STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(landing_path)
)

orders_df = (
    raw_df
    .select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss").alias("order_ts"),
        F.col("amount").cast(DecimalType(12, 2)).alias("amount"),
        F.trim("status").alias("status"),
        F.to_date(F.lit(run_date)).alias("run_date"),
        F.lit(source_file).alias("source_file"),
        F.current_timestamp().alias("ingested_at")
    )
)

spark.sql(f"""
DELETE FROM de_training.batch1.bronze_orders
WHERE source_file = '{source_file}'
  AND run_date = DATE '{run_date}'
""")

(
    orders_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("de_training.batch1.bronze_orders")
)

print(f"Filas transformadas: {orders_df.count()}")
print("Tabla actualizada: de_training.batch1.bronze_orders")
```

7. Ejecute manualmente el notebook de transformación.

8. Consulte los datos creados:

```sql
SELECT
  order_id,
  customer_id,
  order_ts,
  amount,
  status,
  run_date,
  source_file
FROM de_training.batch1.bronze_orders
WHERE source_file = 'orders_batch_001.csv'
ORDER BY order_id, amount;
```

**Resultado esperado**

La tabla `de_training.batch1.bronze_orders` contiene cinco registros del archivo generado. El pedido `1003` aparece dos veces y el pedido `1004` tiene importe negativo; ambos casos se conservarán para que la tarea de validación calcule métricas de calidad.

**Verificación**

Ejecute:

```sql
SELECT COUNT(*) AS total_rows
FROM de_training.batch1.bronze_orders
WHERE source_file = 'orders_batch_001.csv'
  AND run_date = DATE '2026-09-28';
```

El resultado esperado es:

| total_rows |
|---:|
| 5 |

---

### Paso 3: Implementar los notebooks de validación y auditoría

**Objetivo**

Calcular métricas de calidad, publicar task values y registrar el resultado de la ejecución en una tabla Delta de auditoría.

**Instrucciones**

1. Abra el notebook `03_validate_orders`.

2. Agregue la siguiente celda Python:

```python
from pyspark.sql import functions as F
import re

dbutils.widgets.text("run_date", "2026-09-28")
dbutils.widgets.text("source_file", "orders_batch_001.csv")

run_date = dbutils.widgets.get("run_date").strip()
source_file = dbutils.widgets.get("source_file").strip()

if not re.fullmatch(r"[A-Za-z0-9_-]+\.csv", source_file):
    raise ValueError(
        "source_file debe ser un nombre CSV simple, por ejemplo: orders_batch_001.csv"
    )

source_df = (
    spark.table("de_training.batch1.bronze_orders")
    .filter(
        (F.col("source_file") == source_file) &
        (F.col("run_date") == F.to_date(F.lit(run_date)))
    )
)

metrics_row = (
    source_df
    .agg(
        F.count("*").alias("total_rows"),
        F.sum(
            F.when(
                F.col("order_id").isNotNull() &
                F.col("customer_id").isNotNull() &
                F.col("order_ts").isNotNull() &
                (F.col("amount") > 0),
                1
            ).otherwise(0)
        ).alias("valid_rows"),
        F.sum(
            F.when(
                F.col("order_id").isNull() |
                F.col("customer_id").isNull() |
                F.col("order_ts").isNull() |
                (F.col("amount") <= 0),
                1
            ).otherwise(0)
        ).alias("invalid_rows"),
        (
            F.count("*") - F.countDistinct("order_id")
        ).alias("duplicate_excess")
    )
    .collect()[0]
)

total_rows = int(metrics_row["total_rows"] or 0)
valid_rows = int(metrics_row["valid_rows"] or 0)
invalid_rows = int(metrics_row["invalid_rows"] or 0)
duplicate_excess = int(metrics_row["duplicate_excess"] or 0)

quality_status = (
    "PASS"
    if total_rows > 0 and invalid_rows == 0 and duplicate_excess == 0
    else "WARN"
)

dbutils.jobs.taskValues.set(key="total_rows", value=total_rows)
dbutils.jobs.taskValues.set(key="valid_rows", value=valid_rows)
dbutils.jobs.taskValues.set(key="invalid_rows", value=invalid_rows)
dbutils.jobs.taskValues.set(key="duplicate_excess", value=duplicate_excess)
dbutils.jobs.taskValues.set(key="quality_status", value=quality_status)

print(
    f"total_rows={total_rows}, valid_rows={valid_rows}, "
    f"invalid_rows={invalid_rows}, duplicate_excess={duplicate_excess}, "
    f"quality_status={quality_status}"
)
```

3. Abra el notebook `04_publish_run_metrics`.

4. Agregue la siguiente celda Python:

```python
from pyspark.sql import Row
from pyspark.sql import functions as F
import re

dbutils.widgets.text("run_date", "2026-09-28")
dbutils.widgets.text("source_file", "orders_batch_001.csv")

run_date = dbutils.widgets.get("run_date").strip()
source_file = dbutils.widgets.get("source_file").strip()

if not re.fullmatch(r"[A-Za-z0-9_-]+\.csv", source_file):
    raise ValueError(
        "source_file debe ser un nombre CSV simple, por ejemplo: orders_batch_001.csv"
    )

total_rows = dbutils.jobs.taskValues.get(
    taskKey="validate_orders",
    key="total_rows",
    debugValue=-1
)

valid_rows = dbutils.jobs.taskValues.get(
    taskKey="validate_orders",
    key="valid_rows",
    debugValue=-1
)

invalid_rows = dbutils.jobs.taskValues.get(
    taskKey="validate_orders",
    key="invalid_rows",
    debugValue=-1
)

duplicate_excess = dbutils.jobs.taskValues.get(
    taskKey="validate_orders",
    key="duplicate_excess",
    debugValue=-1
)

quality_status = dbutils.jobs.taskValues.get(
    taskKey="validate_orders",
    key="quality_status",
    debugValue="UNKNOWN"
)

if min(int(total_rows), int(valid_rows), int(invalid_rows), int(duplicate_excess)) < 0:
    raise ValueError(
        "No se recuperaron métricas válidas desde la tarea validate_orders."
    )

spark.sql("""
CREATE TABLE IF NOT EXISTS de_training.batch1.batch_ingestion_audit (
    audit_timestamp TIMESTAMP,
    batch_id STRING,
    run_date DATE,
    source_file STRING,
    total_rows BIGINT,
    valid_rows BIGINT,
    invalid_rows BIGINT,
    duplicate_excess BIGINT,
    quality_status STRING
)
USING DELTA
""")

audit_df = spark.createDataFrame([
    Row(
        batch_id="batch_001",
        run_date=run_date,
        source_file=source_file,
        total_rows=int(total_rows),
        valid_rows=int(valid_rows),
        invalid_rows=int(invalid_rows),
        duplicate_excess=int(duplicate_excess),
        quality_status=str(quality_status)
    )
]).withColumn("audit_timestamp", F.current_timestamp()) \
  .withColumn("run_date", F.to_date("run_date"))

audit_df.write.format("delta").mode("append").saveAsTable(
    "de_training.batch1.batch_ingestion_audit"
)

display(audit_df)
```

5. No ejecute todavía el notebook `04_publish_run_metrics` de forma aislada. Depende de task values que solo estarán disponibles cuando el Job ejecute previamente la tarea `validate_orders`.

**Resultado esperado**

Los notebooks contienen la lógica necesaria para calcular cuatro métricas:

| Métrica | Valor esperado |
|---|---:|
| `total_rows` | 5 |
| `valid_rows` | 4 |
| `invalid_rows` | 1 |
| `duplicate_excess` | 1 |
| `quality_status` | `WARN` |

**Verificación**

Ejecute manualmente `03_validate_orders` y revise el resultado de la celda. En una ejecución manual fuera de un Job, los task values se registran para la tarea actual, pero el notebook de auditoría no podrá recuperar valores desde una tarea denominada `validate_orders`. Esta dependencia se validará correctamente al ejecutar el Job multi-tarea.

---

### Paso 4: Crear y configurar el Job multi-tarea

**Objetivo**

Crear el Job `job_orders_orchestration_b2`, definir sus parámetros, tareas, dependencias, cómputo, timeout, reintentos y notificación de fallo.

**Instrucciones**

1. En el menú lateral de Databricks, seleccione **Workflows**.

2. Seleccione **Create job**.

3. Asigne el siguiente nombre al Job:

   ```text
   job_orders_orchestration_b2
   ```

4. En la sección **Parameters**, cree los siguientes parámetros de Job:

   | Clave | Valor predeterminado |
   |---|---|
   | `run_date` | `2026-09-28` |
   | `source_file` | `orders_batch_001.csv` |

5. Cree la primera tarea con esta configuración:

   | Propiedad | Valor |
   |---|---|
   | Task name | `ingest_landing` |
   | Type | Notebook |
   | Notebook path | `/Workspace/Users/<usuario>/batch_1/01_setup/01_ingest_landing` |
   | Timeout | 20 minutos |
   | Retries | 0 |

6. Cree o seleccione un Job Compute con esta configuración mínima:

   | Propiedad | Valor |
   |---|---|
   | Databricks Runtime | 15.4 LTS |
   | Access mode | Standard |
   | Workers | 1 o 2 |
   | Driver | 1 |
   | Photon | Opcional |
   | Terminación automática | Configurada según política institucional; recomendación: 15 minutos para pruebas |

7. Cree la segunda tarea:

   | Propiedad | Valor |
   |---|---|
   | Task name | `transform_orders` |
   | Type | Notebook |
   | Notebook path | `/Workspace/Users/<usuario>/batch_1/01_setup/02_transform_orders` |
   | Depends on | `ingest_landing` |
   | Timeout | 20 minutos |
   | Retries | **1** |

8. Cree la tercera tarea:

   | Propiedad | Valor |
   |---|---|
   | Task name | `validate_orders` |
   | Type | Notebook |
   | Notebook path | `/Workspace/Users/<usuario>/batch_1/01_setup/03_validate_orders` |
   | Depends on | `transform_orders` |
   | Timeout | 20 minutos |
   | Retries | 0 |

9. Cree la cuarta tarea:

   | Propiedad | Valor |
   |---|---|
   | Task name | `publish_run_metrics` |
   | Type | Notebook |
   | Notebook path | `/Workspace/Users/<usuario>/batch_1/01_setup/04_publish_run_metrics` |
   | Depends on | `validate_orders` |
   | Timeout | 20 minutos |
   | Retries | 0 |

10. Confirme que las dependencias formen este DAG:

```text
ingest_landing
      |
      v
transform_orders
      |
      v
validate_orders
      |
      v
publish_run_metrics
```

11. En la configuración del Job, establezca el número máximo de ejecuciones concurrentes en:

```text
1
```

Esto evita que dos ejecuciones simultáneas sobrescriban el mismo archivo de entrada o generen auditorías ambiguas para el mismo lote.

12. Configure una notificación por correo electrónico ante fallo:

   - Evento: **On failure**.
   - Destinatario: correo institucional del estudiante o grupo.
   - No configure notificación de éxito para esta práctica, salvo que el instructor lo solicite.

13. No configure programación. La primera ejecución debe ser manual para facilitar la observación y depuración.

14. Guarde el Job.

**Resultado esperado**

El Job muestra cuatro tareas conectadas secuencialmente. La tarea `transform_orders` tiene exactamente un reintento; las demás no tienen reintentos configurados.

**Verificación**

En la vista del Job, compruebe visualmente:

- Nombre: `job_orders_orchestration_b2`.
- Dos parámetros de Job: `run_date` y `source_file`.
- Cuatro tareas Notebook.
- Dependencias lineales correctas.
- Timeout de 20 minutos en cada tarea.
- Un reintento solo en `transform_orders`.
- Notificación de fallo configurada.
- Máximo de una ejecución concurrente.

---

### Paso 5: Ejecutar, observar y verificar el workflow

**Objetivo**

Lanzar una ejecución manual, interpretar el grafo de ejecución, revisar logs y verificar las tablas Delta resultantes.

**Instrucciones**

1. En el Job `job_orders_orchestration_b2`, seleccione **Run now**.

2. Confirme o introduzca estos parámetros:

   | Parámetro | Valor |
   |---|---|
   | `run_date` | `2026-09-28` |
   | `source_file` | `orders_batch_001.csv` |

3. Inicie la ejecución.

4. Observe el grafo de ejecución. Verifique que las tareas comienzan en este orden:

   1. `ingest_landing`
   2. `transform_orders`
   3. `validate_orders`
   4. `publish_run_metrics`

5. Abra los logs de `ingest_landing` y confirme que aparece una salida similar a:

```text
Archivo generado: /Volumes/de_training/batch1/landing/orders_batch_001.csv
Fecha de proceso: 2026-09-28
Registros escritos: 5
```

6. Abra los logs de `transform_orders` y confirme que aparece:

```text
Filas transformadas: 5
Tabla actualizada: de_training.batch1.bronze_orders
```

7. Abra los logs de `validate_orders` y confirme que aparecen las métricas esperadas:

```text
total_rows=5, valid_rows=4, invalid_rows=1, duplicate_excess=1, quality_status=WARN
```

8. Abra la tarea `publish_run_metrics` y compruebe que finaliza correctamente. Revise la tabla mostrada en la salida del notebook.

9. Ejecute la siguiente consulta SQL para revisar el registro de auditoría:

```sql
SELECT
  audit_timestamp,
  batch_id,
  run_date,
  source_file,
  total_rows,
  valid_rows,
  invalid_rows,
  duplicate_excess,
  quality_status
FROM de_training.batch1.batch_ingestion_audit
WHERE source_file = 'orders_batch_001.csv'
ORDER BY audit_timestamp DESC;
```

**Resultado esperado**

La ejecución completa finaliza con estado **Succeeded**. El archivo CSV existe en el volumen, la tabla `bronze_orders` contiene cinco registros y la tabla `batch_ingestion_audit` contiene una fila de auditoría con las métricas publicadas.

**Verificación**

La siguiente consulta debe devolver al menos un registro con los valores esperados:

```sql
SELECT
  total_rows,
  valid_rows,
  invalid_rows,
  duplicate_excess,
  quality_status
FROM de_training.batch1.batch_ingestion_audit
WHERE source_file = 'orders_batch_001.csv'
ORDER BY audit_timestamp DESC
LIMIT 1;
```

Resultado esperado:

| total_rows | valid_rows | invalid_rows | duplicate_excess | quality_status |
|---:|---:|---:|---:|---|
| 5 | 4 | 1 | 1 | WARN |

## Validación y Pruebas

Complete las siguientes pruebas y conserve evidencia técnica suficiente: resultados SQL, salida de logs o captura del DAG. No es necesario documentar razonamiento interno; la evidencia debe demostrar comportamiento observable y reproducible.

| ID | Prueba | Acción | Criterio medible de aprobación |
|---|---|---|---|
| V1 | Estructura del DAG | Revisar el grafo de la ejecución | Cuatro tareas visibles y conectadas en el orden Ingesta → Transformación → Validación → Publicación. |
| V2 | Archivo de entrada | Listar el volumen | Existe exactamente el archivo `orders_batch_001.csv` en `/Volumes/de_training/batch1/landing/`. |
| V3 | Carga Delta | Consultar `bronze_orders` | Existen 5 filas para `source_file = 'orders_batch_001.csv'` y `run_date = '2026-09-28'`. |
| V4 | Métricas de calidad | Revisar logs de `validate_orders` y auditoría | Métricas: 5 total, 4 válidas, 1 inválida, 1 duplicado excedente, estado `WARN`. |
| V5 | Transferencia de task values | Revisar resultado de `publish_run_metrics` | La tarea finaliza correctamente y crea una fila en `batch_ingestion_audit` con las métricas de V4. |
| V6 | Reintento configurado | Revisar configuración de `transform_orders` | La tarea tiene `Retries = 1`; las demás tareas tienen `Retries = 0`. |
| V7 | Protección de parámetro adversarial | Ejecutar manualmente con `source_file=../archivo.csv` | La tarea `ingest_landing` falla con `ValueError`; no debe crear archivos fuera de la ruta esperada. |
| V8 | Contenido adversarial como dato | Añadir temporalmente una línea CSV que contenga texto como `ignore previous instructions` en una columna no utilizada y reprocesar en una copia de archivo | El workflow trata el texto como dato, no como una instrucción. No existen prompts, agentes ni mensajes de sistema que puedan ejecutar esa cadena. La validación depende únicamente de las reglas Spark definidas en el notebook. |

Para V7, utilice esta ejecución manual controlada:

```text
run_date = 2026-09-28
source_file = ../archivo.csv
```

**Resultado esperado de V7:** la validación con expresión regular rechaza el valor antes de construir una ruta de archivo.

Después de la prueba V7, ejecute nuevamente el Job con los parámetros válidos:

```text
run_date = 2026-09-28
source_file = orders_batch_001.csv
```

> **Limitación operativa:** el estado `WARN` es intencional en este laboratorio porque el archivo determinista contiene un importe inválido y un duplicado. En un proceso productivo, el equipo debe definir explícitamente si un estado `WARN` permite continuar, envía registros a cuarentena o debe fallar el Job.

## Solución de Problemas

### Problema 1: La tarea `publish_run_metrics` falla al recuperar task values

**Síntomas**

- La tarea `validate_orders` finaliza correctamente.
- La tarea `publish_run_metrics` muestra un error similar a:
  ```text
  No se recuperaron métricas válidas desde la tarea validate_orders.
  ```
- Los valores recuperados son `-1` o `UNKNOWN`.

**Causa**

El nombre de la tarea configurado en el Job no coincide exactamente con `validate_orders`, o el notebook de auditoría se ejecutó de forma aislada fuera del Job. Los task values se comparten entre tareas de una misma ejecución de Job y se identifican por `taskKey`.

**Corrección**

1. Abra la configuración del Job.
2. Verifique que el nombre de la tercera tarea sea exactamente:

   ```text
   validate_orders
   ```

3. Confirme que `publish_run_metrics` dependa de `validate_orders`.
4. No ejecute el notebook de auditoría de forma aislada para validar task values.
5. Ejecute nuevamente el Job completo desde **Run now**.

### Problema 2: La tarea `transform_orders` no encuentra el archivo CSV

**Síntomas**

- La tarea muestra un error de ruta inexistente, por ejemplo:
  ```text
  Path does not exist
  ```
- No aparece el archivo esperado al listar el volumen.

**Causa**

El valor de `source_file` no es idéntico entre tareas, la tarea de ingesta falló o se modificó manualmente la ruta del volumen. También puede ocurrir si el usuario no tiene permisos de escritura o lectura sobre el volumen administrado.

**Corrección**

1. Verifique que el parámetro del Job sea:

   ```text
   source_file = orders_batch_001.csv
   ```

2. Revise que las cuatro tareas reciban automáticamente los parámetros del Job y que no tengan parámetros de tarea que sobrescriban el valor.
3. Compruebe el contenido del volumen:

   ```python
   display(dbutils.fs.ls("/Volumes/de_training/batch1/landing/"))
   ```

4. Confirme que existe el archivo:

   ```text
   orders_batch_001.csv
   ```

5. Si no existe, ejecute de nuevo el Job completo; no ejecute solamente `transform_orders`.
6. Si persiste el problema, solicite al instructor la revisión de permisos sobre `de_training.batch1.landing`.

## Limpieza

Los activos creados en esta práctica serán reutilizados en prácticas posteriores. Por tanto, **no elimine los notebooks, el Job ni las tablas** salvo que el instructor solicite explícitamente una limpieza completa.

Si necesita reiniciar únicamente los datos de prueba, ejecute estas instrucciones SQL:

```sql
DELETE FROM de_training.batch1.bronze_orders
WHERE source_file = 'orders_batch_001.csv'
  AND run_date = DATE '2026-09-28';

DELETE FROM de_training.batch1.batch_ingestion_audit
WHERE source_file = 'orders_batch_001.csv'
  AND run_date = DATE '2026-09-28';
```

Para eliminar solamente el archivo de entrada de prueba:

```python
dbutils.fs.rm(
    "/Volumes/de_training/batch1/landing/orders_batch_001.csv",
    recurse=False
)
```

Si el instructor solicita limpieza total al finalizar el bloque formativo, ejecute únicamente con autorización:

```sql
DROP TABLE IF EXISTS de_training.batch1.batch_ingestion_audit;
DROP TABLE IF EXISTS de_training.batch1.bronze_orders;
```

Después, elimine manualmente el Job desde **Workflows** y borre los notebooks únicamente si no serán utilizados en prácticas posteriores.

## Resumen

En esta práctica se creó un workflow multi-tarea reutilizable con Databricks Workflows. El Job `job_orders_orchestration_b2` orquesta cuatro notebooks mediante un DAG lineal, recibe parámetros de ejecución, utiliza task values para transferir métricas y registra resultados en una tabla Delta de auditoría.

La ejecución exitosa demuestra los siguientes conceptos operativos:

- Un **Job** es una definición persistente; un **Run** es una ejecución concreta.
- Cada **Task** tiene una responsabilidad delimitada y depende explícitamente de la tarea anterior.
- Los parámetros `run_date` y `source_file` permiten reutilizar la misma definición del Job.
- Los task values permiten comunicar resultados pequeños entre tareas sin depender de archivos intermedios.
- Los timeouts, reintentos, notificaciones y límites de concurrencia son controles esenciales para la operación de workflows.
- Los logs de tareas, el grafo de ejecución y las tablas Delta proporcionan evidencia verificable del comportamiento del pipeline.

Recursos oficiales:

- Databricks Workflows: <https://docs.databricks.com/en/workflows/index.html>
- Configuración de Jobs: <https://docs.databricks.com/en/jobs/configure-job.html>
- Parámetros de Job: <https://docs.databricks.com/en/jobs/job-parameters.html>
- Task values: <https://docs.databricks.com/en/jobs/task-values.html>
- Unity Catalog Volumes: <https://docs.databricks.com/en/volumes/index.html>
- Tablas Delta: <https://docs.databricks.com/en/delta/index.html>

---

# Configuración de tareas condicionales y recuperación ante fallos

## Metadatos

| Campo | Valor |
|---|---|
| Duración | 50 minutos |
| Complejidad | Media |
| Nivel de Bloom | Aplicar |

## Descripción General

En esta práctica se amplía el Job `job_orders_orchestration_b2`, creado en la Práctica 6, para incorporar una compuerta de calidad basada en valores publicados entre tareas. El flujo decidirá entre promover datos válidos para su consumo posterior por un pipeline DLT o enviar registros inválidos a una tabla de cuarentena con trazabilidad de auditoría. Finalmente, se provocará una falla controlada en una tarea descendiente y se utilizará un **Repair run** para recuperar únicamente las tareas necesarias.

## Objetivos de Aprendizaje

- [ ] Publicar los task values `quality_status`, `invalid_row_count` y `total_row_count` desde un notebook de validación.
- [ ] Configurar una tarea condicional **If/else** que evalúe una métrica publicada por una tarea previa.
- [ ] Implementar ramas diferenciadas para datos aprobados y datos con observaciones de calidad.
- [ ] Simular una falla controlada, interpretar los estados de tareas y ejecutar un **Repair run** selectivo.
- [ ] Verificar que los resultados del flujo sean trazables mediante tablas Delta de cuarentena y auditoría.

## Prerrequisitos

### Conocimientos requeridos

- Comprensión de Jobs, Tasks, dependencias y ejecuciones en Databricks Workflows.
- Uso básico de notebooks Databricks con Python y Spark SQL.
- Conocimiento de tablas Delta, escritura `append` y validaciones de datos.
- Comprensión básica de parámetros de tareas y widgets de Databricks.
- Práctica 6 completada, incluido el Job `job_orders_orchestration_b2`.

### Accesos requeridos

El estudiante debe disponer de:

- Permiso para editar, ejecutar y consultar el historial del Job `job_orders_orchestration_b2`.
- Permiso de uso sobre el clúster o Job cluster asignado.
- Permisos `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `MODIFY` y `SELECT` sobre el esquema de trabajo.
- Permiso para escribir en el volumen administrado `de_training.batch1.landing`.
- Acceso a los notebooks de la Práctica 6, especialmente `03_validate_orders`.
- Acceso de escritura a las tablas de cuarentena y auditoría del flujo.

> **Nota de nomenclatura:** este laboratorio utiliza `de_training.batch1` como catálogo y esquema de referencia institucional. Si la Práctica 6 utiliza el esquema `workflow_dlt`, sustituya de forma consistente todas las referencias `de_training.batch1` por `de_training.workflow_dlt`. No mezcle ambos esquemas dentro de una misma ejecución.

## Entorno de Laboratorio

### Versiones y fuentes oficiales

| Componente | Versión exacta utilizada | Uso en la práctica | Fuente oficial |
|---|---:|---|---|
| Databricks Runtime | 15.4 LTS, arquitectura x86_64 | Ejecución de notebooks y Jobs | https://docs.databricks.com/en/release-notes/runtime/15.4lts.html |
| Apache Spark | 3.5.0, incluido en Databricks Runtime 15.4 LTS | Transformaciones y escritura Delta | https://spark.apache.org/releases/spark-release-3-5-0.html |
| Python | 3.11.0, incluido en Databricks Runtime 15.4 LTS | Código de notebooks | https://docs.python.org/release/3.11.0/ |
| Delta Lake | 3.2.0, incluido en Databricks Runtime 15.4 LTS | Tablas Delta de cuarentena y auditoría | https://docs.delta.io/3.2.0/index.html |
| Databricks Workflows | Servicio SaaS compatible con Databricks Runtime 15.4 LTS; sin versión independiente publicada | Orquestación, If/else y Repair runs | https://docs.databricks.com/en/workflows/index.html |
| Unity Catalog | Servicio SaaS compatible con Databricks Runtime 15.4 LTS; sin versión independiente publicada | Gobierno de tablas y volumen | https://docs.databricks.com/en/data-governance/unity-catalog/index.html |

### Configuración base

| Recurso | Valor esperado |
|---|---|
| Catálogo | `de_training` |
| Esquema | `batch1` |
| Volumen administrado | `de_training.batch1.landing` |
| Ruta del volumen | `/Volumes/de_training/batch1/landing/` |
| Carpeta de notebooks | `/Workspace/Users/<usuario>/batch_1/` |
| Job existente | `job_orders_orchestration_b2` |
| Lote válido | `batch_001` |
| Lote con observaciones | `batch_002` |
| Lote para recuperación | `batch_003` |

### Comandos de preparación

Ejecute la siguiente celda en un notebook de preparación o al inicio de `03_validate_orders`. Ajuste únicamente el esquema si su instructor provisionó `workflow_dlt`.

```python
CATALOG = "de_training"
SCHEMA = "batch1"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.quarantine_orders (
  order_id STRING,
  customer_id STRING,
  order_date DATE,
  amount DECIMAL(18,2),
  validation_reason STRING,
  batch_id STRING,
  quarantined_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.batch_ingestion_audit (
  batch_id STRING,
  event_type STRING,
  quality_status STRING,
  invalid_row_count BIGINT,
  total_row_count BIGINT,
  event_timestamp TIMESTAMP,
  message STRING
)
USING DELTA
""")
```

## Instrucciones Paso a Paso

### Paso 1: Revisar el flujo inicial del Job

**Objetivo:** identificar la tarea de validación existente, sus dependencias y el punto donde se agregará la compuerta condicional.

1. En Databricks, abra **Workflows** y seleccione el Job `job_orders_orchestration_b2`.
2. Seleccione **Edit task** o **Edit job** según la interfaz disponible.
3. Identifique la tarea que ejecuta el notebook `03_validate_orders`. Para esta práctica se asumirá que la clave de tarea es:

   ```text
   validate_orders
   ```

4. Confirme que la tarea de validación depende de la ingesta o preparación de pedidos realizada en la Práctica 6.
5. Revise los parámetros actuales del Job. Si no existen, cree los siguientes parámetros a nivel de Job:

   | Parámetro | Valor inicial | Uso |
   |---|---|---|
   | `batch_id` | `batch_001` | Identificador trazable de la ejecución |
   | `quality_scenario` | `valid` | Escenario de datos: `valid` o `invalid` |
   | `simulate_publish_failure` | `false` | Controla la falla inducida en promoción |

6. Verifique que la tarea `validate_orders` reciba estos parámetros como parámetros del notebook:

   ```text
   batch_id = {{job.parameters.batch_id}}
   quality_scenario = {{job.parameters.quality_scenario}}
   ```

**Resultado esperado:** el Job contiene una tarea de validación identificada y dispone de parámetros reutilizables para controlar los escenarios del laboratorio.

**Verificación:**

- El grafo del Job muestra la tarea `validate_orders`.
- Los parámetros `batch_id`, `quality_scenario` y `simulate_publish_failure` están definidos.
- La tarea de validación recibe al menos `batch_id` y `quality_scenario`.

---

### Paso 2: Publicar métricas de calidad mediante task values

**Objetivo:** modificar `03_validate_orders` para validar pedidos y publicar métricas consumibles por tareas posteriores.

1. Abra el notebook:

   ```text
   /Workspace/Users/<usuario>/batch_1/05_validations/03_validate_orders
   ```

   Si la estructura de la Práctica 6 es distinta, abra el notebook asociado a la tarea `validate_orders`.

2. Agregue o sustituya el contenido de validación por el siguiente código. El código crea datos controlados para que los resultados del laboratorio sean reproducibles.

   ```python
   from pyspark.sql import functions as F
   from pyspark.sql.types import (
       StructType, StructField, StringType, DateType, DecimalType
   )
   from decimal import Decimal

   dbutils.widgets.text("batch_id", "batch_001")
   dbutils.widgets.dropdown("quality_scenario", "valid", ["valid", "invalid"])

   batch_id = dbutils.widgets.get("batch_id")
   quality_scenario = dbutils.widgets.get("quality_scenario")

   CATALOG = "de_training"
   SCHEMA = "batch1"

   valid_rows = [
       ("ORD-1001", "CUST-001", "2026-09-28", Decimal("120.50")),
       ("ORD-1002", "CUST-002", "2026-09-28", Decimal("89.90")),
       ("ORD-1003", "CUST-003", "2026-09-28", Decimal("45.00"))
   ]

   invalid_rows = [
       ("ORD-1001", "CUST-001", "2026-09-28", Decimal("120.50")),
       ("INVALID", "CUST-002", "2026-09-28", Decimal("89.90")),
       ("ORD-1003", "CUST-003", "2026-09-28", None)
   ]

   source_rows = valid_rows if quality_scenario == "valid" else invalid_rows

   schema = StructType([
       StructField("order_id", StringType(), False),
       StructField("customer_id", StringType(), False),
       StructField("order_date_text", StringType(), False),
       StructField("amount", DecimalType(18, 2), True)
   ])

   orders_df = (
       spark.createDataFrame(source_rows, schema)
       .withColumn("order_date", F.to_date("order_date_text"))
       .drop("order_date_text")
       .withColumn("batch_id", F.lit(batch_id))
   )

   invalid_condition = (
       F.col("amount").isNull() |
       (~F.col("order_id").rlike(r"^ORD-[0-9]+$"))
   )

   validated_df = orders_df.withColumn(
       "validation_reason",
       F.when(F.col("amount").isNull(), F.lit("amount_null"))
        .when(~F.col("order_id").rlike(r"^ORD-[0-9]+$"), F.lit("invalid_order_id"))
        .otherwise(F.lit(None).cast("string"))
   )

   invalid_df = validated_df.filter(invalid_condition)
   total_row_count = validated_df.count()
   invalid_row_count = invalid_df.count()

   quality_status = "APPROVED" if invalid_row_count == 0 else "OBSERVED"

   # Persistencia temporal y trazable de la fuente validada para tareas descendientes.
   (
       validated_df
       .write
       .format("delta")
       .mode("overwrite")
       .option("replaceWhere", f"batch_id = '{batch_id}'")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.validation_orders_staging")
   )

   # Publicación de valores de tarea para consumo por If/else y auditoría.
   dbutils.jobs.taskValues.set(key="quality_status", value=quality_status)
   dbutils.jobs.taskValues.set(key="invalid_row_count", value=int(invalid_row_count))
   dbutils.jobs.taskValues.set(key="total_row_count", value=int(total_row_count))

   print(f"batch_id={batch_id}")
   print(f"quality_status={quality_status}")
   print(f"invalid_row_count={invalid_row_count}")
   print(f"total_row_count={total_row_count}")

   display(validated_df)
   ```

3. Ejecute el notebook de forma interactiva con:

   ```text
   batch_id = batch_001
   quality_scenario = valid
   ```

4. Revise la salida de la celda y confirme que las tres métricas se imprimen.

5. Guarde el notebook.

> **Diseño aplicado:** `taskValues` permite intercambiar resultados pequeños y serializables entre tareas del mismo Job. No se utiliza para transportar DataFrames ni grandes volúmenes de registros; los registros se mantienen en una tabla Delta.

**Resultado esperado:** el notebook publica un estado `APPROVED` para el escenario válido y deja disponible la tabla `validation_orders_staging`.

**Verificación:**

Ejecute:

```sql
SELECT
  batch_id,
  order_id,
  amount,
  validation_reason
FROM de_training.batch1.validation_orders_staging
WHERE batch_id = 'batch_001';
```

El resultado debe contener tres registros y `validation_reason` debe ser `NULL` para todos.

---

### Paso 3: Crear la tarea condicional evaluate_quality_gate

**Objetivo:** configurar una tarea If/else que seleccione la rama aprobada o la rama de observación según `quality_status`.

1. Regrese a la edición del Job `job_orders_orchestration_b2`.
2. Seleccione **Add task**.
3. Configure la tarea con los siguientes valores:

   | Campo | Valor |
   |---|---|
   | Tipo de tarea | `If/else condition` |
   | Nombre de tarea | `evaluate_quality_gate` |
   | Depende de | `validate_orders` |
   | Operando izquierdo | `{{tasks.validate_orders.values.quality_status}}` |
   | Operador | `Equals` |
   | Operando derecho | `APPROVED` |

4. Guarde la tarea.
5. Confirme visualmente que `evaluate_quality_gate` depende de `validate_orders`.

La condición equivale conceptualmente a:

```text
{{tasks.validate_orders.values.quality_status}} == "APPROVED"
```

6. No configure reintentos en esta tarea condicional. Un error de configuración de la condición debe ser visible y corregirse en la definición del Job, no ocultarse mediante reintentos.

**Resultado esperado:** la tarea `evaluate_quality_gate` queda configurada para evaluar el valor publicado por `validate_orders`.

**Verificación:**

- La interfaz muestra el valor dinámico exactamente como:

  ```text
  {{tasks.validate_orders.values.quality_status}}
  ```

- La dependencia de `evaluate_quality_gate` apunta a `validate_orders`.
- El valor comparado es `APPROVED`, respetando mayúsculas.

---

### Paso 4: Configurar las ramas aprobada y de observación

**Objetivo:** agregar notebooks descendientes que promocionen datos válidos o pongan en cuarentena los registros inválidos.

1. Cree el notebook `05_promote_for_dlt` en la carpeta:

   ```text
   /Workspace/Users/<usuario>/batch_1/04_gold/05_promote_for_dlt
   ```

2. Agregue el siguiente código:

   ```python
   from pyspark.sql import functions as F

   dbutils.widgets.text("batch_id", "batch_001")
   dbutils.widgets.dropdown("simulate_publish_failure", "false", ["false", "true"])

   batch_id = dbutils.widgets.get("batch_id")
   simulate_publish_failure = dbutils.widgets.get("simulate_publish_failure").lower()

   CATALOG = "de_training"
   SCHEMA = "batch1"
   prepared_path = f"/Volumes/{CATALOG}/{SCHEMA}/landing/dlt_ready/orders_{batch_id}"

   if simulate_publish_failure == "true":
       raise RuntimeError(
           "Falla controlada: simulate_publish_failure=true. "
           "La promoción para DLT se interrumpió intencionalmente."
       )

   approved_df = (
       spark.table(f"{CATALOG}.{SCHEMA}.validation_orders_staging")
       .filter(
           (F.col("batch_id") == batch_id) &
           F.col("validation_reason").isNull()
       )
   )

   approved_count = approved_df.count()

   if approved_count == 0:
       raise ValueError(
           f"No existen registros aprobados para batch_id={batch_id}."
       )

   (
       approved_df
       .write
       .format("delta")
       .mode("overwrite")
       .save(prepared_path)
   )

   spark.sql(f"""
   INSERT INTO {CATALOG}.{SCHEMA}.batch_ingestion_audit
   VALUES (
     '{batch_id}',
     'DLT_PROMOTION',
     'APPROVED',
     0,
     {approved_count},
     current_timestamp(),
     'Archivo Delta preparado para consumo por DLT: {prepared_path}'
   )
   """)

   print(f"Promoción completada. Registros preparados: {approved_count}")
   print(f"Ruta preparada para DLT: {prepared_path}")
   ```

3. Cree el notebook `06_quarantine_invalid_rows` en:

   ```text
   /Workspace/Users/<usuario>/batch_1/05_validations/06_quarantine_invalid_rows
   ```

4. Agregue el siguiente código:

   ```python
   from pyspark.sql import functions as F

   dbutils.widgets.text("batch_id", "batch_002")

   batch_id = dbutils.widgets.get("batch_id")

   CATALOG = "de_training"
   SCHEMA = "batch1"

   invalid_df = (
       spark.table(f"{CATALOG}.{SCHEMA}.validation_orders_staging")
       .filter(
           (F.col("batch_id") == batch_id) &
           F.col("validation_reason").isNotNull()
       )
       .select(
           "order_id",
           "customer_id",
           "order_date",
           "amount",
           "validation_reason",
           "batch_id"
       )
       .withColumn("quarantined_at", F.current_timestamp())
   )

   invalid_row_count = invalid_df.count()

   if invalid_row_count == 0:
       raise ValueError(
           f"La rama de cuarentena se ejecutó sin registros inválidos para {batch_id}."
       )

   (
       invalid_df
       .write
       .format("delta")
       .mode("append")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.quarantine_orders")
   )

   total_row_count = (
       spark.table(f"{CATALOG}.{SCHEMA}.validation_orders_staging")
       .filter(F.col("batch_id") == batch_id)
       .count()
   )

   spark.sql(f"""
   INSERT INTO {CATALOG}.{SCHEMA}.batch_ingestion_audit
   VALUES (
     '{batch_id}',
     'QUALITY_ALERT',
     'OBSERVED',
     {invalid_row_count},
     {total_row_count},
     current_timestamp(),
     'Registros enviados a cuarentena por incumplimiento de reglas de calidad.'
   )
   """)

   print(f"Registros enviados a cuarentena: {invalid_row_count}")
   ```

5. Regrese al Job y agregue una tarea de tipo **Notebook** con la siguiente configuración:

   | Campo | Valor |
   |---|---|
   | Nombre de tarea | `promote_for_dlt` |
   | Notebook | `05_promote_for_dlt` |
   | Dependencia | `evaluate_quality_gate` |
   | Condición de ejecución | `All succeeded` y rama `True` |
   | Parámetro `batch_id` | `{{job.parameters.batch_id}}` |
   | Parámetro `simulate_publish_failure` | `{{job.parameters.simulate_publish_failure}}` |
   | Reintentos | `0` |

6. Agregue otra tarea de tipo **Notebook**:

   | Campo | Valor |
   |---|---|
   | Nombre de tarea | `quarantine_invalid_rows` |
   | Notebook | `06_quarantine_invalid_rows` |
   | Dependencia | `evaluate_quality_gate` |
   | Condición de ejecución | `All succeeded` y rama `False` |
   | Parámetro `batch_id` | `{{job.parameters.batch_id}}` |
   | Reintentos | `0` |

7. Guarde el Job y revise el grafo. Debe reflejar la siguiente estructura:

```text
validate_orders
       |
       v
evaluate_quality_gate
      /             \
 True /               \ False
    v                   v
promote_for_dlt   quarantine_invalid_rows
```

**Resultado esperado:** el Job tiene dos rutas mutuamente excluyentes, determinadas por el resultado de la compuerta de calidad.

**Verificación:**

- `promote_for_dlt` se ejecuta únicamente en la rama `True`.
- `quarantine_invalid_rows` se ejecuta únicamente en la rama `False`.
- Ambas tareas reciben el mismo `batch_id` del Job.
- La política de reintentos de ambas tareas es `0`, para observar claramente la falla controlada y usar Repair run.

---

### Paso 5: Ejecutar los escenarios aprobado y observado

**Objetivo:** validar que el flujo cambia de ruta según el resultado de la validación.

1. En el Job, seleccione **Run now with different parameters**.
2. Ejecute el escenario válido con estos valores:

   | Parámetro | Valor |
   |---|---|
   | `batch_id` | `batch_001` |
   | `quality_scenario` | `valid` |
   | `simulate_publish_failure` | `false` |

3. Espere a que finalice la ejecución.
4. Confirme los estados esperados:

   | Tarea | Estado esperado |
   |---|---|
   | `validate_orders` | Succeeded |
   | `evaluate_quality_gate` | Succeeded; condición verdadera |
   | `promote_for_dlt` | Succeeded |
   | `quarantine_invalid_rows` | Excluded o Skipped |

5. Ejecute el escenario con observaciones:

   | Parámetro | Valor |
   |---|---|
   | `batch_id` | `batch_002` |
   | `quality_scenario` | `invalid` |
   | `simulate_publish_failure` | `false` |

6. Confirme los estados esperados:

   | Tarea | Estado esperado |
   |---|---|
   | `validate_orders` | Succeeded |
   | `evaluate_quality_gate` | Succeeded; condición falsa |
   | `promote_for_dlt` | Excluded o Skipped |
   | `quarantine_invalid_rows` | Succeeded |

7. Consulte las evidencias generadas:

   ```sql
   SELECT
     batch_id,
     order_id,
     amount,
     validation_reason,
     quarantined_at
   FROM de_training.batch1.quarantine_orders
   WHERE batch_id = 'batch_002'
   ORDER BY order_id;
   ```

8. Consulte la auditoría:

   ```sql
   SELECT
     batch_id,
     event_type,
     quality_status,
     invalid_row_count,
     total_row_count,
     message
   FROM de_training.batch1.batch_ingestion_audit
   WHERE batch_id IN ('batch_001', 'batch_002')
   ORDER BY event_timestamp;
   ```

**Resultado esperado:** el lote `batch_001` se promociona para DLT y el lote `batch_002` genera dos registros de cuarentena y un evento de auditoría `QUALITY_ALERT`.

**Verificación:**

Para `batch_001`, ejecute:

```python
display(
    spark.read.format("delta").load(
        "/Volumes/de_training/batch1/landing/dlt_ready/orders_batch_001"
    )
)
```

Deben mostrarse tres pedidos aprobados.

Para `batch_002`, la tabla `quarantine_orders` debe contener exactamente dos registros:

| order_id | validation_reason |
|---|---|
| `INVALID` | `invalid_order_id` |
| `ORD-1003` | `amount_null` |

---

### Paso 6: Inducir una falla y recuperar mediante Repair run

**Objetivo:** provocar una falla controlada en una tarea descendiente y recuperar la ejecución sin repetir tareas ya exitosas.

1. Inicie una nueva ejecución con los siguientes parámetros:

   | Parámetro | Valor |
   |---|---|
   | `batch_id` | `batch_003` |
   | `quality_scenario` | `valid` |
   | `simulate_publish_failure` | `true` |

2. Espere a que finalice la ejecución.
3. Abra la ejecución fallida y confirme estos estados:

   | Tarea | Estado esperado |
   |---|---|
   | `validate_orders` | Succeeded |
   | `evaluate_quality_gate` | Succeeded |
   | `promote_for_dlt` | Failed |
   | `quarantine_invalid_rows` | Excluded o Skipped |

4. Abra los logs de `promote_for_dlt`.
5. Localice el mensaje:

   ```text
   Falla controlada: simulate_publish_failure=true.
   ```

6. Seleccione **Repair run**.
7. En la interfaz de recuperación:
   - Seleccione exclusivamente la tarea fallida `promote_for_dlt`.
   - Incluya tareas dependientes solamente si existieran en su versión del Job.
   - No seleccione `validate_orders` ni `evaluate_quality_gate`, ya que finalizaron correctamente.
8. Cambie el valor del parámetro:

   | Parámetro | Nuevo valor |
   |---|---|
   | `simulate_publish_failure` | `false` |

9. Mantenga:

   ```text
   batch_id = batch_003
   quality_scenario = valid
   ```

10. Inicie el Repair run.
11. Revise el grafo de la ejecución reparada y confirme que se reutilizan las tareas exitosas de la ejecución original.

**Resultado esperado:** el Repair run ejecuta `promote_for_dlt` correctamente sin repetir la validación ni la compuerta de calidad.

**Verificación:**

Ejecute:

```sql
SELECT
  batch_id,
  event_type,
  quality_status,
  message
FROM de_training.batch1.batch_ingestion_audit
WHERE batch_id = 'batch_003'
ORDER BY event_timestamp;
```

Debe existir un evento `DLT_PROMOTION` con `quality_status = 'APPROVED'`.

Compruebe además la salida preparada:

```python
recovered_df = spark.read.format("delta").load(
    "/Volumes/de_training/batch1/landing/dlt_ready/orders_batch_003"
)

assert recovered_df.count() == 3, "Se esperaban tres registros promovidos."
print("Repair run validado: se promovieron 3 registros para batch_003.")
```

## Validación y Pruebas

Complete las siguientes pruebas y conserve como evidencia las salidas SQL, el estado del grafo de tareas y los logs de la ejecución fallida.

| ID | Prueba | Evidencia medible | Criterio de aprobación |
|---|---|---|---|
| V1 | Publicación de task values | Logs de `validate_orders` | Se imprimen `quality_status`, `invalid_row_count` y `total_row_count`. |
| V2 | Ruta aprobada | Grafo de `batch_001` | `promote_for_dlt` finaliza correctamente y cuarentena queda excluida. |
| V3 | Ruta observada | Consulta a `quarantine_orders` | `batch_002` contiene exactamente 2 registros inválidos. |
| V4 | Auditoría funcional | Consulta a `batch_ingestion_audit` | Existe `QUALITY_ALERT` para `batch_002` y `DLT_PROMOTION` para `batch_001`. |
| V5 | Falla controlada | Logs de `promote_for_dlt` para `batch_003` | La ejecución falla por `simulate_publish_failure=true`. |
| V6 | Recuperación selectiva | Grafo del Repair run | Solo se reejecuta `promote_for_dlt`; validación y compuerta no se repiten. |
| V7 | Caso adversarial de calidad | Ejecución con `quality_scenario=invalid` y un `order_id` no conforme | El texto inválido se trata como dato y se dirige a cuarentena; no altera la definición ni las dependencias del Job. |

Ejecute esta consulta de validación consolidada:

```sql
SELECT
  batch_id,
  event_type,
  quality_status,
  invalid_row_count,
  total_row_count,
  message
FROM de_training.batch1.batch_ingestion_audit
WHERE batch_id IN ('batch_001', 'batch_002', 'batch_003')
ORDER BY batch_id, event_timestamp;
```

Criterios mínimos de finalización:

1. `batch_001` tiene una promoción exitosa y tres registros preparados para DLT.
2. `batch_002` tiene una alerta de calidad y exactamente dos registros en cuarentena.
3. `batch_003` presenta una falla inicial documentada y una promoción exitosa tras el Repair run.
4. Las tareas exitosas previas a la falla no se ejecutan de nuevo durante la recuperación.
5. El estudiante puede explicar la diferencia entre una política de reintentos y un Repair run:
   - Un **reintento** vuelve a intentar automáticamente una tarea según su configuración.
   - Un **Repair run** permite recuperar una ejecución fallida seleccionando tareas fallidas y dependientes, evitando repetir tareas exitosas cuando no es necesario.

> **Nota sobre instrucciones y seguridad:** este laboratorio no utiliza un asistente de IA, prompts ni mensajes de sistema para tomar decisiones de calidad. El caso adversarial verifica que valores de datos inesperados o texto potencialmente engañoso se procesen como registros de entrada y no como instrucciones ejecutables. La decisión operacional depende exclusivamente de reglas Spark explícitas y task values auditables.

## Solución de Problemas

### Problema 1: La tarea If/else no encuentra `quality_status`

**Síntomas:**

- `evaluate_quality_gate` falla al iniciar.
- La interfaz muestra un error similar a valor dinámico no resuelto.
- La condición no puede interpretar `{{tasks.validate_orders.values.quality_status}}`.

**Causa probable:**

La tarea `validate_orders` no publicó el task value, la clave tiene un nombre distinto o `evaluate_quality_gate` no depende directamente de la tarea que publica el valor.

**Corrección:**

1. Confirme que el notebook contiene esta instrucción y que se ejecuta antes de terminar:

   ```python
   dbutils.jobs.taskValues.set(key="quality_status", value=quality_status)
   ```

2. Confirme que la clave coincide exactamente con la referencia de la condición:

   ```text
   {{tasks.validate_orders.values.quality_status}}
   ```

3. Verifique que el nombre técnico de la tarea sea `validate_orders`, no solo su nombre visible.
4. Configure `evaluate_quality_gate` para depender de `validate_orders`.
5. Ejecute nuevamente el Job desde el inicio; los task values pertenecen a una ejecución concreta y no se recuperan de una ejecución anterior.

### Problema 2: El Repair run vuelve a fallar o no genera el archivo para DLT

**Síntomas:**

- `promote_for_dlt` falla otra vez durante el Repair run.
- No existe la ruta `/Volumes/de_training/batch1/landing/dlt_ready/orders_batch_003`.
- La auditoría no registra un evento `DLT_PROMOTION`.

**Causa probable:**

El parámetro `simulate_publish_failure` continúa con valor `true`, el Repair run se lanzó con parámetros incorrectos o la tarea no tiene permiso de escritura sobre el volumen administrado.

**Corrección:**

1. Antes de iniciar el Repair run, establezca explícitamente:

   ```text
   simulate_publish_failure = false
   ```

2. Mantenga el mismo `batch_id` de la ejecución fallida:

   ```text
   batch_id = batch_003
   ```

3. Verifique permisos sobre el volumen:

   ```sql
   SHOW GRANTS ON VOLUME de_training.batch1.landing;
   ```

4. Si faltan permisos, solicite al instructor privilegios de escritura sobre el volumen.
5. Ejecute un nuevo Repair run seleccionando `promote_for_dlt`, no una ejecución completa del Job.

## Limpieza

No elimine los siguientes artefactos, ya que serán utilizados en las Prácticas 8 y 9:

- La ruta preparada para DLT correspondiente a `batch_001`.
- Las tablas `quarantine_orders` y `batch_ingestion_audit`.
- El Job `job_orders_orchestration_b2`.
- Los notebooks `03_validate_orders`, `05_promote_for_dlt` y `06_quarantine_invalid_rows`.

Si el instructor solicita eliminar exclusivamente los datos de prueba de recuperación, ejecute:

```sql
DELETE FROM de_training.batch1.batch_ingestion_audit
WHERE batch_id = 'batch_003';
```

Y elimine únicamente la salida recuperada:

```python
dbutils.fs.rm(
    "/Volumes/de_training/batch1/landing/dlt_ready/orders_batch_003",
    True
)
```

No elimine `batch_001` ni `batch_002`, porque constituyen evidencia funcional para las prácticas posteriores.

## Resumen

En esta práctica se extendió un Job de Databricks Workflows con una compuerta condicional basada en task values. Se implementó una ruta aprobada que prepara datos Delta para DLT y una ruta de observación que registra datos inválidos en cuarentena junto con una alerta de auditoría.

También se comprobó que los reintentos y los Repair runs cumplen propósitos diferentes: los reintentos atienden fallas transitorias de una tarea, mientras que un Repair run permite recuperar una ejecución fallida seleccionando solo las tareas necesarias. El flujo resultante conserva trazabilidad por lote, separa datos inválidos y reduce reprocesamientos innecesarios durante una recuperación operativa.

Recursos oficiales:

- Databricks Workflows: https://docs.databricks.com/en/workflows/index.html
- Parámetros y valores dinámicos en Jobs: https://docs.databricks.com/en/jobs/parameter-value-references.html
- Reparación de ejecuciones de Jobs: https://docs.databricks.com/en/jobs/repair-job-failures.html
- Task values: https://docs.databricks.com/en/jobs/task-values.html
- Tablas Delta en Unity Catalog: https://docs.databricks.com/en/tables/index.html
