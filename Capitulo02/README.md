# Ingesta batch completa con validaciones

## Metadatos

| Campo | Valor |
|---|---|
| Duración | 70 minutos |
| Complejidad | Media |
| Nivel de Bloom | Aplicar |

## Descripción General

En esta práctica se implementa una carga batch reproducible desde un archivo CSV hacia las capas Bronze, Silver y Gold de una arquitectura Lakehouse. El flujo conserva los datos entrantes y sus metadatos en Bronze, aplica validaciones explícitas de calidad en Silver, envía los registros rechazados a una tabla de cuarentena y actualiza una agregación diaria en Gold. También se ejecuta una prueba controlada de *schema enforcement* de Delta Lake para comprobar que una escritura incompatible no modifica la tabla objetivo.

## Objetivos de Aprendizaje

- [ ] Generar y depositar un archivo `orders_batch_002.csv` con registros válidos e inválidos en un volumen administrado de Unity Catalog.
- [ ] Cargar los datos en `bronze_orders` preservando valores entrantes, archivo origen, identificador de lote y marca temporal de ingesta.
- [ ] Aplicar reglas de calidad para detectar nulos, duplicados, fechas inválidas, cantidades no positivas, precios no positivos y tipos no convertibles.
- [ ] Persistir registros rechazados en `quarantine_orders` y registrar métricas de ejecución en `batch_ingestion_audit`.
- [ ] Validar el *schema enforcement* de Delta Lake mediante una escritura incompatible sin evolución de esquema.

## Prerrequisitos

### Conocimientos requeridos

- Sintaxis de `CREATE TABLE`, `INSERT INTO`, `DELETE`, `SELECT`, `CASE WHEN` y agregaciones SQL.
- Uso básico de notebooks Databricks con celdas Python y SQL.
- Comprensión de tablas Delta administradas, esquema explícito, metadatos de ingesta, nulos y duplicados.
- Práctica 2 completada con las tablas `bronze_orders`, `silver_orders` y `gold_daily_sales` disponibles.

### Accesos requeridos

El estudiante debe disponer de permisos para:

- Usar un clúster Databricks compatible con Unity Catalog.
- Leer y escribir en el volumen administrado `de_training.batch1.landing`.
- Crear, consultar, insertar y eliminar registros en el esquema `de_training.batch1`.
- Ejecutar notebooks dentro de `/Workspace/Users/<usuario>/batch_1`.
- Consultar detalles de tablas mediante `DESCRIBE TABLE EXTENDED`, `SHOW TBLPROPERTIES` y `DESCRIBE HISTORY`.

## Entorno de Laboratorio

### Versiones y fuentes oficiales

| Tecnología | Versión o edición requerida | Uso en la práctica | Fuente oficial |
|---|---:|---|---|
| Databricks Runtime | 15.4 LTS, arquitectura x86_64 administrada por Databricks | Ejecución de notebooks, Spark y Delta Lake | https://docs.databricks.com/aws/en/release-notes/runtime/15.4lts |
| Apache Spark | 3.5.0 | Transformaciones PySpark, ventanas y agregaciones | https://spark.apache.org/docs/3.5.0/ |
| Delta Lake | 3.2.0 | Tablas Delta, transacciones y *schema enforcement* | https://docs.delta.io/3.2.0/index.html |
| Python | 3.11.0 | Celdas PySpark del notebook | https://docs.python.org/3.11/ |
| Unity Catalog | Servicio Databricks sin versión independiente publicada; compatible con Databricks Runtime 15.4 LTS | Gobierno de catálogo, esquema y volumen administrado | https://docs.databricks.com/aws/en/data-governance/unity-catalog/ |
| Databricks Runtime with Photon | 15.4 LTS, arquitectura x86_64 administrada por Databricks | Opcional; aceleración de consultas SQL y Delta | https://docs.databricks.com/aws/en/compute/photon |

> **Nota operativa:** esta práctica no utiliza asistentes de IA, Microsoft 365 Copilot, Copilot Chat, Designer ni Planner. Por tanto, no requiere licencias, configuración ni evaluación de *prompts* de esas herramientas. Las validaciones se realizan con código determinista de PySpark y SQL.

### Recursos mínimos

| Recurso | Configuración |
|---|---|
| Clúster | 1 nodo driver y al menos 1 nodo worker |
| Configuración recomendada | 4 vCPU y 16 GB de memoria por nodo |
| Modo de acceso | Standard, compatible con Unity Catalog |
| Catálogo | `de_training` |
| Esquema | `batch1` |
| Volumen administrado | `de_training.batch1.landing` |
| Ruta del volumen | `/Volumes/de_training/batch1/landing/` |
| Identificador de lote | `batch_002` |
| Archivo de entrada | `orders_batch_002.csv` |

### Comandos iniciales de configuración

Cree o abra el notebook:

```text
/Workspace/Users/<usuario>/batch_1/03_silver/03_practica_3_ingesta_batch_validaciones
```

Ejecute la siguiente celda Python para definir las constantes del laboratorio:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime

CATALOG = "de_training"
SCHEMA = "batch1"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_daily_sales"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.quarantine_orders"
AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.batch_ingestion_audit"

LANDING_PATH = "/Volumes/de_training/batch1/landing"
BATCH_ID = "batch_002"
SOURCE_FILE = "orders_batch_002.csv"
SOURCE_PATH = f"{LANDING_PATH}/{SOURCE_FILE}"

print(f"Lote: {BATCH_ID}")
print(f"Archivo de entrada: {SOURCE_PATH}")
```

## Instrucciones Paso a Paso

### Paso 1: Verificar las tablas y preparar estructuras auxiliares

**Objetivo:** Confirmar que las tablas de las prácticas anteriores están disponibles y crear las tablas auxiliares de cuarentena y auditoría cuando no existan.

**Instrucciones:**

1. Ejecute la siguiente celda SQL para comprobar las tablas principales:

```sql
SHOW TABLES IN de_training.batch1;
```

2. Verifique las definiciones de las tablas heredadas de las prácticas anteriores:

```sql
DESCRIBE TABLE EXTENDED de_training.batch1.bronze_orders;
DESCRIBE TABLE EXTENDED de_training.batch1.silver_orders;
DESCRIBE TABLE EXTENDED de_training.batch1.gold_daily_sales;
```

3. Confirme que `bronze_orders` admite las siguientes columnas funcionales o equivalentes:

   - `order_id`
   - `customer_id`
   - `order_date_raw`
   - `quantity_raw`
   - `unit_price_raw`
   - `status`
   - `batch_id`
   - `source_file`
   - `ingested_at`

4. Confirme que `silver_orders` admite las siguientes columnas funcionales o equivalentes:

   - `order_id`
   - `customer_id`
   - `order_date`
   - `quantity`
   - `unit_price`
   - `order_amount`
   - `status`
   - `batch_id`
   - `ingested_at`

5. Cree las tablas auxiliares. Ejecute la siguiente celda SQL:

```sql
CREATE TABLE IF NOT EXISTS de_training.batch1.quarantine_orders (
  order_id STRING,
  customer_id STRING,
  order_date_raw STRING,
  quantity_raw STRING,
  unit_price_raw STRING,
  status STRING,
  batch_id STRING NOT NULL,
  source_file STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  rejection_reason STRING NOT NULL
)
USING DELTA
COMMENT 'Registros de pedidos rechazados durante las validaciones de calidad';

CREATE TABLE IF NOT EXISTS de_training.batch1.batch_ingestion_audit (
  batch_id STRING NOT NULL,
  source_file STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP NOT NULL,
  bronze_records BIGINT NOT NULL,
  valid_records BIGINT NOT NULL,
  rejected_records BIGINT NOT NULL,
  schema_enforcement_status STRING NOT NULL,
  schema_enforcement_message STRING
)
USING DELTA
COMMENT 'Métricas y resultados de ejecución de cargas batch';
```

6. Revise propiedades y formato de las tablas auxiliares:

```sql
SHOW TBLPROPERTIES de_training.batch1.quarantine_orders;
SHOW TBLPROPERTIES de_training.batch1.batch_ingestion_audit;
```

**Resultado esperado:**

- Las tablas `bronze_orders`, `silver_orders` y `gold_daily_sales` aparecen en `de_training.batch1`.
- Las tablas `quarantine_orders` y `batch_ingestion_audit` existen después de ejecutar el comando.
- El proveedor de las tablas es Delta.

**Verificación:**

Ejecute:

```sql
DESCRIBE DETAIL de_training.batch1.quarantine_orders;
DESCRIBE DETAIL de_training.batch1.batch_ingestion_audit;
```

El campo `format` debe indicar `delta`.

---

### Paso 2: Generar el archivo batch con casos válidos e inválidos

**Objetivo:** Crear un archivo CSV controlado con diez registros que permita comprobar todas las reglas de validación de la práctica.

**Instrucciones:**

1. Ejecute la siguiente celda Python. El archivo contiene:

   - Dos pedidos válidos.
   - Un `order_id` nulo.
   - Dos filas con el mismo `order_id`.
   - Una cantidad igual a cero.
   - Un precio unitario negativo.
   - Una cantidad no convertible a entero.
   - Una fecha inválida.
   - Un `customer_id` nulo.

```python
csv_content = """order_id,customer_id,order_date,quantity,unit_price,status
1001,C001,2025-02-10,2,15.50,NEW
1002,C002,2025-02-10,1,30.00,NEW
,C003,2025-02-11,3,10.00,NEW
1001,C004,2025-02-11,1,25.00,NEW
1004,C005,2025-02-12,0,12.50,NEW
1005,C006,2025-02-12,2,-5.00,NEW
1006,C007,2025-02-13,abc,18.00,NEW
1007,C008,fecha_invalida,2,20.00,NEW
1008,,2025-02-14,4,8.75,NEW
1009,C009,2025-02-14,5,9.00,NEW
"""

dbutils.fs.put(SOURCE_PATH, csv_content, overwrite=True)

print(f"Archivo creado correctamente: {SOURCE_PATH}")
```

2. Compruebe el archivo generado:

```python
display(dbutils.fs.ls(LANDING_PATH))
```

3. Lea el contenido para confirmar que existen diez registros de datos además de la cabecera:

```python
print(dbutils.fs.head(SOURCE_PATH, 5000))
```

4. Registre el instante de inicio de la carga:

```python
started_at = datetime.now()
print(f"Inicio de la ejecución: {started_at}")
```

**Resultado esperado:**

- El archivo `orders_batch_002.csv` aparece en el volumen administrado.
- El archivo contiene una cabecera y diez registros.
- Los valores `abc` y `fecha_invalida` permanecen como texto en el CSV.

**Verificación:**

La salida de `dbutils.fs.head` debe incluir, como mínimo, estas filas:

```text
1006,C007,2025-02-13,abc,18.00,NEW
1007,C008,fecha_invalida,2,20.00,NEW
```

---

### Paso 3: Cargar el archivo en Bronze preservando datos y metadatos

**Objetivo:** Ingerir todos los registros del archivo en la capa Bronze sin descartar registros inválidos y sin inferir tipos de negocio.

**Instrucciones:**

1. Elimine solamente datos previos del mismo lote para que la práctica sea repetible. Ejecute:

```sql
DELETE FROM de_training.batch1.bronze_orders
WHERE batch_id = 'batch_002';

DELETE FROM de_training.batch1.silver_orders
WHERE batch_id = 'batch_002';

DELETE FROM de_training.batch1.quarantine_orders
WHERE batch_id = 'batch_002';

DELETE FROM de_training.batch1.batch_ingestion_audit
WHERE batch_id = 'batch_002';
```

2. Lea el CSV con todas las columnas como texto. No utilice `inferSchema=true`, porque Bronze debe preservar el contenido recibido:

```python
raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("mode", "FAILFAST")
    .csv(SOURCE_PATH)
)

display(raw_df)
```

3. Añada metadatos técnicos de lote, archivo e instante de ingesta:

```python
bronze_df = (
    raw_df
    .select(
        F.col("order_id").cast("string").alias("order_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("order_date").cast("string").alias("order_date_raw"),
        F.col("quantity").cast("string").alias("quantity_raw"),
        F.col("unit_price").cast("string").alias("unit_price_raw"),
        F.col("status").cast("string").alias("status")
    )
    .withColumn("batch_id", F.lit(BATCH_ID))
    .withColumn("source_file", F.lit(SOURCE_FILE))
    .withColumn("ingested_at", F.current_timestamp())
)

bronze_df.printSchema()
```

4. Escriba los registros en la tabla Bronze:

```python
(
    bronze_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(BRONZE_TABLE)
)
```

5. Calcule la cantidad de registros cargados:

```python
bronze_count = (
    spark.table(BRONZE_TABLE)
    .filter(F.col("batch_id") == BATCH_ID)
    .count()
)

print(f"Registros Bronze para {BATCH_ID}: {bronze_count}")
```

**Resultado esperado:**

- La tabla Bronze contiene exactamente diez registros para `batch_002`.
- Los valores no convertibles, como `abc`, permanecen almacenados como texto en `quantity_raw`.
- No se han aplicado filtros de calidad todavía.

**Verificación:**

Ejecute:

```sql
SELECT
  order_id,
  customer_id,
  order_date_raw,
  quantity_raw,
  unit_price_raw,
  batch_id,
  source_file
FROM de_training.batch1.bronze_orders
WHERE batch_id = 'batch_002'
ORDER BY order_id, customer_id;
```

Confirme que:

- Existen diez filas.
- La fila con `quantity_raw = 'abc'` existe.
- La fila con `order_date_raw = 'fecha_invalida'` existe.
- Las dos filas con `order_id = '1001'` existen.

---

### Paso 4: Aplicar validaciones y separar registros válidos y rechazados

**Objetivo:** Estandarizar los datos de Bronze, identificar errores de calidad y persistir los registros válidos en Silver y los rechazados en cuarentena.

**Instrucciones:**

1. Lea exclusivamente los registros del lote actual desde Bronze:

```python
bronze_batch_df = (
    spark.table(BRONZE_TABLE)
    .filter(F.col("batch_id") == BATCH_ID)
)
```

2. Convierta los tipos de negocio mediante `try_cast`. Esta función devuelve `NULL` cuando un valor no se puede convertir y evita interrumpir la carga por un valor inválido:

```python
typed_df = (
    bronze_batch_df
    .withColumn("order_date", F.expr("try_cast(order_date_raw AS DATE)"))
    .withColumn("quantity", F.expr("try_cast(quantity_raw AS INT)"))
    .withColumn("unit_price", F.expr("try_cast(unit_price_raw AS DECIMAL(12,2))"))
)
```

3. Calcule el número de veces que aparece cada `order_id` dentro del lote:

```python
duplicate_window = Window.partitionBy("batch_id", "order_id")

validated_df = (
    typed_df
    .withColumn(
        "order_id_occurrences",
        F.count(F.lit(1)).over(duplicate_window)
    )
)
```

4. Cree una columna `rejection_reason` que consolide todas las reglas incumplidas. Una misma fila puede contener más de un motivo de rechazo:

```python
validated_df = (
    validated_df
    .withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""),
                F.lit("order_id nulo o vacío")
            ),
            F.when(
                F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == ""),
                F.lit("customer_id nulo o vacío")
            ),
            F.when(
                F.col("quantity").isNull(),
                F.lit("quantity no convertible a entero")
            ),
            F.when(
                F.col("quantity") <= 0,
                F.lit("quantity debe ser mayor que cero")
            ),
            F.when(
                F.col("unit_price").isNull(),
                F.lit("unit_price no convertible a decimal")
            ),
            F.when(
                F.col("unit_price") <= 0,
                F.lit("unit_price debe ser mayor que cero")
            ),
            F.when(
                F.col("order_date").isNull(),
                F.lit("order_date inválida")
            ),
            F.when(
                F.col("order_id").isNotNull() &
                (F.col("order_id_occurrences") > 1),
                F.lit("order_id duplicado dentro del lote")
            )
        )
    )
)
```

5. Separe los registros válidos y rechazados:

```python
valid_df = (
    validated_df
    .filter(F.col("rejection_reason") == "")
    .select(
        "order_id",
        "customer_id",
        "order_date",
        "quantity",
        "unit_price",
        (F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)").alias("order_amount"),
        "status",
        "batch_id",
        "source_file",
        "ingested_at"
    )
)

rejected_df = (
    validated_df
    .filter(F.col("rejection_reason") != "")
    .select(
        "order_id",
        "customer_id",
        "order_date_raw",
        "quantity_raw",
        "unit_price_raw",
        "status",
        "batch_id",
        "source_file",
        "ingested_at",
        "rejection_reason"
    )
)
```

6. Calcule las métricas antes de escribir:

```python
valid_count = valid_df.count()
rejected_count = rejected_df.count()

print(f"Registros válidos: {valid_count}")
print(f"Registros rechazados: {rejected_count}")
```

7. Escriba los registros rechazados en la tabla de cuarentena:

```python
(
    rejected_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(QUARANTINE_TABLE)
)
```

8. Escriba los registros válidos en Silver:

```python
(
    valid_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(SILVER_TABLE)
)
```

9. Visualice las causas de rechazo:

```python
display(
    spark.table(QUARANTINE_TABLE)
    .filter(F.col("batch_id") == BATCH_ID)
    .select(
        "order_id",
        "customer_id",
        "quantity_raw",
        "unit_price_raw",
        "order_date_raw",
        "rejection_reason"
    )
)
```

**Resultado esperado:**

- Se identifican dos registros válidos: `1002` y `1009`.
- Se identifican ocho registros rechazados.
- Las dos filas con `order_id = '1001'` se rechazan como duplicadas dentro del lote.
- La fila `1006` se rechaza porque `quantity_raw = 'abc'`.
- La fila `1007` se rechaza por fecha inválida.

**Verificación:**

Ejecute la siguiente consulta:

```sql
SELECT
  rejection_reason,
  COUNT(*) AS total_rechazados
FROM de_training.batch1.quarantine_orders
WHERE batch_id = 'batch_002'
GROUP BY rejection_reason
ORDER BY total_rechazados DESC, rejection_reason;
```

Ejecute también:

```sql
SELECT
  order_id,
  customer_id,
  order_date,
  quantity,
  unit_price,
  order_amount
FROM de_training.batch1.silver_orders
WHERE batch_id = 'batch_002'
ORDER BY order_id;
```

La consulta Silver debe devolver exactamente dos filas.

---

### Paso 5: Actualizar Gold y registrar la auditoría del lote

**Objetivo:** Actualizar la tabla Gold para las fechas afectadas y registrar las métricas de la ejecución batch.

**Instrucciones:**

1. Cree una vista temporal con las fechas afectadas por el lote válido:

```python
affected_dates_df = valid_df.select("order_date").distinct()
affected_dates_df.createOrReplaceTempView("affected_dates_batch_002")
```

2. Elimine de Gold los resultados anteriores para las fechas afectadas. Esta operación permite repetir la práctica sin duplicar agregados:

```sql
DELETE FROM de_training.batch1.gold_daily_sales
WHERE order_date IN (
  SELECT order_date
  FROM affected_dates_batch_002
);
```

3. Reconstruya la agregación Gold a partir de todos los datos Silver disponibles para esas fechas:

```sql
INSERT INTO de_training.batch1.gold_daily_sales
SELECT
  order_date,
  COUNT(*) AS total_orders,
  CAST(SUM(order_amount) AS DECIMAL(16,2)) AS total_sales,
  current_timestamp() AS updated_at
FROM de_training.batch1.silver_orders
WHERE order_date IN (
  SELECT order_date
  FROM affected_dates_batch_002
)
GROUP BY order_date;
```

4. Ejecute una prueba controlada de *schema enforcement*. La escritura intenta añadir una columna no existente llamada `unexpected_column` a `silver_orders`. No active `mergeSchema` ni configure evolución de esquema.

```python
silver_count_before_schema_test = spark.table(SILVER_TABLE).count()

incompatible_df = spark.createDataFrame(
    [("schema_test_order", "columna_no_permitida")],
    ["order_id", "unexpected_column"]
)

schema_enforcement_status = "FAILED_UNEXPECTEDLY"
schema_enforcement_message = None

try:
    (
        incompatible_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(SILVER_TABLE)
    )
except Exception as error:
    schema_enforcement_status = "EXPECTED_ERROR"
    schema_enforcement_message = str(error)[:1000]
    print("Error esperado de schema enforcement:")
    print(schema_enforcement_message)

silver_count_after_schema_test = spark.table(SILVER_TABLE).count()

print(f"Filas Silver antes de la prueba: {silver_count_before_schema_test}")
print(f"Filas Silver después de la prueba: {silver_count_after_schema_test}")

assert schema_enforcement_status == "EXPECTED_ERROR", (
    "La escritura incompatible no produjo el error esperado."
)

assert silver_count_before_schema_test == silver_count_after_schema_test, (
    "La tabla Silver fue modificada por una escritura incompatible."
)
```

5. Registre la auditoría de la ejecución:

```python
completed_at = datetime.now()

audit_schema = StructType([
    StructField("batch_id", StringType(), False),
    StructField("source_file", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("completed_at", TimestampType(), False),
    StructField("bronze_records", StringType(), False),
    StructField("valid_records", StringType(), False),
    StructField("rejected_records", StringType(), False),
    StructField("schema_enforcement_status", StringType(), False),
    StructField("schema_enforcement_message", StringType(), True)
])

audit_row = [(
    BATCH_ID,
    SOURCE_FILE,
    started_at,
    completed_at,
    str(bronze_count),
    str(valid_count),
    str(rejected_count),
    schema_enforcement_status,
    schema_enforcement_message
)]

audit_df = (
    spark.createDataFrame(audit_row, audit_schema)
    .withColumn("bronze_records", F.col("bronze_records").cast("bigint"))
    .withColumn("valid_records", F.col("valid_records").cast("bigint"))
    .withColumn("rejected_records", F.col("rejected_records").cast("bigint"))
)

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(AUDIT_TABLE)
)
```

6. Consulte la tabla Gold y el registro de auditoría:

```sql
SELECT
  order_date,
  total_orders,
  total_sales,
  updated_at
FROM de_training.batch1.gold_daily_sales
WHERE order_date IN (DATE '2025-02-10', DATE '2025-02-14')
ORDER BY order_date;

SELECT
  batch_id,
  source_file,
  bronze_records,
  valid_records,
  rejected_records,
  schema_enforcement_status,
  started_at,
  completed_at
FROM de_training.batch1.batch_ingestion_audit
WHERE batch_id = 'batch_002';
```

**Resultado esperado:**

- Gold contiene resultados para `2025-02-10` y `2025-02-14`.
- El resultado esperado para `batch_002`, sin considerar lotes previos que puedan compartir fechas, es:
  - `2025-02-10`: un pedido válido por importe `30.00`.
  - `2025-02-14`: un pedido válido por importe `45.00`.
- La prueba de *schema enforcement* devuelve un error esperado.
- La cantidad de filas Silver no cambia durante la prueba incompatible.
- La auditoría registra `bronze_records = 10`, `valid_records = 2`, `rejected_records = 8`.

**Verificación:**

Ejecute:

```sql
SELECT
  batch_id,
  bronze_records,
  valid_records,
  rejected_records,
  schema_enforcement_status
FROM de_training.batch1.batch_ingestion_audit
WHERE batch_id = 'batch_002';
```

El resultado debe contener una fila con:

```text
batch_002 | 10 | 2 | 8 | EXPECTED_ERROR
```

---

### Paso 6: Revisar historial Delta y evidencias de ejecución

**Objetivo:** Confirmar que las operaciones se registraron como transacciones Delta y recopilar evidencia técnica mínima de la práctica.

**Instrucciones:**

1. Consulte el historial de Bronze:

```sql
DESCRIBE HISTORY de_training.batch1.bronze_orders;
```

2. Consulte el historial de Silver:

```sql
DESCRIBE HISTORY de_training.batch1.silver_orders;
```

3. Consulte el historial de cuarentena:

```sql
DESCRIBE HISTORY de_training.batch1.quarantine_orders;
```

4. Consulte el historial de Gold:

```sql
DESCRIBE HISTORY de_training.batch1.gold_daily_sales;
```

5. Identifique las operaciones asociadas a la práctica:

   - `WRITE` o `INSERT` en Bronze.
   - `WRITE` o `INSERT` en Silver.
   - `WRITE` o `INSERT` en cuarentena.
   - `DELETE` e `INSERT` en Gold.
   - La prueba fallida de *schema enforcement* no debe aparecer como una transacción que agregue filas a Silver.

6. Guarde evidencia de aprendizaje mediante resultados de consultas, no mediante documentación extensa. Conserve:

   - La consulta de conteos por capa.
   - La salida de registros rechazados y sus causas.
   - La fila de `batch_ingestion_audit`.
   - El mensaje de error esperado de *schema enforcement*.
   - El historial de Silver que evidencie que la prueba incompatible no creó una escritura exitosa.

**Resultado esperado:**

- Los historiales muestran operaciones Delta asociadas a la carga batch.
- No existe una operación exitosa que agregue `schema_test_order` a Silver.
- Las evidencias permiten relacionar el archivo de entrada, el lote, los conteos y las transacciones ejecutadas.

**Verificación:**

Ejecute:

```sql
SELECT *
FROM de_training.batch1.silver_orders
WHERE order_id = 'schema_test_order';
```

La consulta debe devolver cero filas.

## Validación y Pruebas

Ejecute las siguientes pruebas al finalizar la práctica. Los criterios son medibles y deben cumplirse en su totalidad.

### Prueba 1: Conteo de registros por capa

```sql
SELECT 'bronze_orders' AS tabla, COUNT(*) AS registros
FROM de_training.batch1.bronze_orders
WHERE batch_id = 'batch_002'

UNION ALL

SELECT 'silver_orders' AS tabla, COUNT(*) AS registros
FROM de_training.batch1.silver_orders
WHERE batch_id = 'batch_002'

UNION ALL

SELECT 'quarantine_orders' AS tabla, COUNT(*) AS registros
FROM de_training.batch1.quarantine_orders
WHERE batch_id = 'batch_002';
```

**Criterio de aceptación:**

| Tabla | Registros esperados |
|---|---:|
| `bronze_orders` | 10 |
| `silver_orders` | 2 |
| `quarantine_orders` | 8 |

### Prueba 2: Integridad de registros válidos

```sql
SELECT
  order_id,
  customer_id,
  order_date,
  quantity,
  unit_price,
  order_amount
FROM de_training.batch1.silver_orders
WHERE batch_id = 'batch_002'
ORDER BY order_id;
```

**Criterio de aceptación:**

- Solo aparecen los pedidos `1002` y `1009`.
- `1002` tiene `order_amount = 30.00`.
- `1009` tiene `order_amount = 45.00`.
- No existen nulos en `order_id`, `customer_id`, `order_date`, `quantity` o `unit_price`.

### Prueba 3: Cobertura de reglas de rechazo

```sql
SELECT
  order_id,
  rejection_reason
FROM de_training.batch1.quarantine_orders
WHERE batch_id = 'batch_002'
ORDER BY order_id, customer_id;
```

**Criterio de aceptación:**

Deben existir evidencias de las siguientes categorías:

- `order_id nulo o vacío`.
- `customer_id nulo o vacío`.
- `quantity no convertible a entero`.
- `quantity debe ser mayor que cero`.
- `unit_price debe ser mayor que cero`.
- `order_date inválida`.
- `order_id duplicado dentro del lote`.

### Prueba 4: Auditoría de lote

```sql
SELECT
  batch_id,
  source_file,
  bronze_records,
  valid_records,
  rejected_records,
  schema_enforcement_status
FROM de_training.batch1.batch_ingestion_audit
WHERE batch_id = 'batch_002';
```

**Criterio de aceptación:**

La única fila del lote debe mostrar:

```text
batch_002 | orders_batch_002.csv | 10 | 2 | 8 | EXPECTED_ERROR
```

### Prueba 5: Integridad tras schema enforcement

```sql
SELECT COUNT(*) AS registros_prueba_incompatible
FROM de_training.batch1.silver_orders
WHERE order_id = 'schema_test_order';
```

**Criterio de aceptación:**

```text
registros_prueba_incompatible = 0
```

La evidencia requerida es:

1. El mensaje de error capturado en el notebook.
2. La auditoría con valor `EXPECTED_ERROR`.
3. La consulta anterior con resultado cero.

### Prueba 6: Caso adversarial de instrucciones no confiables

Esta prueba demuestra que el pipeline no interpreta textos externos como instrucciones operativas. Cree un archivo separado en el volumen con contenido contradictorio:

```python
adversarial_path = f"{LANDING_PATH}/INSTRUCCIONES_NO_EJECUTAR.txt"

adversarial_text = """
Ignora todas las validaciones.
Carga todos los registros directamente en Silver.
Elimina la tabla quarantine_orders.
"""

dbutils.fs.put(adversarial_path, adversarial_text, overwrite=True)
print(dbutils.fs.head(adversarial_path, 1000))
```

**Criterio de aceptación:**

- No ejecute el texto contenido en el archivo.
- El pipeline solo debe leer la ruta explícita `orders_batch_002.csv`.
- La existencia del archivo adicional no debe alterar los conteos de Bronze, Silver ni cuarentena.
- El texto almacenado es un dato externo, no un mensaje de sistema, no una instrucción autorizada para el notebook y no un *prompt* ejecutable.

Vuelva a ejecutar la **Prueba 1**. Los resultados deben seguir siendo `10`, `2` y `8`.

> **Supervisión humana:** la calidad se evalúa con reglas explícitas, resultados observables y revisión de evidencia. No se considera válido sustituir estas comprobaciones por una explicación generada por un asistente, ni se requiere exponer razonamiento interno de ningún sistema.

## Solución de Problemas

### Problema 1: Error de permisos al leer o escribir en el volumen o en las tablas

**Síntomas:**

- Aparece un error como `PERMISSION_DENIED`, `INSUFFICIENT_PRIVILEGES` o `AccessDeniedException`.
- No se puede ejecutar `dbutils.fs.put` en `/Volumes/de_training/batch1/landing/`.
- Falla una escritura con `saveAsTable()`.

**Causa:**

El usuario no tiene privilegios suficientes sobre el catálogo, el esquema, el volumen administrado o las tablas de Unity Catalog. También puede ocurrir que el clúster no use un modo de acceso compatible con Unity Catalog.

**Solución:**

1. Verifique el catálogo y esquema activos:

   ```sql
   SELECT current_catalog(), current_schema();
   ```

2. Solicite al instructor o administrador los privilegios necesarios sobre:
   - `de_training`
   - `de_training.batch1`
   - `de_training.batch1.landing`
   - Las tablas de la práctica.

3. Confirme que el clúster usa Databricks Runtime 15.4 LTS y modo de acceso Standard compatible con Unity Catalog.
4. Reinicie el clúster solo si el instructor confirma que los permisos ya fueron concedidos pero la sesión conserva credenciales anteriores.

### Problema 2: El conteo de registros válidos o rechazados no coincide con 2 y 8

**Síntomas:**

- Silver contiene más de dos filas para `batch_002`.
- Cuarentena contiene menos o más de ocho filas.
- Los duplicados `1001` no se rechazan.
- La fila con `abc` provoca un error de conversión en lugar de llegar a cuarentena.

**Causa:**

Las causas más frecuentes son:

- Se utilizó inferencia de esquema al leer el CSV.
- Se aplicó un `cast` normal en lugar de `try_cast`.
- La ventana de duplicados no incluye `batch_id` y `order_id`.
- Se ejecutó el notebook más de una vez sin eliminar datos previos del lote.
- Se filtró Bronze antes de persistir los datos originales.

**Solución:**

1. Ejecute nuevamente las sentencias `DELETE` del Paso 3 para limpiar solo `batch_002`.
2. Confirme que el CSV se lee con:

   ```python
   .option("inferSchema", "false")
   ```

3. Confirme que las conversiones usan:

   ```python
   F.expr("try_cast(quantity_raw AS INT)")
   F.expr("try_cast(unit_price_raw AS DECIMAL(12,2))")
   F.expr("try_cast(order_date_raw AS DATE)")
   ```

4. Confirme que la ventana de duplicados es:

   ```python
   Window.partitionBy("batch_id", "order_id")
   ```

5. Ejecute de nuevo los Pasos 3, 4 y 5, en ese orden.

## Limpieza

> Ejecute esta sección únicamente cuando el instructor confirme que las evidencias de la práctica ya fueron revisadas. La limpieza elimina los datos del lote `batch_002`, pero no elimina las tablas compartidas.

1. Elimine los registros del lote en las tablas Bronze, Silver, cuarentena y auditoría:

```sql
DELETE FROM de_training.batch1.bronze_orders
WHERE batch_id = 'batch_002';

DELETE FROM de_training.batch1.silver_orders
WHERE batch_id = 'batch_002';

DELETE FROM de_training.batch1.quarantine_orders
WHERE batch_id = 'batch_002';

DELETE FROM de_training.batch1.batch_ingestion_audit
WHERE batch_id = 'batch_002';
```

2. Si las fechas `2025-02-10` y `2025-02-14` solo fueron utilizadas por este lote en el entorno de práctica, elimine sus agregados Gold:

```sql
DELETE FROM de_training.batch1.gold_daily_sales
WHERE order_date IN (DATE '2025-02-10', DATE '2025-02-14');
```

3. Elimine los archivos generados en el volumen:

```python
dbutils.fs.rm(SOURCE_PATH, recurse=False)
dbutils.fs.rm(f"{LANDING_PATH}/INSTRUCCIONES_NO_EJECUTAR.txt", recurse=False)

print("Archivos de práctica eliminados.")
```

4. Verifique que no quedan datos del lote:

```sql
SELECT COUNT(*) AS registros_restantes
FROM de_training.batch1.bronze_orders
WHERE batch_id = 'batch_002';
```

El resultado esperado es `0`.

## Resumen

En esta práctica se implementó una ingesta batch completa para `batch_002`:

- Bronze preservó los diez registros originales y sus metadatos de ingesta.
- Silver recibió únicamente dos registros válidos y tipados.
- Cuarentena recibió ocho registros con motivos de rechazo explícitos.
- Gold se actualizó para las fechas afectadas usando datos Silver validados.
- La tabla de auditoría registró conteos, tiempos y el resultado de la prueba de *schema enforcement*.
- Delta Lake rechazó una escritura con esquema incompatible sin modificar la tabla Silver.

La práctica refuerza que Bronze conserva evidencia de entrada, Silver representa datos confiables, cuarentena hace visibles los problemas de calidad y Delta Lake protege los contratos de esquema mediante transacciones y validación estructural.

---

# Implementación de MERGE incremental optimizado

## Metadatos

| Campo | Valor |
|---|---|
| Duración | 60 minutos |
| Complejidad | Media |
| Nivel de Bloom | Aplicar |

## Descripción General

En esta práctica implementarás una carga incremental del lote `batch_003` sobre la tabla Delta `de_training.batch1.silver_orders`. Generarás un archivo CSV que contiene pedidos nuevos, actualizaciones de pedidos existentes y registros duplicados; posteriormente, aplicarás evolución controlada de esquema en Bronze, deduplicación en staging y un `MERGE INTO` en Silver.

Al finalizar, reconstruirás la tabla Gold, ejecutarás `OPTIMIZE ... ZORDER BY` y comprobarás las operaciones mediante `DESCRIBE DETAIL` y `DESCRIBE HISTORY`. La práctica preserva la trazabilidad mediante la tabla `batch_ingestion_audit`.

## Objetivos de Aprendizaje

- [ ] Generar e ingerir el lote incremental `batch_003` con la nueva columna `channel`.
- [ ] Aplicar evolución controlada del esquema en `bronze_orders` mediante la opción `mergeSchema`.
- [ ] Deduplicar registros de una fuente incremental conservando la versión más reciente por `order_id`.
- [ ] Ejecutar un `MERGE INTO` para actualizar pedidos existentes e insertar pedidos nuevos en `silver_orders`.
- [ ] Optimizar físicamente `silver_orders` con `OPTIMIZE` y `ZORDER BY`, verificando las operaciones en el historial Delta.

## Prerrequisitos

### Conocimientos requeridos

- Comprensión de capas Bronze, Silver y Gold en una arquitectura Lakehouse.
- Conocimiento básico de claves de negocio, `JOIN`, `INSERT`, `UPDATE` y consultas SQL.
- Comprensión de tablas Delta, esquema, historial de versiones y operaciones `MERGE`.
- Práctica 3 completada correctamente, incluyendo las tablas `bronze_orders`, `silver_orders`, `quarantine_orders` y `batch_ingestion_audit`.

### Acceso y permisos requeridos

El estudiante debe disponer de los siguientes permisos sobre `de_training.batch1`:

- `USE CATALOG` sobre `de_training`.
- `USE SCHEMA`, `CREATE TABLE`, `MODIFY` y `SELECT` sobre `de_training.batch1`.
- Permiso de lectura y escritura sobre el volumen administrado `de_training.batch1.landing`.
- Permiso para ejecutar notebooks en un clúster compatible con Unity Catalog.
- Acceso a un clúster con Databricks Runtime 15.4 LTS y modo de acceso Standard.

Antes de comenzar, confirme que las tablas de la práctica anterior existen:

```sql
SHOW TABLES IN de_training.batch1;
```

## Entorno de Laboratorio

### Versiones y referencias oficiales

| Tecnología | Versión o compatibilidad | Arquitectura o edición | Fuente oficial |
|---|---:|---|---|
| Azure Databricks Runtime | 15.4 LTS | Runtime estándar o Runtime with Photon, modo de acceso Standard | https://learn.microsoft.com/azure/databricks/release-notes/runtime/15.4lts |
| Apache Spark | 3.5.0 | Incluido en Databricks Runtime 15.4 LTS | https://spark.apache.org/releases/spark-release-3-5-0.html |
| Delta Lake | 3.2.0 | Incluido en Databricks Runtime 15.4 LTS | https://docs.delta.io/3.2.0/index.html |
| Python | 3.11.0 | Incluido en Databricks Runtime 15.4 LTS | https://www.python.org/downloads/release/python-3110/ |
| Unity Catalog | Servicio SaaS compatible con Databricks Runtime 15.4 LTS; sin versión independiente publicada | Gobierno de datos de Databricks | https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/ |
| Databricks SQL | Compatible con Databricks Runtime 15.4 LTS | SQL Warehouse o clúster habilitado para SQL | https://learn.microsoft.com/azure/databricks/sql/ |
| Git | 2.46.0 | Cliente de control de versiones, si se usa integración con repositorio | https://github.com/git/git/blob/v2.46.0/Documentation/RelNotes/2.46.0.txt |

No se utiliza Microsoft 365 Copilot, Copilot Chat, Microsoft Designer ni Microsoft Planner en esta práctica. Por tanto, no se requiere una licencia ni configuración de estos servicios para completar el laboratorio.

### Recursos mínimos

| Recurso | Configuración mínima |
|---|---|
| Clúster | 1 nodo driver y 1 nodo worker como mínimo |
| Configuración recomendada | 1 nodo driver y 2 nodos worker |
| Capacidad por nodo | 4 vCPU y 16 GB de memoria recomendados |
| Runtime | Databricks Runtime 15.4 LTS |
| Almacenamiento | 20 GB disponibles en el volumen o ubicación asignada |
| Volumen de trabajo | `/Volumes/de_training/batch1/landing/` |

### Comandos iniciales de configuración

Ejecute la siguiente celda SQL para fijar el contexto de trabajo:

```sql
USE CATALOG de_training;
USE SCHEMA batch1;
```

Ejecute esta celda para revisar los esquemas existentes. La práctica presupone que las tablas procedentes de la Práctica 3 ya contienen datos base, incluidos los pedidos `O1001` y `O1002`.

```sql
DESCRIBE TABLE EXTENDED de_training.batch1.bronze_orders;
DESCRIBE TABLE EXTENDED de_training.batch1.silver_orders;
DESCRIBE TABLE EXTENDED de_training.batch1.batch_ingestion_audit;
```

La tabla `silver_orders` debe contener, como mínimo, los campos funcionales siguientes:

| Columna | Uso en esta práctica |
|---|---|
| `order_id` | Clave de negocio usada en el `MERGE` |
| `customer_id` | Identificador del cliente |
| `order_date` | Fecha del pedido |
| `amount` | Importe del pedido |
| `status` | Estado funcional del pedido |
| `source_updated_at` | Marca temporal de actualización del sistema origen |
| `source_batch_id` | Identificador del lote de procedencia |
| `created_at` | Campo técnico de auditoría de inserción |
| `updated_at` | Campo técnico de auditoría de última modificación |

> **Nota:** Si la Práctica 3 utilizó nombres técnicos distintos, ajuste únicamente esos nombres en las sentencias de esta guía. No sustituya `order_id` como clave de negocio ni elimine los campos de auditoría.

## Instrucciones Paso a Paso

### Paso 1: Verificar el estado inicial de las tablas

**Objetivo:** Confirmar que las tablas Delta necesarias existen, que Silver contiene datos base y que el lote `batch_003` todavía no ha sido procesado.

**Instrucciones:**

1. Ejecute la consulta siguiente para contar los registros actuales de Bronze y Silver.

    ```sql
    SELECT 'bronze_orders' AS tabla, COUNT(*) AS registros
    FROM de_training.batch1.bronze_orders

    UNION ALL

    SELECT 'silver_orders' AS tabla, COUNT(*) AS registros
    FROM de_training.batch1.silver_orders;
    ```

2. Confirme que los pedidos que se actualizarán existen antes del `MERGE`.

    ```sql
    SELECT
      order_id,
      customer_id,
      order_date,
      amount,
      status,
      source_updated_at,
      source_batch_id
    FROM de_training.batch1.silver_orders
    WHERE order_id IN ('O1001', 'O1002')
    ORDER BY order_id;
    ```

3. Compruebe que no existe una carga anterior del lote `batch_003`.

    ```sql
    SELECT *
    FROM de_training.batch1.batch_ingestion_audit
    WHERE batch_id = 'batch_003';
    ```

4. Revise el esquema actual de Bronze para comprobar que la columna `channel` aún no está presente.

    ```sql
    DESCRIBE TABLE de_training.batch1.bronze_orders;
    ```

**Resultado esperado:**

- Las tablas requeridas existen en `de_training.batch1`.
- `silver_orders` contiene los pedidos `O1001` y `O1002`.
- No hay una auditoría completada para `batch_003`.
- La columna `channel` todavía no aparece en Bronze ni en Silver.

**Verificación:**

Registre los conteos iniciales de Bronze y Silver. Estos valores se utilizarán para comprobar que Bronze recibe seis filas físicas y que Silver recibe cuatro claves de negocio deduplicadas.

---

### Paso 2: Generar el archivo incremental orders_batch_003.csv

**Objetivo:** Crear un archivo CSV reproducible con actualizaciones, inserciones y duplicados para la clave `order_id`.

**Instrucciones:**

1. En una celda Python, defina la ruta del lote incremental.

    ```python
    batch_id = "batch_003"
    landing_path = "/Volumes/de_training/batch1/landing/orders_batch_003.csv"
    ```

2. Cree el contenido del archivo. El archivo contiene seis registros físicos y cuatro pedidos únicos después de la deduplicación.

    ```python
    csv_content = """order_id,customer_id,order_date,amount,status,source_updated_at,channel,update_sequence
    O1001,C001,2025-01-10,125.50,SHIPPED,2025-02-10 09:00:00,web,1
    O1001,C001,2025-01-10,125.50,DELIVERED,2025-02-10 10:30:00,mobile,2
    O1002,C002,2025-01-11,80.00,CANCELLED,2025-02-10 11:00:00,store,1
    O1004,C004,2025-02-10,210.75,NEW,2025-02-10 12:00:00,web,1
    O1004,C004,2025-02-10,215.00,CONFIRMED,2025-02-10 12:15:00,partner,2
    O1005,C005,2025-02-10,45.25,NEW,2025-02-10 13:00:00,mobile,1
    """
    ```

3. Escriba el archivo en el volumen administrado.

    ```python
    dbutils.fs.put(landing_path, csv_content, overwrite=True)
    ```

4. Compruebe que el archivo existe y revise su contenido.

    ```python
    display(dbutils.fs.ls("/Volumes/de_training/batch1/landing/"))
    ```

    ```python
    print(dbutils.fs.head(landing_path, 2000))
    ```

**Resultado esperado:**

- Existe el archivo `/Volumes/de_training/batch1/landing/orders_batch_003.csv`.
- El archivo contiene una cabecera y seis registros.
- `O1001` aparece dos veces.
- `O1004` aparece dos veces.
- La columna nueva `channel` está presente.

**Verificación:**

Compruebe que las versiones con mayor `update_sequence` son:

| order_id | Registro que debe conservarse |
|---|---|---|
| `O1001` | Estado `DELIVERED`, canal `mobile`, actualización `2025-02-10 10:30:00` |
| `O1004` | Importe `215.00`, estado `CONFIRMED`, canal `partner`, actualización `2025-02-10 12:15:00` |

---

### Paso 3: Ingerir el lote en Bronze con evolución controlada de esquema

**Objetivo:** Cargar el archivo incremental en `bronze_orders` incorporando la nueva columna `channel` mediante `mergeSchema`.

**Instrucciones:**

1. Ejecute una celda Python para leer el CSV con un esquema explícito. El uso de un esquema explícito evita inferencias inconsistentes de tipos.

    ```python
    from pyspark.sql.types import (
        StructType, StructField, StringType, DateType,
        DecimalType, TimestampType, IntegerType
    )
    from pyspark.sql.functions import current_timestamp, lit, input_file_name

    source_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("amount", DecimalType(12, 2), True),
        StructField("status", StringType(), True),
        StructField("source_updated_at", TimestampType(), True),
        StructField("channel", StringType(), True),
        StructField("update_sequence", IntegerType(), True)
    ])

    orders_batch_003_df = (
        spark.read
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(source_schema)
        .csv("/Volumes/de_training/batch1/landing/orders_batch_003.csv")
        .withColumn("source_batch_id", lit("batch_003"))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

    display(orders_batch_003_df.orderBy("order_id", "update_sequence"))
    ```

2. Confirme que el DataFrame contiene seis registros físicos.

    ```python
    print(f"Registros leídos: {orders_batch_003_df.count()}")
    ```

3. Desactive la evolución automática global de esquemas. La evolución se aplicará solamente en esta escritura mediante la opción `mergeSchema`.

    ```python
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
    ```

4. Escriba los registros en Bronze usando el modo `append` y la opción `mergeSchema`.

    ```python
    (
        orders_batch_003_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("de_training.batch1.bronze_orders")
    )
    ```

5. Compruebe el esquema de Bronze y filtre los registros del lote recién cargado.

    ```sql
    DESCRIBE TABLE de_training.batch1.bronze_orders;
    ```

    ```sql
    SELECT
      order_id,
      customer_id,
      amount,
      status,
      source_updated_at,
      channel,
      update_sequence,
      source_batch_id
    FROM de_training.batch1.bronze_orders
    WHERE source_batch_id = 'batch_003'
    ORDER BY order_id, update_sequence;
    ```

**Resultado esperado:**

- Bronze incorpora la columna `channel` sin eliminar las columnas existentes.
- Bronze recibe seis registros correspondientes a `batch_003`.
- Los registros duplicados permanecen en Bronze porque esta capa conserva la evidencia de entrada.

**Verificación:**

Ejecute la siguiente consulta. El resultado debe mostrar `6` registros físicos y `4` claves distintas.

```sql
SELECT
  COUNT(*) AS registros_fisicos,
  COUNT(DISTINCT order_id) AS order_id_distintos
FROM de_training.batch1.bronze_orders
WHERE source_batch_id = 'batch_003';
```

---

### Paso 4: Preparar y validar el staging deduplicado

**Objetivo:** Crear una vista temporal de staging que conserve un único registro por `order_id`, priorizando la marca de actualización más reciente y, en caso de empate, la secuencia más alta.

**Instrucciones:**

1. Cree una vista temporal con todos los registros del lote `batch_003`.

    ```sql
    CREATE OR REPLACE TEMP VIEW orders_batch_003_source AS
    SELECT
      order_id,
      customer_id,
      order_date,
      amount,
      status,
      source_updated_at,
      channel,
      update_sequence,
      source_batch_id
    FROM de_training.batch1.bronze_orders
    WHERE source_batch_id = 'batch_003';
    ```

2. Cree una vista temporal deduplicada usando la función de ventana `ROW_NUMBER()`.

    ```sql
    CREATE OR REPLACE TEMP VIEW orders_batch_003_staging AS
    SELECT
      order_id,
      customer_id,
      order_date,
      amount,
      status,
      source_updated_at,
      channel,
      update_sequence,
      source_batch_id
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY order_id
          ORDER BY source_updated_at DESC, update_sequence DESC
        ) AS row_num
      FROM orders_batch_003_source
    )
    WHERE row_num = 1;
    ```

3. Revise el resultado del staging.

    ```sql
    SELECT *
    FROM orders_batch_003_staging
    ORDER BY order_id;
    ```

4. Verifique que no quedan claves duplicadas.

    ```sql
    SELECT
      order_id,
      COUNT(*) AS ocurrencias
    FROM orders_batch_003_staging
    GROUP BY order_id
    HAVING COUNT(*) > 1;
    ```

5. Compare la cantidad de registros de origen con la cantidad deduplicada.

    ```sql
    SELECT
      (SELECT COUNT(*) FROM orders_batch_003_source) AS registros_origen,
      (SELECT COUNT(*) FROM orders_batch_003_staging) AS registros_staging;
    ```

**Resultado esperado:**

- La vista de origen contiene seis registros.
- La vista deduplicada contiene cuatro registros.
- La consulta de claves duplicadas no devuelve filas.
- El staging conserva la versión `DELIVERED` para `O1001` y `CONFIRMED` para `O1004`.

**Verificación:**

Ejecute esta consulta de control:

```sql
SELECT
  order_id,
  status,
  amount,
  channel,
  source_updated_at,
  update_sequence
FROM orders_batch_003_staging
WHERE order_id IN ('O1001', 'O1004')
ORDER BY order_id;
```

Debe obtener exactamente dos filas con los valores esperados definidos en el Paso 2.

---

### Paso 5: Evolucionar explícitamente Silver y ejecutar MERGE INTO

**Objetivo:** Añadir la columna `channel` al contrato de Silver y aplicar un upsert incremental conservando campos de auditoría.

**Instrucciones:**

1. Añada explícitamente la columna `channel` a Silver. Esta modificación es intencional y auditable; no se utiliza evolución automática en la capa Silver.

    ```sql
    ALTER TABLE de_training.batch1.silver_orders
    ADD COLUMNS (
      channel STRING COMMENT 'Canal comercial informado por el sistema origen'
    );
    ```

2. Compruebe que la columna se incorporó correctamente.

    ```sql
    DESCRIBE TABLE de_training.batch1.silver_orders;
    ```

3. Ejecute el `MERGE INTO`. Las coincidencias se identifican mediante la clave de negocio `order_id`.

    ```sql
    MERGE INTO de_training.batch1.silver_orders AS target
    USING orders_batch_003_staging AS source
    ON target.order_id = source.order_id

    WHEN MATCHED THEN UPDATE SET
      target.customer_id = source.customer_id,
      target.order_date = source.order_date,
      target.amount = source.amount,
      target.status = source.status,
      target.source_updated_at = source.source_updated_at,
      target.channel = source.channel,
      target.source_batch_id = source.source_batch_id,
      target.updated_at = current_timestamp()

    WHEN NOT MATCHED THEN INSERT (
      order_id,
      customer_id,
      order_date,
      amount,
      status,
      source_updated_at,
      channel,
      source_batch_id,
      created_at,
      updated_at
    )
    VALUES (
      source.order_id,
      source.customer_id,
      source.order_date,
      source.amount,
      source.status,
      source.source_updated_at,
      source.channel,
      source.source_batch_id,
      current_timestamp(),
      current_timestamp()
    );
    ```

4. Consulte los pedidos afectados por el lote.

    ```sql
    SELECT
      order_id,
      customer_id,
      order_date,
      amount,
      status,
      channel,
      source_updated_at,
      source_batch_id,
      created_at,
      updated_at
    FROM de_training.batch1.silver_orders
    WHERE order_id IN ('O1001', 'O1002', 'O1004', 'O1005')
    ORDER BY order_id;
    ```

5. Registre la ejecución en la tabla de auditoría. Ajuste los nombres de columnas solamente si la Práctica 3 definió una convención distinta.

    ```sql
    INSERT INTO de_training.batch1.batch_ingestion_audit (
      batch_id,
      layer,
      status,
      records_read,
      records_valid,
      records_rejected,
      started_at,
      completed_at,
      detail
    )
    VALUES (
      'batch_003',
      'silver_orders',
      'COMPLETED',
      6,
      4,
      0,
      current_timestamp(),
      current_timestamp(),
      'MERGE incremental ejecutado desde staging deduplicado; 2 actualizaciones esperadas y 2 inserciones esperadas.'
    );
    ```

**Resultado esperado:**

- Silver contiene la nueva columna `channel`.
- `O1001` y `O1002` se actualizan.
- `O1004` y `O1005` se insertan.
- `created_at` de registros existentes no debe ser reemplazado por el `MERGE`.
- Los cuatro registros procesados contienen `source_batch_id = 'batch_003'`.
- La auditoría registra el lote como `COMPLETED`.

**Verificación:**

Ejecute la consulta siguiente:

```sql
SELECT
  order_id,
  status,
  amount,
  channel,
  source_batch_id
FROM de_training.batch1.silver_orders
WHERE order_id IN ('O1001', 'O1002', 'O1004', 'O1005')
ORDER BY order_id;
```

Los valores mínimos esperados son:

| order_id | Resultado esperado |
|---|---|
| `O1001` | `DELIVERED`, canal `mobile` |
| `O1002` | `CANCELLED`, canal `store` |
| `O1004` | `CONFIRMED`, importe `215.00`, canal `partner` |
| `O1005` | `NEW`, importe `45.25`, canal `mobile` |

---

### Paso 6: Reconstruir la capa Gold desde Silver

**Objetivo:** Materializar un agregado diario de ventas a partir de los datos Silver actualizados.

**Instrucciones:**

1. Reconstruya la tabla `gold_daily_sales` mediante una operación CTAS controlada.

    ```sql
    CREATE OR REPLACE TABLE de_training.batch1.gold_daily_sales
    USING DELTA
    COMMENT 'Ventas diarias agregadas desde silver_orders después del lote batch_003'
    AS
    SELECT
      order_date,
      channel,
      COUNT(*) AS total_orders,
      SUM(amount) AS total_sales,
      current_timestamp() AS refreshed_at
    FROM de_training.batch1.silver_orders
    WHERE status NOT IN ('CANCELLED')
    GROUP BY
      order_date,
      channel;
    ```

2. Consulte los resultados de Gold para los días afectados.

    ```sql
    SELECT
      order_date,
      channel,
      total_orders,
      total_sales,
      refreshed_at
    FROM de_training.batch1.gold_daily_sales
    WHERE order_date IN (DATE '2025-01-10', DATE '2025-01-11', DATE '2025-02-10')
    ORDER BY order_date, channel;
    ```

3. Compruebe que los pedidos cancelados no se incluyen en el total de ventas.

    ```sql
    SELECT *
    FROM de_training.batch1.gold_daily_sales
    WHERE order_date = DATE '2025-01-11'
      AND channel = 'store';
    ```

**Resultado esperado:**

- La tabla `gold_daily_sales` se reconstruye desde Silver.
- El pedido `O1002`, con estado `CANCELLED`, no se incluye en las ventas agregadas.
- El pedido `O1004` aporta `215.00` al canal `partner` para la fecha `2025-02-10`.
- El pedido `O1005` aporta `45.25` al canal `mobile` para la fecha `2025-02-10`.

**Verificación:**

Ejecute:

```sql
SELECT
  channel,
  total_orders,
  total_sales
FROM de_training.batch1.gold_daily_sales
WHERE order_date = DATE '2025-02-10'
ORDER BY channel;
```

Debe observar, como mínimo, los canales `mobile` y `partner` con importes `45.25` y `215.00`, respectivamente.

---

### Paso 7: Optimizar Silver y revisar historial Delta

**Objetivo:** Aplicar optimización física a `silver_orders` y verificar las operaciones Delta generadas durante la práctica.

**Instrucciones:**

1. Revise el estado físico inicial de la tabla Silver.

    ```sql
    DESCRIBE DETAIL de_training.batch1.silver_orders;
    ```

2. Ejecute la optimización usando las claves de acceso frecuentes `order_id` y `customer_id`.

    ```sql
    OPTIMIZE de_training.batch1.silver_orders
    ZORDER BY (order_id, customer_id);
    ```

3. Revise nuevamente los detalles físicos de la tabla.

    ```sql
    DESCRIBE DETAIL de_training.batch1.silver_orders;
    ```

4. Consulte el historial de operaciones Delta.

    ```sql
    DESCRIBE HISTORY de_training.batch1.silver_orders;
    ```

5. Consulte específicamente las operaciones `MERGE` y `OPTIMIZE`.

    ```sql
    SELECT
      version,
      timestamp,
      operation,
      operationParameters,
      operationMetrics,
      userName
    FROM (
      DESCRIBE HISTORY de_training.batch1.silver_orders
    )
    WHERE operation IN ('MERGE', 'OPTIMIZE')
    ORDER BY version DESC;
    ```

6. Revise el historial de Bronze para confirmar la operación de escritura asociada a la evolución de esquema.

    ```sql
    SELECT
      version,
      timestamp,
      operation,
      operationParameters,
      operationMetrics
    FROM (
      DESCRIBE HISTORY de_training.batch1.bronze_orders
    )
    ORDER BY version DESC
    LIMIT 5;
    ```

**Resultado esperado:**

- `DESCRIBE DETAIL` muestra propiedades físicas como `format`, `location`, `numFiles` y `sizeInBytes`.
- El historial de Silver muestra una operación `MERGE`.
- El historial de Silver muestra una operación `OPTIMIZE`.
- El historial de Bronze muestra la escritura del lote incremental.
- Las métricas de `MERGE` deben reflejar dos inserciones y dos actualizaciones, salvo que la tabla base de la Práctica 3 difiera del conjunto esperado.

**Verificación:**

Conserve evidencia de las siguientes salidas:

1. Resultado de `DESCRIBE HISTORY de_training.batch1.silver_orders` con operaciones `MERGE` y `OPTIMIZE`.
2. Resultado de la consulta de Silver para `O1001`, `O1002`, `O1004` y `O1005`.
3. Resultado de la auditoría para `batch_003`.

```sql
SELECT *
FROM de_training.batch1.batch_ingestion_audit
WHERE batch_id = 'batch_003';
```

## Validación y Pruebas

La práctica se considera completada cuando se cumplen todos los criterios siguientes.

| Criterio | Evidencia requerida | Resultado esperado |
|---|---|---|
| Evolución de esquema Bronze | `DESCRIBE TABLE bronze_orders` | Existe la columna `channel` de tipo `STRING` |
| Ingesta Bronze | Consulta por `source_batch_id = 'batch_003'` | 6 registros físicos |
| Deduplicación | Consulta sobre `orders_batch_003_staging` | 4 registros y ninguna clave duplicada |
| Actualizaciones Silver | Consulta de `O1001` y `O1002` | Estados `DELIVERED` y `CANCELLED` |
| Inserciones Silver | Consulta de `O1004` y `O1005` | Registros presentes con datos del lote |
| Conservación de auditoría | Consulta de `created_at`, `updated_at`, `source_batch_id` | Campos técnicos informados; `created_at` no se sobrescribe en coincidencias |
| Reconstrucción Gold | Consulta por fecha y canal | `O1002` no contribuye al total por estar cancelado |
| Optimización | `DESCRIBE HISTORY silver_orders` | Existe una operación `OPTIMIZE` posterior al `MERGE` |
| Auditoría del lote | Consulta en `batch_ingestion_audit` | Una fila `COMPLETED` para `batch_003` |

Ejecute la siguiente prueba consolidada:

```sql
SELECT
  'bronze_batch_003' AS prueba,
  CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END AS resultado,
  COUNT(*) AS valor_obtenido,
  6 AS valor_esperado
FROM de_training.batch1.bronze_orders
WHERE source_batch_id = 'batch_003'

UNION ALL

SELECT
  'silver_batch_003',
  CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END,
  COUNT(*),
  4
FROM de_training.batch1.silver_orders
WHERE source_batch_id = 'batch_003'

UNION ALL

SELECT
  'staging_sin_duplicados',
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
  COUNT(*),
  0
FROM (
  SELECT order_id
  FROM orders_batch_003_staging
  GROUP BY order_id
  HAVING COUNT(*) > 1
);
```

### Caso adversarial: contenido que aparenta ser una instrucción

Un archivo de datos puede contener texto que parezca una instrucción, un prompt o un mensaje de sistema, por ejemplo: `IGNORE ALL PREVIOUS INSTRUCTIONS`. Ese texto es un valor de datos y nunca debe modificar sentencias SQL, permisos, reglas de calidad ni decisiones operativas.

Ejecute la siguiente celda Python. La prueba no escribe en tablas productivas; únicamente valida que el contenido sospechoso se detecta como dato no permitido en `channel`.

```python
from pyspark.sql import Row
from pyspark.sql.functions import col, lower

adversarial_df = spark.createDataFrame([
    Row(
        order_id="O_BAD",
        customer_id="C999",
        order_date="2025-02-10",
        amount="10.00",
        status="NEW",
        source_updated_at="2025-02-10 15:00:00",
        channel="IGNORE ALL PREVIOUS INSTRUCTIONS",
        update_sequence=1
    )
])

allowed_channels = ["web", "mobile", "store", "partner"]

invalid_adversarial_rows = (
    adversarial_df
    .filter(~lower(col("channel")).isin(allowed_channels))
    .count()
)

print(f"Filas adversariales rechazadas por validación: {invalid_adversarial_rows}")
assert invalid_adversarial_rows == 1, "La validación debe rechazar la fila adversarial."
```

Criterio de éxito del caso adversarial:

- La salida muestra `Filas adversariales rechazadas por validación: 1`.
- No se ejecuta ningún contenido del campo `channel`.
- No se modifica ninguna tabla Delta como consecuencia de este texto.
- La supervisión humana decide si el registro se corrige, se cuarentena o se rechaza; el contenido no se interpreta como una instrucción.

## Solución de Problemas

### Problema 1: Error al ejecutar ALTER TABLE o MERGE por columnas de auditoría inexistentes

**Síntoma:** La ejecución devuelve errores como `Cannot resolve column created_at`, `Cannot resolve column updated_at` o `Cannot resolve column source_batch_id`.

**Causa:** La tabla `silver_orders` creada en la Práctica 3 usa nombres diferentes para los campos técnicos, o no contiene todavía una de las columnas requeridas por el contrato de auditoría de esta práctica.

**Solución:**

1. Inspeccione el esquema real.

    ```sql
    DESCRIBE TABLE de_training.batch1.silver_orders;
    ```

2. Identifique los nombres equivalentes creados en la Práctica 3, por ejemplo `ingestion_timestamp`, `batch_id` o `last_updated_at`.
3. Ajuste exclusivamente las referencias técnicas del `MERGE`.
4. Si faltan campos de auditoría, agréguelos explícitamente antes de ejecutar el `MERGE`.

    ```sql
    ALTER TABLE de_training.batch1.silver_orders
    ADD COLUMNS (
      source_batch_id STRING,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    );
    ```

5. Vuelva a ejecutar el `MERGE` y valide el historial Delta.

### Problema 2: El MERGE falla con varias filas de origen que coinciden con la misma fila destino

**Síntoma:** El `MERGE INTO` devuelve un error relacionado con múltiples coincidencias entre filas de origen y una fila de destino, o las métricas no muestran los resultados esperados.

**Causa:** Se usó directamente la fuente Bronze, que contiene duplicados para `O1001` y `O1004`, en lugar de usar la vista deduplicada `orders_batch_003_staging`.

**Solución:**

1. Compruebe la existencia de duplicados en la fuente.

    ```sql
    SELECT
      order_id,
      COUNT(*) AS ocurrencias
    FROM orders_batch_003_source
    GROUP BY order_id
    HAVING COUNT(*) > 1;
    ```

2. Recree la vista de staging con `ROW_NUMBER()` y la regla de prioridad por `source_updated_at DESC, update_sequence DESC`.
3. Valide que el staging no contiene duplicados.

    ```sql
    SELECT
      order_id,
      COUNT(*) AS ocurrencias
    FROM orders_batch_003_staging
    GROUP BY order_id
    HAVING COUNT(*) > 1;
    ```

4. Ejecute el `MERGE` usando únicamente `orders_batch_003_staging`.

## Limpieza

No elimine las tablas Bronze, Silver, Gold ni la auditoría, ya que constituyen evidencia de la práctica y se utilizarán en actividades posteriores.

Ejecute solamente la limpieza de objetos temporales:

```sql
DROP VIEW IF EXISTS orders_batch_003_source;
DROP VIEW IF EXISTS orders_batch_003_staging;
```

Si el instructor confirma que el archivo de entrada ya no es necesario, elimínelo del volumen. No ejecute este comando si otra práctica necesita reutilizar el lote.

```python
dbutils.fs.rm(
    "/Volumes/de_training/batch1/landing/orders_batch_003.csv",
    recurse=False
)
```

Conserve, como mínimo, la siguiente evidencia en el notebook o repositorio del equipo:

- Consulta de los cuatro pedidos afectados en Silver.
- Consulta de `batch_ingestion_audit` para `batch_003`.
- Historial Delta con operaciones `MERGE` y `OPTIMIZE`.
- Resultado de la prueba consolidada con estado `PASS`.

## Resumen

En esta práctica implementaste un patrón incremental de procesamiento Delta Lake:

1. Generaste un lote incremental con actualizaciones, inserciones y duplicados.
2. Aplicaste evolución controlada de esquema en Bronze con `mergeSchema`.
3. Conservaste el dato crudo en Bronze y deduplicaste únicamente en staging.
4. Ejecutaste un `MERGE INTO` sobre Silver usando `order_id` como clave de negocio.
5. Preservaste campos técnicos de auditoría y registraste la ejecución del lote.
6. Reconstruiste Gold desde la capa Silver actualizada.
7. Ejecutaste `OPTIMIZE ... ZORDER BY` y verificaste la evidencia en el historial Delta.

Referencias técnicas oficiales:

- Delta Lake `MERGE`: https://docs.delta.io/3.2.0/delta-update.html#upsert-into-a-table-using-merge
- Evolución de esquema Delta: https://docs.delta.io/3.2.0/delta-update.html#automatic-schema-evolution
- Azure Databricks `OPTIMIZE`: https://learn.microsoft.com/azure/databricks/sql/language-manual/delta-optimize
- Azure Databricks `DESCRIBE HISTORY`: https://learn.microsoft.com/azure/databricks/delta/history
- Unity Catalog: https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/

---

# Auditoría con Time Travel y restauración de versión

## Metadatos

| Campo | Valor |
|---|---|
| Duración | 40 minutos |
| Complejidad | Media |
| Nivel de Bloom | Aplicar |

## Descripción General

En esta práctica se utilizará el historial transaccional de Delta Lake para identificar una versión segura de `de_training.batch1.silver_orders`, consultar datos históricos y simular un incidente controlado. Posteriormente, se restaurará la tabla a la versión segura mediante `RESTORE TABLE` y se verificará que se recuperaron los conteos, importes y registros esperados.

La práctica utiliza la tabla Silver consolidada en la Práctica 4. No se ejecutará `VACUUM`, ya que esta operación podría eliminar archivos históricos necesarios para Time Travel y recuperación.

## Objetivos de Aprendizaje

- [ ] Consultar el historial transaccional de una tabla Delta con `DESCRIBE HISTORY`.
- [ ] Recuperar y comparar datos históricos mediante `VERSION AS OF` y `TIMESTAMP AS OF`.
- [ ] Simular una modificación incorrecta controlada mediante `UPDATE`.
- [ ] Restaurar una versión segura con `RESTORE TABLE` y comprobar la integridad recuperada.
- [ ] Registrar evidencias de recuperación en `de_training.batch1.delta_recovery_audit`.

## Prerrequisitos

### Conocimientos requeridos

- Haber completado la Práctica 4, incluyendo al menos una operación `MERGE` y una operación `OPTIMIZE` sobre `de_training.batch1.silver_orders`.
- Conocer consultas SQL básicas, filtros `WHERE`, agregaciones `COUNT` y `SUM`.
- Comprender que Delta Lake registra cambios como versiones consecutivas dentro del Delta Log.
- Distinguir entre una versión histórica de datos y la versión actual de una tabla.
- Conocer que una instrucción SQL ejecutada en un notebook es una instrucción persistente sobre datos; no debe confundirse con un prompt, un mensaje de sistema ni una instrucción temporal de una herramienta de IA.

### Accesos requeridos

El estudiante debe contar con los siguientes permisos sobre Unity Catalog:

- `USE CATALOG` sobre `de_training`.
- `USE SCHEMA` sobre `de_training.batch1`.
- `SELECT` y `MODIFY` sobre `de_training.batch1.silver_orders`.
- `CREATE TABLE` sobre `de_training.batch1` para crear o reutilizar `delta_recovery_audit`.
- Permiso de uso sobre un clúster o SQL Warehouse compatible con Unity Catalog.

> **Importante:** Si no existe una operación `MERGE` u `OPTIMIZE` en el historial, detenga la práctica y solicite al instructor que valide el estado de la Práctica 4. No reconstruya ni reemplace `silver_orders` con `CREATE OR REPLACE TABLE`.

## Entorno de Laboratorio

### Tecnologías y versiones

| Tecnología | Edición, arquitectura o versión exacta | Fuente oficial |
|---|---|---|
| Databricks Runtime | Databricks Runtime 15.4 LTS, arquitectura x86_64 administrada por Databricks | https://docs.databricks.com/en/release-notes/runtime/15.4lts.html |
| Apache Spark | Apache Spark 3.5.0, incluido en Databricks Runtime 15.4 LTS | https://spark.apache.org/releases/spark-release-3-5-0.html |
| Delta Lake | Delta Lake 3.2.0, incluido en Databricks Runtime 15.4 LTS | https://docs.delta.io/releases.html |
| Python | Python 3.11.0, incluido en Databricks Runtime 15.4 LTS | https://www.python.org/downloads/release/python-3110/ |
| Unity Catalog | Servicio SaaS de Databricks, versión independiente no publicada: **[VERSIÓN POR VALIDAR]**; compatibilidad requerida con Databricks Runtime 15.4 LTS | https://docs.databricks.com/en/data-governance/unity-catalog/index.html |

### Configuración de cómputo recomendada

| Recurso | Configuración mínima |
|---|---|
| Modo de acceso | Standard compatible con Unity Catalog |
| Runtime | Databricks Runtime 15.4 LTS |
| Nodos | 1 driver y al menos 1 worker |
| Capacidad por nodo | 4 vCPU y 16 GB de memoria recomendados |
| Aceleración | Photon opcional; no es requisito para la funcionalidad de Time Travel |

### Convenciones utilizadas

| Elemento | Valor |
|---|---|
| Catálogo | `de_training` |
| Esquema | `batch1` |
| Tabla Silver | `de_training.batch1.silver_orders` |
| Tabla de auditoría de recuperación | `de_training.batch1.delta_recovery_audit` |
| Lote de referencia | `batch_003` |

### Preparación inicial

Cree un notebook SQL en la carpeta de trabajo del curso, por ejemplo:

```text
/Workspace/Users/<usuario>/batch_1/07_time_travel
```

Ejecute la siguiente celda SQL para fijar el contexto y comprobar que la tabla objetivo existe:

```sql
USE CATALOG de_training;
USE SCHEMA batch1;

SHOW TABLES LIKE 'silver_orders';

DESCRIBE TABLE EXTENDED de_training.batch1.silver_orders;

SHOW TBLPROPERTIES de_training.batch1.silver_orders;
```

Revise el resultado de `DESCRIBE TABLE EXTENDED` y confirme los nombres reales de las columnas. En esta guía se utilizarán como referencia los nombres `order_id`, `amount` y `status`.

Si su tabla utiliza nombres equivalentes en español, como `pedido_id`, `importe` y `estado`, sustituya los identificadores en todas las consultas posteriores, manteniendo la misma lógica.

Cree la tabla de auditoría si todavía no existe:

```sql
CREATE TABLE IF NOT EXISTS de_training.batch1.delta_recovery_audit (
  recovery_id STRING NOT NULL,
  audit_timestamp TIMESTAMP NOT NULL,
  event_type STRING NOT NULL,
  table_name STRING NOT NULL,
  safe_version BIGINT,
  safe_timestamp TIMESTAMP,
  incident_key STRING,
  row_count BIGINT,
  total_amount DECIMAL(20,2),
  notes STRING
)
USING DELTA
COMMENT 'Evidencia de auditoría para ejercicios de Time Travel y RESTORE TABLE';
```

Compruebe su definición:

```sql
DESCRIBE TABLE EXTENDED de_training.batch1.delta_recovery_audit;
```

## Instrucciones Paso a Paso

### Paso 1: Inspeccionar el estado y el historial de la tabla

**Objetivo:** Confirmar que `silver_orders` existe, contiene datos y posee un historial Delta con versiones disponibles para recuperación.

**Instrucciones:**

1. Ejecute una consulta de perfil básico sobre la tabla actual:

   ```sql
   SELECT
     COUNT(*) AS row_count,
     CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)) AS total_amount,
     MIN(order_id) AS min_order_id,
     MAX(order_id) AS max_order_id
   FROM de_training.batch1.silver_orders;
   ```

2. Consulte el historial completo de la tabla:

   ```sql
   DESCRIBE HISTORY de_training.batch1.silver_orders;
   ```

3. Identifique en el resultado las columnas siguientes:
   - `version`
   - `timestamp`
   - `operation`
   - `operationParameters`
   - `operationMetrics`
   - `userName` o identidad de ejecución, si está disponible

4. Confirme que el historial contiene operaciones similares a `MERGE`, `OPTIMIZE`, `WRITE` o `UPDATE`.

5. Consulte las últimas diez versiones para facilitar su revisión:

   ```sql
   DESCRIBE HISTORY de_training.batch1.silver_orders LIMIT 10;
   ```

**Resultado esperado:**

- La tabla devuelve al menos una fila.
- `DESCRIBE HISTORY` devuelve una secuencia de versiones Delta.
- Se observan operaciones realizadas durante prácticas anteriores, incluyendo una operación `MERGE` y una operación `OPTIMIZE` si la Práctica 4 se completó correctamente.

**Verificación:**

Registre en sus notas de laboratorio:

- Conteo actual de filas.
- Importe total actual.
- Última versión visible.
- Una versión asociada a una operación `MERGE`.
- Una versión asociada a una operación `OPTIMIZE`.

Conserve una captura del resultado de `DESCRIBE HISTORY` o guarde la celda ejecutada y su salida en el notebook. La evidencia debe mostrar al menos las columnas `version`, `timestamp` y `operation`.

---

### Paso 2: Seleccionar y registrar una versión segura

**Objetivo:** Definir una versión base segura antes de simular el incidente operativo.

**Instrucciones:**

1. Seleccione como versión segura la versión más reciente visible antes de realizar cualquier modificación en esta práctica.

2. Anote los siguientes valores de la fila seleccionada en `DESCRIBE HISTORY`:

   | Dato | Valor registrado por el estudiante |
   |---|---|
   | Versión segura | `<VERSION_SEGURA>` |
   | Marca temporal segura | `<TIMESTAMP_SEGURO>` |
   | Operación asociada | `<OPERACION_SEGURA>` |

3. Consulte las métricas de la versión segura. Reemplace `<VERSION_SEGURA>` por el valor identificado:

   ```sql
   SELECT
     COUNT(*) AS row_count,
     CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)) AS total_amount
   FROM de_training.batch1.silver_orders
   VERSION AS OF <VERSION_SEGURA>;
   ```

4. Registre la línea base en la tabla de auditoría. Reemplace los marcadores por los valores reales y asigne un identificador único, por ejemplo `tt_restore_batch_003_20260928_1015`.

   ```sql
   INSERT INTO de_training.batch1.delta_recovery_audit (
     recovery_id,
     audit_timestamp,
     event_type,
     table_name,
     safe_version,
     safe_timestamp,
     incident_key,
     row_count,
     total_amount,
     notes
   )
   SELECT
     'tt_restore_batch_003_YYYYMMDD_HHMM',
     current_timestamp(),
     'BASELINE_CAPTURED',
     'de_training.batch1.silver_orders',
     <VERSION_SEGURA>,
     TIMESTAMP '<TIMESTAMP_SEGURO>',
     NULL,
     COUNT(*),
     CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)),
     'Versión segura identificada antes de la simulación controlada.'
   FROM de_training.batch1.silver_orders
   VERSION AS OF <VERSION_SEGURA>;
   ```

5. Consulte la evidencia insertada:

   ```sql
   SELECT *
   FROM de_training.batch1.delta_recovery_audit
   WHERE recovery_id = 'tt_restore_batch_003_YYYYMMDD_HHMM'
   ORDER BY audit_timestamp;
   ```

**Resultado esperado:**

- Se identifica una versión segura y su marca temporal.
- La consulta con `VERSION AS OF` devuelve el estado histórico de esa versión.
- La tabla de auditoría contiene un evento `BASELINE_CAPTURED`.

**Verificación:**

La versión segura es válida si cumple simultáneamente estos criterios:

1. Es anterior a la modificación incorrecta que se realizará en el Paso 4.
2. Devuelve un conteo de filas mayor que cero.
3. Devuelve un importe total no nulo.
4. Está documentada en `delta_recovery_audit`.

> **Nota técnica:** Una operación `OPTIMIZE` puede crear una nueva versión Delta aunque no cambie el contenido lógico de las filas. Por ello, la versión segura debe validarse por sus métricas de negocio y no solo por el tipo de operación del historial.

---

### Paso 3: Consultar datos históricos con VERSION AS OF y TIMESTAMP AS OF

**Objetivo:** Comprobar que Delta Lake permite recuperar el mismo estado histórico mediante número de versión y marca temporal.

**Instrucciones:**

1. Consulte una muestra de cinco registros usando la versión segura:

   ```sql
   SELECT
     order_id,
     amount,
     status
   FROM de_training.batch1.silver_orders
   VERSION AS OF <VERSION_SEGURA>
   ORDER BY order_id
   LIMIT 5;
   ```

2. Consulte la misma muestra usando la marca temporal registrada en el historial. Reemplace `<TIMESTAMP_SEGURO>` por una marca temporal con formato `YYYY-MM-DD HH:MM:SS`:

   ```sql
   SELECT
     order_id,
     amount,
     status
   FROM de_training.batch1.silver_orders
   TIMESTAMP AS OF '<TIMESTAMP_SEGURO>'
   ORDER BY order_id
   LIMIT 5;
   ```

3. Compare las métricas de ambas consultas históricas:

   ```sql
   WITH by_version AS (
     SELECT
       COUNT(*) AS row_count,
       CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)) AS total_amount
     FROM de_training.batch1.silver_orders
     VERSION AS OF <VERSION_SEGURA>
   ),
   by_timestamp AS (
     SELECT
       COUNT(*) AS row_count,
       CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)) AS total_amount
     FROM de_training.batch1.silver_orders
     TIMESTAMP AS OF '<TIMESTAMP_SEGURO>'
   )
   SELECT
     by_version.row_count AS rows_by_version,
     by_timestamp.row_count AS rows_by_timestamp,
     by_version.total_amount AS amount_by_version,
     by_timestamp.total_amount AS amount_by_timestamp
   FROM by_version
   CROSS JOIN by_timestamp;
   ```

4. Consulte la versión actual para distinguirla explícitamente de la versión histórica:

   ```sql
   SELECT
     COUNT(*) AS current_row_count,
     CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)) AS current_total_amount
   FROM de_training.batch1.silver_orders;
   ```

**Resultado esperado:**

- Las consultas con `VERSION AS OF` y `TIMESTAMP AS OF` devuelven el mismo conjunto lógico de datos para la versión segura seleccionada.
- Los conteos e importes de ambas consultas históricas coinciden.
- La versión actual todavía coincide con la segura porque el incidente aún no se ha ejecutado.

**Verificación:**

La consulta comparativa debe mostrar:

| Métrica | Criterio de aceptación |
|---|---|
| `rows_by_version` y `rows_by_timestamp` | Deben ser iguales |
| `amount_by_version` y `amount_by_timestamp` | Deben ser iguales |
| Estado actual antes del incidente | Debe coincidir con la línea base segura |

Si las métricas no coinciden, no continúe. Revise la precisión de `<TIMESTAMP_SEGURO>` y confirme que la marca temporal se copió desde la fila correspondiente a `<VERSION_SEGURA>`.

---

### Paso 4: Simular una actualización incorrecta controlada

**Objetivo:** Generar una versión Delta incorrecta y cuantificar su impacto sin afectar múltiples registros de forma indiscriminada.

**Instrucciones:**

1. Identifique un único pedido candidato para el incidente. El pedido debe tener un importe no nulo:

   ```sql
   SELECT
     order_id,
     amount,
     status
   FROM de_training.batch1.silver_orders
   WHERE amount IS NOT NULL
   ORDER BY order_id
   LIMIT 1;
   ```

2. Registre el valor de `order_id` como `<ORDER_ID_INCIDENTE>` y el importe original como `<IMPORTE_ORIGINAL>`.

3. Ejecute una actualización incorrecta controlada. Esta operación incrementa deliberadamente el importe de un solo pedido en `9999.99`:

   ```sql
   UPDATE de_training.batch1.silver_orders
   SET amount = amount + 9999.99
   WHERE order_id = '<ORDER_ID_INCIDENTE>';
   ```

4. Compruebe la fila afectada:

   ```sql
   SELECT
     order_id,
     amount,
     status
   FROM de_training.batch1.silver_orders
   WHERE order_id = '<ORDER_ID_INCIDENTE>';
   ```

5. Consulte las métricas actuales después del incidente:

   ```sql
   SELECT
     COUNT(*) AS row_count_after_incident,
     CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)) AS total_amount_after_incident
   FROM de_training.batch1.silver_orders;
   ```

6. Compare la versión segura con la versión actual:

   ```sql
   WITH safe_state AS (
     SELECT
       COUNT(*) AS row_count,
       CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)) AS total_amount
     FROM de_training.batch1.silver_orders
     VERSION AS OF <VERSION_SEGURA>
   ),
   current_state AS (
     SELECT
       COUNT(*) AS row_count,
       CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)) AS total_amount
     FROM de_training.batch1.silver_orders
   )
   SELECT
     safe_state.row_count AS safe_row_count,
     current_state.row_count AS current_row_count,
     current_state.row_count - safe_state.row_count AS row_count_difference,
     safe_state.total_amount AS safe_total_amount,
     current_state.total_amount AS current_total_amount,
     current_state.total_amount - safe_state.total_amount AS amount_difference
   FROM safe_state
   CROSS JOIN current_state;
   ```

7. Consulte de nuevo el historial:

   ```sql
   DESCRIBE HISTORY de_training.batch1.silver_orders LIMIT 5;
   ```

**Resultado esperado:**

- Se crea una nueva versión Delta con operación `UPDATE`.
- El conteo de filas no cambia.
- El importe agregado aumenta en `9999.99`.
- La fila identificada por `<ORDER_ID_INCIDENTE>` muestra el importe incorrecto.
- `DESCRIBE HISTORY` muestra una operación `UPDATE` reciente.

**Verificación:**

Documente los siguientes valores:

| Evidencia | Resultado esperado |
|---|---|
| Diferencia de filas | `0` |
| Diferencia de importe | `9999.99` |
| Operación más reciente | `UPDATE` |
| Identificador afectado | Igual a `<ORDER_ID_INCIDENTE>` |

Si el importe agregado aumenta por un valor distinto de `9999.99`, detenga la práctica. No ejecute otra actualización. Revise el filtro `WHERE` y confirme cuántas filas fueron afectadas mediante el historial y la consulta del pedido.

---

### Paso 5: Restaurar la versión segura y registrar la recuperación

**Objetivo:** Recuperar el estado anterior al incidente mediante `RESTORE TABLE` y conservar evidencia auditable de la operación.

**Instrucciones:**

1. Revise por última vez la versión segura seleccionada en el Paso 2.

2. Ejecute la restauración. Sustituya `<VERSION_SEGURA>` por el número de versión real:

   ```sql
   RESTORE TABLE de_training.batch1.silver_orders
   TO VERSION AS OF <VERSION_SEGURA>;
   ```

3. Consulte el historial inmediatamente después de la restauración:

   ```sql
   DESCRIBE HISTORY de_training.batch1.silver_orders LIMIT 5;
   ```

4. Verifique la fila que se había modificado incorrectamente:

   ```sql
   SELECT
     order_id,
     amount,
     status
   FROM de_training.batch1.silver_orders
   WHERE order_id = '<ORDER_ID_INCIDENTE>';
   ```

5. Compare nuevamente la tabla actual con la versión segura:

   ```sql
   WITH safe_state AS (
     SELECT
       COUNT(*) AS row_count,
       CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)) AS total_amount
     FROM de_training.batch1.silver_orders
     VERSION AS OF <VERSION_SEGURA>
   ),
   restored_current_state AS (
     SELECT
       COUNT(*) AS row_count,
       CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)) AS total_amount
     FROM de_training.batch1.silver_orders
   )
   SELECT
     safe_state.row_count AS safe_row_count,
     restored_current_state.row_count AS restored_row_count,
     restored_current_state.row_count - safe_state.row_count AS row_count_difference,
     safe_state.total_amount AS safe_total_amount,
     restored_current_state.total_amount AS restored_total_amount,
     restored_current_state.total_amount - safe_state.total_amount AS amount_difference
   FROM safe_state
   CROSS JOIN restored_current_state;
   ```

6. Registre la restauración en la tabla de auditoría:

   ```sql
   INSERT INTO de_training.batch1.delta_recovery_audit (
     recovery_id,
     audit_timestamp,
     event_type,
     table_name,
     safe_version,
     safe_timestamp,
     incident_key,
     row_count,
     total_amount,
     notes
   )
   SELECT
     'tt_restore_batch_003_YYYYMMDD_HHMM',
     current_timestamp(),
     'RESTORE_COMPLETED',
     'de_training.batch1.silver_orders',
     <VERSION_SEGURA>,
     TIMESTAMP '<TIMESTAMP_SEGURO>',
     '<ORDER_ID_INCIDENTE>',
     COUNT(*),
     CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)),
     'Tabla restaurada mediante RESTORE TABLE a la versión segura documentada.'
   FROM de_training.batch1.silver_orders;
   ```

7. Consulte la evidencia completa de la ejecución:

   ```sql
   SELECT
     recovery_id,
     audit_timestamp,
     event_type,
     table_name,
     safe_version,
     safe_timestamp,
     incident_key,
     row_count,
     total_amount,
     notes
   FROM de_training.batch1.delta_recovery_audit
   WHERE recovery_id = 'tt_restore_batch_003_YYYYMMDD_HHMM'
   ORDER BY audit_timestamp;
   ```

**Resultado esperado:**

- `RESTORE TABLE` crea una nueva versión en el historial; no elimina las versiones anteriores.
- La operación más reciente del historial es `RESTORE`.
- El importe del pedido afectado vuelve a su valor original.
- El conteo y el importe total actuales coinciden con los de la versión segura.
- La tabla `delta_recovery_audit` contiene al menos los eventos `BASELINE_CAPTURED` y `RESTORE_COMPLETED`.

**Verificación:**

La recuperación se considera correcta únicamente si se cumplen todos los criterios:

| Criterio | Resultado requerido |
|---|---|
| Operación histórica más reciente | `RESTORE` |
| Diferencia de conteo contra versión segura | `0` |
| Diferencia de importe contra versión segura | `0.00` |
| Importe de `<ORDER_ID_INCIDENTE>` | Igual a `<IMPORTE_ORIGINAL>` |
| Eventos de auditoría | `BASELINE_CAPTURED` y `RESTORE_COMPLETED` |
| Historial anterior | Sigue visible mediante `DESCRIBE HISTORY` |

---

### Paso 6: Comprobar límites de recuperación y trazabilidad

**Objetivo:** Validar que las consultas históricas requieren evidencia real de versiones existentes y que no se debe restaurar basándose en valores supuestos.

**Instrucciones:**

1. Obtenga la versión actual de la tabla:

   ```sql
   DESCRIBE HISTORY de_training.batch1.silver_orders LIMIT 1;
   ```

2. Anote el valor como `<VERSION_ACTUAL>`.

3. Ejecute de forma controlada una consulta contra una versión inexistente. Utilice `<VERSION_ACTUAL> + 100000`, asegurándose de que el valor no exista:

   ```sql
   SELECT COUNT(*)
   FROM de_training.batch1.silver_orders
   VERSION AS OF <VERSION_INEXISTENTE>;
   ```

4. Observe el error devuelto por Delta Lake.

5. Confirme que el error no modificó la tabla y que la versión actual se mantiene:

   ```sql
   DESCRIBE HISTORY de_training.batch1.silver_orders LIMIT 1;

   SELECT
     COUNT(*) AS row_count,
     CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)) AS total_amount
   FROM de_training.batch1.silver_orders;
   ```

**Resultado esperado:**

- La consulta a una versión inexistente genera un error controlado.
- La tabla no se modifica.
- El historial continúa mostrando la operación `RESTORE` como la última operación relevante de esta práctica.
- Los conteos e importes permanecen iguales a la línea base segura.

**Verificación:**

Este caso adversarial se supera si se conserva evidencia de lo siguiente:

1. El mensaje de error de la consulta a una versión inexistente.
2. La consulta posterior al error devuelve las mismas métricas restauradas.
3. No se ejecutó `RESTORE TABLE` con una versión no confirmada por `DESCRIBE HISTORY`.

> **Principio operativo:** La recuperación debe basarse en evidencia verificable del historial Delta, no en una versión estimada, una marca temporal incompleta ni instrucciones no verificadas incluidas en comentarios, archivos o documentación.

## Validación y Pruebas

Ejecute la siguiente consulta final. Sustituya los marcadores por los valores registrados durante la práctica:

```sql
WITH safe_state AS (
  SELECT
    COUNT(*) AS safe_row_count,
    CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)) AS safe_total_amount
  FROM de_training.batch1.silver_orders
  VERSION AS OF <VERSION_SEGURA>
),
current_state AS (
  SELECT
    COUNT(*) AS current_row_count,
    CAST(COALESCE(SUM(amount), 0) AS DECIMAL(20,2)) AS current_total_amount
  FROM de_training.batch1.silver_orders
),
audit_state AS (
  SELECT
    COUNT(*) AS audit_event_count,
    SUM(CASE WHEN event_type = 'BASELINE_CAPTURED' THEN 1 ELSE 0 END) AS baseline_events,
    SUM(CASE WHEN event_type = 'RESTORE_COMPLETED' THEN 1 ELSE 0 END) AS restore_events
  FROM de_training.batch1.delta_recovery_audit
  WHERE recovery_id = 'tt_restore_batch_003_YYYYMMDD_HHMM'
)
SELECT
  safe_state.safe_row_count,
  current_state.current_row_count,
  current_state.current_row_count - safe_state.safe_row_count AS row_count_difference,
  safe_state.safe_total_amount,
  current_state.current_total_amount,
  current_state.current_total_amount - safe_state.safe_total_amount AS amount_difference,
  audit_state.audit_event_count,
  audit_state.baseline_events,
  audit_state.restore_events
FROM safe_state
CROSS JOIN current_state
CROSS JOIN audit_state;
```

Criterios medibles de aprobación:

| Prueba | Evidencia requerida | Criterio de aprobación |
|---|---|---|
| Historial Delta | Salida de `DESCRIBE HISTORY` | Existe una operación `UPDATE` y una operación posterior `RESTORE` |
| Recuperación de filas | Consulta comparativa | `row_count_difference = 0` |
| Recuperación de importes | Consulta comparativa | `amount_difference = 0.00` |
| Recuperación del pedido afectado | Consulta por `<ORDER_ID_INCIDENTE>` | El importe coincide con `<IMPORTE_ORIGINAL>` |
| Auditoría | Consulta sobre `delta_recovery_audit` | `baseline_events = 1` y `restore_events = 1` |
| Caso adversarial | Error de `VERSION AS OF <VERSION_INEXISTENTE>` | Se produjo error y no se modificaron las métricas de la tabla |

La evidencia mínima que debe entregar el estudiante es:

1. Resultado de `DESCRIBE HISTORY` donde se aprecie `UPDATE` y `RESTORE`.
2. Resultado de la consulta final con diferencias de filas e importes iguales a cero.
3. Resultado de `delta_recovery_audit` con los dos eventos requeridos.
4. Mensaje de error del caso de versión inexistente.
5. Notebook con las instrucciones SQL ejecutadas y los valores reales utilizados.

## Solución de Problemas

### Problema 1: `RESTORE TABLE` falla por permisos insuficientes

**Síntomas:** La instrucción `RESTORE TABLE de_training.batch1.silver_orders TO VERSION AS OF ...` devuelve un error de autorización, privilegios insuficientes o acceso denegado.

**Causa:** El usuario no dispone de `MODIFY` sobre `de_training.batch1.silver_orders`, no tiene permisos de uso del catálogo o esquema, o una política de Unity Catalog restringe la operación.

**Corrección:**

1. Compruebe que está utilizando el catálogo y esquema correctos:

   ```sql
   USE CATALOG de_training;
   USE SCHEMA batch1;
   ```

2. Solicite al instructor la validación de los privilegios `USE CATALOG`, `USE SCHEMA`, `SELECT` y `MODIFY`.
3. No intente copiar, reemplazar ni recrear la tabla como alternativa.
4. Cuando se otorguen los permisos, repita primero `DESCRIBE HISTORY` para confirmar que la versión segura sigue disponible y después ejecute `RESTORE TABLE`.

### Problema 2: `TIMESTAMP AS OF` no devuelve el estado esperado o informa que no puede resolver la versión

**Síntomas:** La consulta con `TIMESTAMP AS OF` devuelve resultados distintos de `VERSION AS OF`, no encuentra una versión o produce un error de formato de fecha y hora.

**Causa:** La marca temporal fue copiada de una fila distinta del historial, se truncó su precisión, se usó un formato incorrecto o la marca temporal no corresponde a la versión segura elegida.

**Corrección:**

1. Ejecute nuevamente:

   ```sql
   DESCRIBE HISTORY de_training.batch1.silver_orders;
   ```

2. Copie la marca temporal exacta de la misma fila que contiene `<VERSION_SEGURA>`.
3. Utilice una cadena SQL con formato `YYYY-MM-DD HH:MM:SS`:

   ```sql
   SELECT *
   FROM de_training.batch1.silver_orders
   TIMESTAMP AS OF 'YYYY-MM-DD HH:MM:SS'
   LIMIT 5;
   ```

4. Si persiste la diferencia, utilice `VERSION AS OF` como referencia de recuperación, ya que el número de versión es la evidencia inequívoca del Delta Log para esta práctica.

## Limpieza

No elimine las tablas ni ejecute `VACUUM` al finalizar esta práctica. El objetivo es conservar el historial y la evidencia de auditoría para revisión posterior.

Realice únicamente las acciones siguientes:

1. Confirme que `silver_orders` permanece restaurada a la versión segura.
2. Mantenga la tabla `de_training.batch1.delta_recovery_audit`.
3. Guarde el notebook en:

   ```text
   /Workspace/Users/<usuario>/batch_1/07_time_travel
   ```

4. Detenga el clúster si no será utilizado por otra práctica y la política del curso lo permite.
5. No ejecute los siguientes comandos sobre `silver_orders` durante esta práctica:

   ```sql
   DROP TABLE de_training.batch1.silver_orders;
   VACUUM de_training.batch1.silver_orders;
   CREATE OR REPLACE TABLE de_training.batch1.silver_orders;
   ```

## Resumen (+ optional resources)

En esta práctica se consultó el Delta Log de `silver_orders`, se seleccionó una versión segura y se recuperaron datos históricos mediante `VERSION AS OF` y `TIMESTAMP AS OF`. Después de simular una actualización incorrecta sobre un único pedido, se restauró la tabla con `RESTORE TABLE` y se verificó que el conteo de filas, el importe agregado y el registro afectado regresaron al estado esperado.

La restauración no elimina el historial: crea una nueva versión Delta cuyo contenido lógico corresponde a la versión recuperada. La evidencia de la operación debe incluir el historial Delta, las métricas comparativas y los eventos registrados en `delta_recovery_audit`.

Recursos oficiales recomendados:

- Delta Lake Time Travel: https://docs.databricks.com/en/delta/history.html
- Restauración de tablas Delta: https://docs.databricks.com/en/delta/history.html#restore-a-delta-table-to-an-earlier-state
- Historial de tablas Delta: https://docs.databricks.com/en/delta/history.html#describe-history
- Privilegios de Unity Catalog: https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html
