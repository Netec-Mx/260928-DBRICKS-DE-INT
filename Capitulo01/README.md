# Configuración de cluster y ejecución de notebook ETL básico

## Metadatos

| Campo | Valor |
|---|---|
| Duration | 40 minutos |
| Complexity | Fácil |
| Bloom level | Aplicar |

## Descripción General

En esta práctica se prepara el entorno base reutilizable para las siguientes actividades del curso. Configurarás un clúster interactivo compatible con Unity Catalog, organizarás notebooks mediante Databricks Git folders y ejecutarás un pipeline ETL básico con las capas Bronze, Silver y Gold.

El pipeline leerá un archivo CSV controlado desde un volumen administrado de Unity Catalog, almacenará los datos originales en una tabla Delta Bronze, aplicará conversiones de tipo en Silver y generará ventas diarias agregadas en Gold. Al finalizar, validarás que las tablas Delta contienen resultados consistentes y que el proceso puede repetirse sin alterar el resultado funcional esperado.

## Objetivos de Aprendizaje

- [ ] Crear o validar un clúster All-Purpose con Databricks Runtime 15.4 LTS, modo de acceso Standard y compatibilidad con Unity Catalog.
- [ ] Crear una estructura de trabajo en Databricks Git folders y asociarla a un repositorio Git remoto.
- [ ] Crear el esquema `de_training.batch1`, el volumen administrado `landing` y el archivo controlado `orders_batch_001.csv`.
- [ ] Implementar un flujo ETL básico Bronze, Silver y Gold mediante PySpark y tablas Delta administradas.
- [ ] Verificar la integridad funcional de las tablas generadas y diferenciar el uso de un clúster All-Purpose frente a un Job Cluster.

## Prerrequisitos

**Conocimientos requeridos**

- Fundamentos de SQL: `CREATE SCHEMA`, `SELECT`, agregaciones y filtros.
- Fundamentos de Python y PySpark: `DataFrame`, lectura de CSV, transformaciones y escritura de tablas.
- Conceptos de arquitectura Lakehouse, Delta Lake y capas Bronze, Silver y Gold.
- Comprensión básica de control de versiones con Git y ramas.
- Diferencia conceptual entre almacenamiento, cómputo, tablas lógicas y archivos físicos.

**Accesos requeridos**

- Acceso a un workspace de Databricks habilitado con Unity Catalog.
- Privilegios `USE CATALOG`, `CREATE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME` y `USE SCHEMA` en el catálogo `de_training`.
- Permiso para crear o usar un clúster All-Purpose compatible con la política del curso.
- Acceso a un repositorio Git remoto privado, compartido o de solo lectura autorizado por el instructor.
- Acceso de escritura al directorio de workspace del estudiante:

```text
/Workspace/Users/<usuario>/batch_1
```

> Si el catálogo `de_training` no existe en un entorno compartido, usa el catálogo equivalente provisionado por el instructor. Sustituye todas las referencias calificadas del laboratorio de forma consistente.

## Entorno de Laboratorio

### Versiones y fuentes oficiales

| Tecnología | Versión, edición o arquitectura | Uso en la práctica | Fuente oficial |
|---|---|---|---|
| Databricks Runtime | 15.4 LTS, arquitectura Apache Spark 3.5.0 | Ejecución de notebooks y pipeline ETL | https://docs.databricks.com/en/release-notes/runtime/15.4lts.html |
| Apache Spark | 3.5.0 | Procesamiento distribuido con PySpark | https://spark.apache.org/releases/spark-release-3-5-0.html |
| Python | 3.11.0, incluido en Databricks Runtime 15.4 LTS | Código de notebooks | https://docs.databricks.com/en/release-notes/runtime/15.4lts.html |
| Delta Lake | 3.2.0 | Tablas transaccionales Bronze, Silver y Gold | https://github.com/delta-io/delta/releases/tag/v3.2.0 |
| Unity Catalog | Servicio SaaS compatible con Databricks Runtime 15.4 LTS; sin versión independiente publicada | Gobernanza de catálogos, esquemas, volúmenes y tablas | https://docs.databricks.com/en/data-governance/unity-catalog/index.html |
| Databricks Git folders | Servicio de workspace compatible con Databricks Runtime 15.4 LTS; sin versión independiente publicada | Integración con repositorio Git remoto | https://docs.databricks.com/en/repos/index.html |
| Git | 2.46.0, cliente distribuido de control de versiones | Repositorio remoto y commits de notebooks | https://github.com/git/git/blob/v2.46.0/Documentation/RelNotes/2.46.0.adoc |

### Configuración mínima del clúster

| Propiedad | Valor requerido |
|---|---|
| Tipo de clúster | All-Purpose |
| Databricks Runtime | 15.4 LTS |
| Modo de acceso | Standard |
| Unity Catalog | Habilitado mediante modo Standard y política compatible |
| Driver | 1 nodo |
| Workers | 2 nodos como línea base |
| Capacidad recomendada por nodo | Mínimo 4 vCPU y 14 GB de memoria; se recomienda 16 GB |
| Photon | No requerido para esta práctica |
| Autoescalado | Opcional; para reproducibilidad se recomienda usar 2 workers fijos |
| Terminación automática | 20 minutos o el valor definido por la política institucional |

### Convenciones globales del laboratorio

| Elemento | Valor |
|---|---|
| Catálogo | `de_training` |
| Esquema | `batch1` |
| Volumen administrado | `de_training.batch1.landing` |
| Ruta del volumen | `/Volumes/de_training/batch1/landing/` |
| Carpeta de notebooks | `/Workspace/Users/<usuario>/batch_1` |
| Lote de esta práctica | `batch_001` |
| Archivo de origen | `orders_batch_001.csv` |
| Tabla Bronze | `de_training.batch1.bronze_orders` |
| Tabla Silver | `de_training.batch1.silver_orders` |
| Tabla Gold | `de_training.batch1.gold_daily_sales` |

### Comparación operativa de cómputo

| Característica | Clúster All-Purpose | Job Cluster |
|---|---|---|
| Uso principal | Desarrollo interactivo, exploración y depuración | Ejecución automatizada de una tarea o workflow |
| Ciclo de vida | Permanece disponible hasta detenerse o alcanzar la terminación automática | Se crea para la ejecución y se termina al finalizar el job |
| Adecuado para esta práctica | Sí; permite crear, ejecutar y corregir varios notebooks | No es necesario en esta práctica inicial |
| Consideración de costo | Puede generar costo durante periodos inactivos si no se detiene | Reduce tiempo inactivo si está correctamente definido en un workflow |
| Uso posterior | Desarrollo y pruebas del pipeline | Orquestación batch programada y repetible |

## Instrucciones Paso a Paso

### Paso 1: Crear o validar el clúster All-Purpose

**Objetivo:** Disponer de un clúster interactivo con configuración compatible con Unity Catalog y suficiente capacidad para ejecutar los notebooks del pipeline.

**Instrucciones**

1. En el workspace de Databricks, abre **Compute**.
2. Selecciona un clúster existente del curso o crea uno nuevo mediante **Create compute**.
3. Configura los valores siguientes, respetando las restricciones de la política institucional:
   - Nombre sugerido: `batch1-etl-<usuario>`.
   - Tipo: All-Purpose.
   - Runtime: `15.4 LTS`.
   - Access mode: `Standard`.
   - Workers: `2`.
   - Driver: `1`.
   - Terminación automática: `20 minutes`, si la política lo permite.
4. Inicia el clúster y espera a que su estado sea **Running**.
5. Crea un notebook temporal de validación, adjúntalo al clúster y ejecuta la siguiente celda Python:

```python
import sys

print(f"Python: {sys.version}")
print(f"Spark: {spark.version}")
print(f"Cluster: {spark.conf.get('spark.databricks.clusterUsageTags.clusterName')}")
```

6. Registra en una celda Markdown del notebook la fecha, nombre del clúster y runtime seleccionado.

**Resultado esperado**

El clúster se encuentra en estado **Running**, el notebook puede ejecutarse y la salida indica Spark `3.5.0` y Python `3.11.0`.

**Verificación**

- El modo de acceso mostrado en la página del clúster es `Standard`.
- La salida de `spark.version` es `3.5.0`.
- El clúster tiene un driver y dos workers, o la configuración equivalente aprobada por la política del curso.
- No uses modo de acceso `No isolation shared` ni una configuración no compatible con Unity Catalog.

---

### Paso 2: Crear la estructura de notebooks y conectar Git folders

**Objetivo:** Organizar los notebooks del pipeline en una estructura reutilizable y asociarla a un repositorio Git remoto.

**Instrucciones**

1. En la barra lateral del workspace, abre **Workspace** y después **Git folders**.
2. Selecciona **Add Git folder**.
3. Indica la URL HTTPS del repositorio Git asignado por el instructor o creado por tu equipo.
4. Autentícate con el método autorizado por tu proveedor Git:
   - OAuth, si está habilitado por la organización.
   - Token de acceso personal, solo en el cuadro de autenticación del proveedor.
5. No copies tokens, contraseñas ni credenciales dentro de notebooks, archivos CSV, celdas Markdown o commits.
6. Selecciona la rama de trabajo autorizada, normalmente `main` o una rama individual del estudiante.
7. Crea o verifica la siguiente estructura dentro del Git folder:

```text
batch_1/
├── 01_setup
├── 02_bronze
├── 03_silver
├── 04_gold
├── 05_validations
├── 06_merge
└── 07_time_travel
```

8. Para esta práctica, crea los notebooks:
   - `01_setup`
   - `02_bronze`
   - `03_silver`
   - `04_gold`
   - `05_validations`
9. Configura el lenguaje predeterminado de los notebooks como Python.
10. Realiza un commit inicial con un mensaje trazable, por ejemplo:

```text
lab-01: create batch_1 notebook structure
```

**Resultado esperado**

Existe un Git folder asociado a un repositorio remoto y contiene la estructura de notebooks requerida.

**Verificación**

- El Git folder muestra el repositorio y la rama activa.
- Los cinco notebooks de esta práctica son visibles.
- El estado de Git no muestra cambios pendientes después del commit inicial.
- La estructura se encuentra dentro de la carpeta Git; no solamente en una carpeta local aislada del workspace.

---

### Paso 3: Crear el esquema, el volumen y el archivo de entrada controlado

**Objetivo:** Preparar el almacenamiento gobernado por Unity Catalog y generar un archivo CSV reproducible para el lote `batch_001`.

**Instrucciones**

1. Abre el notebook `01_setup`.
2. Adjunta el notebook al clúster creado en el Paso 1.
3. Agrega una primera celda SQL y ejecuta el siguiente código:

```sql
CREATE SCHEMA IF NOT EXISTS de_training.batch1;

CREATE VOLUME IF NOT EXISTS de_training.batch1.landing;
```

4. Agrega una celda Python con las constantes del laboratorio:

```python
CATALOG = "de_training"
SCHEMA = "batch1"
LANDING_PATH = "/Volumes/de_training/batch1/landing"
BATCH_ID = "batch_001"
SOURCE_FILE = f"{LANDING_PATH}/orders_{BATCH_ID}.csv"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_daily_sales"
```

5. Agrega una nueva celda Python para crear el archivo CSV controlado:

```python
csv_content = """order_id,order_date,customer_id,region,amount
1001,2025-01-15,C001,Norte,120.50
1002,2025-01-15,C002,Sur,80.00
1003,2025-01-16,C003,Norte,50.25
1004,2025-01-16,C004,Centro,200.00
1005,2025-01-16,C005,Sur,19.75
"""

dbutils.fs.put(SOURCE_FILE, csv_content, True)

print(f"Archivo creado: {SOURCE_FILE}")
```

6. Agrega una celda final para inspeccionar el contenido del volumen:

```python
display(dbutils.fs.ls(LANDING_PATH))
```

7. Guarda el notebook y realiza un commit con el mensaje:

```text
lab-01: create governed landing file for batch_001
```

**Resultado esperado**

El esquema `de_training.batch1` y el volumen administrado `landing` existen. El archivo `orders_batch_001.csv` está disponible en la ruta del volumen y contiene cinco pedidos.

**Verificación**

Ejecuta la siguiente consulta SQL:

```sql
SHOW VOLUMES IN de_training.batch1;
```

Y verifica que aparece un volumen llamado `landing`.

Ejecuta también:

```python
display(
    spark.read
    .option("header", True)
    .csv(SOURCE_FILE)
)
```

La visualización debe mostrar exactamente cinco filas y las columnas:

```text
order_id, order_date, customer_id, region, amount
```

---

### Paso 4: Crear la tabla Bronze con datos de origen

**Objetivo:** Leer el archivo CSV conservando los valores de origen y escribir una tabla Delta Bronze administrada por Unity Catalog.

**Instrucciones**

1. Abre el notebook `02_bronze`.
2. Incluye una celda Markdown que describa el propósito de la capa Bronze:

```markdown
La capa Bronze conserva los datos ingeridos con transformaciones mínimas.
En este laboratorio, los valores del CSV se mantienen inicialmente como texto
y se agregan metadatos técnicos de trazabilidad.
```

3. Agrega una celda Python con las constantes necesarias:

```python
from pyspark.sql.functions import current_timestamp, input_file_name, lit

CATALOG = "de_training"
SCHEMA = "batch1"
LANDING_PATH = "/Volumes/de_training/batch1/landing"
BATCH_ID = "batch_001"
SOURCE_FILE = f"{LANDING_PATH}/orders_{BATCH_ID}.csv"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"
```

4. Lee el CSV sin inferencia automática de tipos para conservar el carácter crudo de los datos:

```python
raw_orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(SOURCE_FILE)
)
```

5. Agrega columnas de trazabilidad técnica:

```python
bronze_orders_df = (
    raw_orders_df
    .withColumn("batch_id", lit(BATCH_ID))
    .withColumn("ingested_at", current_timestamp())
    .withColumn("source_file", input_file_name())
)
```

6. Escribe los datos como tabla Delta administrada:

```python
(
    bronze_orders_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)
```

7. Consulta la tabla resultante:

```python
display(spark.table(BRONZE_TABLE))
```

8. Guarda el notebook y realiza el commit:

```text
lab-01: create bronze_orders delta table
```

**Resultado esperado**

La tabla `de_training.batch1.bronze_orders` existe, tiene cinco registros y conserva las columnas de origen como texto junto con las columnas `batch_id`, `ingested_at` y `source_file`.

**Verificación**

Ejecuta estas consultas SQL:

```sql
SELECT COUNT(*) AS bronze_record_count
FROM de_training.batch1.bronze_orders;
```

```sql
DESCRIBE DETAIL de_training.batch1.bronze_orders;
```

Criterios de aceptación:

- `bronze_record_count` es igual a `5`.
- El formato informado por `DESCRIBE DETAIL` es `delta`.
- La tabla pertenece al catálogo `de_training` y al esquema `batch1`.
- La columna `batch_id` contiene el valor `batch_001`.

---

### Paso 5: Crear la tabla Silver con conversiones de tipo

**Objetivo:** Aplicar limpieza básica y tipado explícito a los datos Bronze para producir una tabla Silver apta para consumo analítico posterior.

**Instrucciones**

1. Abre el notebook `03_silver`.
2. Incluye una celda Markdown que describa el propósito de la capa Silver:

```markdown
La capa Silver contiene datos limpios, tipados y validados.
En esta práctica se convierten las fechas e importes, se eliminan espacios
y se conservan únicamente registros con campos obligatorios válidos.
```

3. Agrega una celda Python con las importaciones y las constantes:

```python
from pyspark.sql.functions import col, to_date, trim
from pyspark.sql.types import DecimalType

CATALOG = "de_training"
SCHEMA = "batch1"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"
```

4. Lee la tabla Bronze y aplica conversiones explícitas:

```python
bronze_df = spark.table(BRONZE_TABLE)

silver_orders_df = (
    bronze_df
    .select(
        trim(col("order_id")).alias("order_id"),
        to_date(trim(col("order_date")), "yyyy-MM-dd").alias("order_date"),
        trim(col("customer_id")).alias("customer_id"),
        trim(col("region")).alias("region"),
        trim(col("amount")).cast(DecimalType(12, 2)).alias("order_amount"),
        col("batch_id"),
        col("ingested_at"),
        col("source_file")
    )
    .filter(col("order_id").isNotNull())
    .filter(col("order_date").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("order_amount").isNotNull())
    .filter(col("order_amount") >= 0)
)
```

5. Escribe la tabla Silver como tabla Delta administrada:

```python
(
    silver_orders_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)
```

6. Inspecciona el esquema y los datos:

```python
spark.table(SILVER_TABLE).printSchema()
display(spark.table(SILVER_TABLE))
```

7. Guarda el notebook y realiza el commit:

```text
lab-01: create typed silver_orders delta table
```

**Resultado esperado**

La tabla Silver contiene cinco registros válidos. La columna `order_date` tiene tipo `date` y la columna `order_amount` tiene tipo `decimal(12,2)`.

**Verificación**

Ejecuta:

```sql
DESCRIBE TABLE de_training.batch1.silver_orders;
```

Confirma que se observan, como mínimo, los siguientes tipos:

| Columna | Tipo esperado |
|---|---|
| `order_id` | `string` |
| `order_date` | `date` |
| `customer_id` | `string` |
| `region` | `string` |
| `order_amount` | `decimal(12,2)` |
| `batch_id` | `string` |

Después ejecuta:

```sql
SELECT
  COUNT(*) AS silver_record_count,
  MIN(order_amount) AS minimum_amount,
  MAX(order_amount) AS maximum_amount
FROM de_training.batch1.silver_orders;
```

Los valores esperados son:

| Métrica | Valor esperado |
|---|---:|
| `silver_record_count` | 5 |
| `minimum_amount` | 19.75 |
| `maximum_amount` | 200.00 |

---

### Paso 6: Crear la tabla Gold de ventas diarias

**Objetivo:** Crear una tabla Gold agregada por fecha de pedido para representar un producto de datos simple para análisis de negocio.

**Instrucciones**

1. Abre el notebook `04_gold`.
2. Incluye una celda Markdown con el propósito de la capa Gold:

```markdown
La capa Gold presenta datos agregados o modelados para un caso de negocio.
En este laboratorio se genera una métrica de ventas diarias a partir de
los pedidos validados de la capa Silver.
```

3. Agrega las importaciones y constantes:

```python
from pyspark.sql.functions import countDistinct, sum as spark_sum

CATALOG = "de_training"
SCHEMA = "batch1"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_daily_sales"
```

4. Lee Silver y calcula las ventas diarias:

```python
silver_df = spark.table(SILVER_TABLE)

gold_daily_sales_df = (
    silver_df
    .groupBy("order_date")
    .agg(
        countDistinct("order_id").alias("order_count"),
        spark_sum("order_amount").alias("total_sales")
    )
    .orderBy("order_date")
)
```

5. Escribe el resultado como tabla Delta Gold:

```python
(
    gold_daily_sales_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)
```

6. Muestra los resultados:

```python
display(spark.table(GOLD_TABLE).orderBy("order_date"))
```

7. Guarda el notebook y realiza el commit:

```text
lab-01: create gold daily sales aggregate
```

**Resultado esperado**

La tabla `de_training.batch1.gold_daily_sales` contiene dos filas, una por cada fecha de pedido.

**Verificación**

Ejecuta:

```sql
SELECT
  order_date,
  order_count,
  total_sales
FROM de_training.batch1.gold_daily_sales
ORDER BY order_date;
```

El resultado esperado es:

| order_date | order_count | total_sales |
|---|---:|---:|
| 2025-01-15 | 2 | 200.50 |
| 2025-01-16 | 3 | 270.00 |

El total general debe ser `470.50`.

---

### Paso 7: Validar el flujo Lakehouse y registrar la evidencia técnica

**Objetivo:** Comprobar que las capas Bronze, Silver y Gold son consistentes, que las tablas son Delta y que la carga controlada no incorpora archivos no autorizados.

**Instrucciones**

1. Abre el notebook `05_validations`.
2. Ejecuta la siguiente consulta para contar registros por capa:

```sql
SELECT 'bronze_orders' AS layer, COUNT(*) AS record_count
FROM de_training.batch1.bronze_orders

UNION ALL

SELECT 'silver_orders' AS layer, COUNT(*) AS record_count
FROM de_training.batch1.silver_orders

UNION ALL

SELECT 'gold_daily_sales' AS layer, COUNT(*) AS record_count
FROM de_training.batch1.gold_daily_sales;
```

3. Verifica que la agregación Gold coincide con los datos Silver:

```sql
WITH expected_gold AS (
  SELECT
    order_date,
    COUNT(DISTINCT order_id) AS expected_order_count,
    SUM(order_amount) AS expected_total_sales
  FROM de_training.batch1.silver_orders
  GROUP BY order_date
)
SELECT
  e.order_date,
  e.expected_order_count,
  g.order_count AS actual_order_count,
  e.expected_total_sales,
  g.total_sales AS actual_total_sales
FROM expected_gold e
FULL OUTER JOIN de_training.batch1.gold_daily_sales g
  ON e.order_date = g.order_date
ORDER BY e.order_date;
```

4. Inspecciona el historial transaccional de la tabla Bronze:

```sql
DESCRIBE HISTORY de_training.batch1.bronze_orders;
```

5. Ejecuta el siguiente caso adversarial controlado en una celda Python. El objetivo es demostrar que el pipeline utiliza una ruta de entrada explícita y no procesa automáticamente archivos extra ni texto que simule instrucciones:

```python
ADVERSARIAL_FILE = "/Volumes/de_training/batch1/landing/orders_batch_001_adversarial.csv"

adversarial_content = """order_id,order_date,customer_id,region,amount
IGNORE_PREVIOUS_INSTRUCTIONS,2099-99-99,UNKNOWN,N/A,0
"""

dbutils.fs.put(ADVERSARIAL_FILE, adversarial_content, True)

print("Archivo adversarial creado únicamente para la prueba.")
print(f"Archivo esperado por Bronze: {SOURCE_FILE}")
print(f"Archivo no autorizado: {ADVERSARIAL_FILE}")
```

6. Vuelve a ejecutar únicamente el código del Paso 4 que lee `SOURCE_FILE` y sobrescribe `bronze_orders`.
7. Comprueba que Bronze sigue teniendo cinco filas y que ninguna fila proviene del archivo adversarial:

```sql
SELECT
  COUNT(*) AS records_from_adversarial_file
FROM de_training.batch1.bronze_orders
WHERE source_file LIKE '%orders_batch_001_adversarial.csv%';
```

8. Elimina el archivo adversarial tras la prueba:

```python
dbutils.fs.rm(ADVERSARIAL_FILE)
```

9. Realiza un commit final:

```text
lab-01: validate bronze silver gold pipeline
```

**Resultado esperado**

Las capas muestran recuentos coherentes, la tabla Gold coincide con las agregaciones de Silver y las tablas conservan historial Delta. El archivo adversarial no altera el contenido de Bronze.

**Verificación**

- `bronze_orders` contiene `5` registros.
- `silver_orders` contiene `5` registros.
- `gold_daily_sales` contiene `2` registros.
- La consulta de reconciliación entre Silver y Gold no muestra diferencias entre valores esperados y reales.
- `DESCRIBE HISTORY` devuelve al menos una versión de la tabla Bronze.
- `records_from_adversarial_file` es igual a `0`.
- El archivo `orders_batch_001_adversarial.csv` ya no existe tras la limpieza.

> Una instrucción temporal es una indicación dada al estudiante durante una sesión. Un prompt es el texto enviado a un sistema de IA para solicitar una respuesta. Un mensaje de sistema define reglas persistentes de comportamiento para un asistente o agente. En este pipeline no se utiliza un asistente de IA: todo contenido dentro de CSV se trata exclusivamente como dato y nunca como una instrucción ejecutable.

## Validación y Pruebas

La validación debe producir evidencia ejecutable en los notebooks, no solo capturas de pantalla. Conserva los resultados de las consultas SQL y los commits Git como evidencia técnica.

| Prueba | Comando o evidencia | Criterio medible de aprobación |
|---|---|---|
| Compatibilidad de cómputo | Salida de `spark.version` | El valor es `3.5.0` y el clúster usa Databricks Runtime 15.4 LTS con acceso Standard |
| Existencia del volumen | `SHOW VOLUMES IN de_training.batch1` | Existe el volumen `landing` |
| Archivo de entrada | Lectura de `orders_batch_001.csv` | Se obtienen exactamente 5 filas |
| Tabla Bronze | `SELECT COUNT(*) FROM bronze_orders` | Resultado igual a 5 |
| Tipo de tabla | `DESCRIBE DETAIL` sobre las tres tablas | El formato de Bronze, Silver y Gold es `delta` |
| Tipado Silver | `DESCRIBE TABLE silver_orders` | `order_date` es `date` y `order_amount` es `decimal(12,2)` |
| Agregación Gold | Consulta ordenada sobre `gold_daily_sales` | Dos filas con totales 200.50 y 270.00 |
| Integridad funcional | Reconciliación entre Silver y Gold | No existen diferencias entre métricas esperadas y reales |
| Trazabilidad | `DESCRIBE HISTORY bronze_orders` | Existe historial de operaciones Delta |
| Caso adversarial | Archivo CSV adicional con texto de pseudo-instrucción | El recuento Bronze permanece en 5 y hay 0 filas provenientes del archivo adversarial |
| Control de versiones | Historial de Git folder | Existen commits trazables para estructura, setup, Bronze, Silver, Gold y validación |

Criterio global de finalización: todas las pruebas deben aprobarse. Un resultado parcial, como tener una tabla creada pero sin validación de tipos, reconciliación o trazabilidad, no completa la práctica.

## Solución de Problemas

### Incidencia 1: Error de permisos al crear el esquema o el volumen

**Síntomas**

Al ejecutar `CREATE SCHEMA` o `CREATE VOLUME`, aparece un error similar a:

```text
PERMISSION_DENIED: User does not have CREATE SCHEMA privilege
```

o:

```text
PERMISSION_DENIED: User does not have CREATE VOLUME privilege
```

**Causa probable**

El usuario no tiene privilegios suficientes sobre el catálogo `de_training`, el catálogo asignado no corresponde al indicado en el laboratorio o el clúster no está configurado con un modo de acceso compatible con Unity Catalog.

**Corrección**

1. Ejecuta:

```sql
SHOW GRANTS ON CATALOG de_training;
```

2. Confirma con el instructor que tu usuario o grupo posee `USE CATALOG`, `CREATE SCHEMA` y los privilegios necesarios en el esquema.
3. Verifica que el clúster usa modo de acceso `Standard`.
4. Si el instructor asignó un catálogo individual, reemplaza `de_training` en todos los notebooks por el catálogo autorizado.
5. No intentes escribir directamente en rutas de almacenamiento no gobernadas como sustituto del volumen administrado.

### Incidencia 2: La tabla Gold tiene cero filas o totales inesperados

**Síntomas**

La tabla `gold_daily_sales` no contiene las dos fechas esperadas, muestra valores nulos o sus totales no coinciden con `200.50` y `270.00`.

**Causa probable**

La tabla Silver no se ejecutó después de Bronze, la conversión de fecha no coincide con el formato del archivo, la columna `amount` no se convirtió correctamente a decimal o se ejecutó Gold antes de actualizar Silver.

**Corrección**

1. Revisa el contenido de Bronze:

```sql
SELECT order_id, order_date, amount
FROM de_training.batch1.bronze_orders;
```

2. Revisa los tipos y valores de Silver:

```sql
SELECT order_id, order_date, order_amount
FROM de_training.batch1.silver_orders;
```

3. Asegúrate de usar el formato correcto en Silver:

```python
to_date(trim(col("order_date")), "yyyy-MM-dd")
```

4. Ejecuta nuevamente los notebooks en este orden:

```text
01_setup → 02_bronze → 03_silver → 04_gold → 05_validations
```

5. Vuelve a ejecutar la consulta de reconciliación entre Silver y Gold.

## Limpieza

Esta práctica prepara recursos que se reutilizarán en las prácticas siguientes. Por tanto, **no elimines las tablas, el volumen ni los notebooks** si continuarás con el curso.

Si el instructor solicita reiniciar por completo el entorno de esta práctica, ejecuta las acciones siguientes únicamente después de confirmar que no hay actividades posteriores dependientes de estos objetos.

1. Elimina las tablas Delta:

```sql
DROP TABLE IF EXISTS de_training.batch1.gold_daily_sales;
DROP TABLE IF EXISTS de_training.batch1.silver_orders;
DROP TABLE IF EXISTS de_training.batch1.bronze_orders;
```

2. Elimina los archivos del volumen y después elimina el volumen:

```python
dbutils.fs.rm("/Volumes/de_training/batch1/landing", True)
```

```sql
DROP VOLUME IF EXISTS de_training.batch1.landing;
```

3. Elimina el esquema solo si está vacío y el instructor lo autoriza:

```sql
DROP SCHEMA IF EXISTS de_training.batch1;
```

4. Detén el clúster All-Purpose al finalizar la sesión si no será usado de inmediato. Esto evita consumo innecesario de cómputo.
5. Conserva el Git folder y los commits salvo que el instructor solicite eliminar el repositorio de práctica.

## Resumen

En esta práctica configuraste un entorno de trabajo inicial para un pipeline Lakehouse gobernado por Unity Catalog. Creaste un clúster All-Purpose compatible, organizaste notebooks en un Git folder, generaste un archivo de entrada controlado y construiste tablas Delta administradas en las capas Bronze, Silver y Gold.

También verificaste propiedades fundamentales de un pipeline de ingeniería de datos: tipado explícito, trazabilidad mediante metadatos de ingesta, agregación reproducible, historial transaccional Delta, control de archivos de entrada y reconciliación funcional entre capas.

### Recursos

- Delta Lake en Databricks: https://docs.databricks.com/en/delta/index.html
- Unity Catalog: https://docs.databricks.com/en/data-governance/unity-catalog/index.html
- Git folders de Databricks: https://docs.databricks.com/en/repos/index.html
- Arquitectura Medallion: https://docs.databricks.com/en/lakehouse/medallion.html
- Tablas administradas de Unity Catalog: https://docs.databricks.com/en/tables/managed.html

---

# Implementación inicial de arquitectura Bronze/Silver

## Metadatos

| Campo | Valor |
|---|---|
| Duración | 40 minutos |
| Complejidad | Fácil |
| Nivel de Bloom | Aplicar |

## Descripción General

En esta práctica se transforma el flujo de carga básico creado en la Práctica 1 en una arquitectura Lakehouse con capas Bronze, Silver y Gold. Se conserva el archivo de origen en una tabla Bronze con metadatos técnicos de ingesta, se estandarizan y deduplican los pedidos en Silver, y se recalcula una tabla Gold de ventas diarias.

La práctica utiliza tablas Delta administradas por Unity Catalog para aportar transacciones ACID, control de esquema, historial de versiones y una ubicación gobernada de los datos. El resultado será la base para las cargas incrementales, validaciones de calidad y operaciones `MERGE` de las prácticas posteriores.

## Objetivos de Aprendizaje

- [ ] Crear una tabla Bronze administrada que preserve los campos de origen e incluya `source_file`, `ingest_ts` y `batch_id`.
- [ ] Transformar datos Bronze en una tabla Silver con tipos estandarizados, eliminación de identificadores nulos y deduplicación por `order_id`.
- [ ] Calcular el importe total de cada pedido mediante `quantity * unit_price`.
- [ ] Construir o reemplazar la tabla Gold `gold_daily_sales` a partir de `silver_orders`.
- [ ] Verificar trazabilidad, calidad básica e historial de tablas Delta mediante consultas reproducibles.

## Prerrequisitos

### Conocimientos requeridos

- Comprensión básica de las capas Bronze, Silver y Gold de una arquitectura Medallion.
- Uso elemental de notebooks de Databricks, SQL y PySpark DataFrames.
- Conocimiento de tipos de datos, valores nulos, agregaciones y claves de negocio.
- Comprensión de que una tabla Delta combina archivos de datos con un registro transaccional que permite lecturas consistentes e historial de cambios.

### Acceso y estado inicial requeridos

Antes de comenzar, confirme que se cumplen estas condiciones:

1. La Práctica 1 fue completada.
2. Existe el catálogo `de_training` y el esquema `de_training.batch1`.
3. Existe el volumen administrado `de_training.batch1.landing`.
4. El archivo de entrada está disponible en:

   ```text
   /Volumes/de_training/batch1/landing/orders_batch_001.csv
   ```

5. Tiene permisos para:
   - Usar el clúster asignado.
   - Crear, reemplazar y consultar tablas dentro de `de_training.batch1`.
   - Leer archivos del volumen `de_training.batch1.landing`.
   - Ejecutar notebooks en `/Workspace/Users/<usuario>/batch_1`.

6. El clúster está iniciado y utiliza Databricks Runtime 15.4 LTS con modo de acceso Standard compatible con Unity Catalog.

## Entorno de Laboratorio

### Tecnologías y versiones

| Tecnología | Versión o configuración | Uso en la práctica | Fuente oficial |
|---|---:|---|---|
| Databricks Runtime | 15.4 LTS, arquitectura x86_64 administrada por Databricks | Ejecución de notebooks y Spark | https://docs.databricks.com/en/release-notes/runtime/15.4lts.html |
| Apache Spark | 3.5.0 | Lectura CSV, transformaciones DataFrame y ventanas | https://spark.apache.org/releases/spark-release-3-5-0.html |
| Python | 3.11.0 | Lenguaje de las celdas PySpark | https://www.python.org/downloads/release/python-3110/ |
| Delta Lake | 3.2.0 | Tablas Delta, historial transaccional y persistencia | https://docs.delta.io/3.2.0/index.html |
| Unity Catalog | Servicio SaaS, versión independiente no publicada **[VERSIÓN POR VALIDAR]** | Catálogo, esquema, volumen y tablas administradas | https://docs.databricks.com/en/data-governance/unity-catalog/index.html |
| PySpark | 3.5.0 | API DataFrame y funciones SQL de Spark | https://spark.apache.org/docs/3.5.0/api/python/ |
| SQL de Databricks | Incluido en Databricks Runtime 15.4 LTS | Consultas de validación y administración de tablas | https://docs.databricks.com/en/sql/index.html |

### Recursos mínimos

| Recurso | Configuración |
|---|---|
| Cómputo | 1 nodo driver y al menos 1 worker |
| Configuración recomendada | 4 vCPU y 16 GB de memoria por nodo |
| Modo de acceso | Standard, compatible con Unity Catalog |
| Catálogo | `de_training` |
| Esquema | `batch1` |
| Volumen | `de_training.batch1.landing` |
| Archivo de entrada | `orders_batch_001.csv` |
| Identificador de lote | `batch_001` |

### Herramientas no utilizadas

Esta práctica no utiliza Microsoft 365 Copilot, Copilot Chat, Microsoft Designer, Microsoft Planner ni agentes de IA. Por tanto, no requiere licencias, configuraciones ni permisos de esas herramientas. Las acciones se ejecutan explícitamente desde un notebook Databricks mediante código PySpark y SQL.

Un archivo CSV es un dato de entrada, no una instrucción, un prompt ni un mensaje de sistema. Si algún valor textual del archivo contiene frases que parezcan instrucciones, debe tratarse como contenido de datos y no debe ejecutarse como código ni como acción operativa.

### Preparación del notebook

1. En el espacio de trabajo, cree o abra el notebook:

   ```text
   /Workspace/Users/<usuario>/batch_1/02_bronze/02_bronze_silver_orders
   ```

2. Adjunte el notebook al clúster asignado.
3. Cree una primera celda Python y ejecute la siguiente configuración:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "de_training"
SCHEMA = "batch1"

LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
SOURCE_FILE = f"{LANDING_PATH}/orders_batch_001.csv"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_daily_sales"

BATCH_ID = "batch_001"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
```

## Instrucciones Paso a Paso

### Paso 1: Verificar el archivo de origen y el contexto de Unity Catalog

**Objetivo:** Confirmar que el notebook usa el catálogo, esquema, volumen y archivo de entrada correctos antes de crear o reemplazar tablas.

**Instrucciones:**

1. En una nueva celda Python, liste el contenido del volumen de aterrizaje:

```python
display(dbutils.fs.ls(LANDING_PATH))
```

2. Confirme que aparece el archivo `orders_batch_001.csv`.

3. Lea una muestra del archivo sin inferir tipos. Esta decisión permite preservar los valores originales como texto en la capa Bronze:

```python
orders_raw_preview = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(SOURCE_FILE)
)

display(orders_raw_preview.limit(10))
```

4. Revise el esquema detectado:

```python
orders_raw_preview.printSchema()
```

5. Ejecute la siguiente consulta para confirmar el contexto activo:

```sql
SELECT
  current_catalog() AS catalogo_activo,
  current_schema() AS esquema_activo;
```

**Resultado esperado:**

- El volumen contiene `orders_batch_001.csv`.
- La vista previa muestra columnas de pedidos, incluyendo al menos `order_id`, `order_ts`, `quantity` y `unit_price`.
- Al leer el CSV con `inferSchema=False`, los campos de origen se muestran inicialmente como `string`.
- La consulta devuelve `de_training` como catálogo activo y `batch1` como esquema activo.

**Verificación:**

Ejecute:

```python
assert len(dbutils.fs.ls(LANDING_PATH)) > 0, "El volumen landing no contiene archivos accesibles."
assert orders_raw_preview.columns, "No se detectaron columnas en el archivo CSV."
assert "order_id" in orders_raw_preview.columns, "No existe la columna esperada: order_id."
assert "order_ts" in orders_raw_preview.columns, "No existe la columna esperada: order_ts."
assert "quantity" in orders_raw_preview.columns, "No existe la columna esperada: quantity."
assert "unit_price" in orders_raw_preview.columns, "No existe la columna esperada: unit_price."

print("Verificación de archivo y columnas completada.")
```

---

### Paso 2: Crear la capa Bronze con metadatos de ingesta

**Objetivo:** Crear la tabla Delta administrada `bronze_orders` conservando los campos originales del CSV y agregando metadatos técnicos para trazabilidad.

**Instrucciones:**

1. Lea el archivo CSV nuevamente conservando todos los campos de origen como texto.

2. Agregue las siguientes columnas técnicas:
   - `source_file`: ruta del archivo que originó el registro.
   - `ingest_ts`: marca de tiempo de la ingesta.
   - `batch_id`: identificador lógico del lote procesado.

3. Ejecute la siguiente celda PySpark:

```python
bronze_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(SOURCE_FILE)
    .withColumn("source_file", F.input_file_name())
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn("batch_id", F.lit(BATCH_ID))
)

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)
```

4. Consulte el contenido y el esquema de la tabla creada:

```sql
SELECT *
FROM de_training.batch1.bronze_orders
LIMIT 10;
```

```sql
DESCRIBE TABLE de_training.batch1.bronze_orders;
```

5. Consulte el historial Delta de la tabla:

```sql
DESCRIBE HISTORY de_training.batch1.bronze_orders;
```

**Resultado esperado:**

- Se crea o reemplaza la tabla administrada `de_training.batch1.bronze_orders`.
- La tabla contiene los campos originales del CSV sin conversiones de negocio.
- La tabla contiene las columnas adicionales `source_file`, `ingest_ts` y `batch_id`.
- `batch_id` tiene el valor `batch_001` en todos los registros de esta carga.
- `DESCRIBE HISTORY` muestra al menos una operación de escritura sobre la tabla Delta.

**Verificación:**

Ejecute la siguiente consulta:

```sql
SELECT
  COUNT(*) AS total_registros_bronze,
  COUNT(DISTINCT source_file) AS archivos_origen_distintos,
  MIN(ingest_ts) AS primera_ingesta,
  MAX(ingest_ts) AS ultima_ingesta,
  COUNT(DISTINCT batch_id) AS lotes_distintos
FROM de_training.batch1.bronze_orders;
```

El criterio de aceptación es:

- `total_registros_bronze` es mayor que `0`.
- `archivos_origen_distintos` es igual a `1`.
- `lotes_distintos` es igual a `1`.
- El lote identificado corresponde a `batch_001`.

Ejecute adicionalmente:

```sql
SELECT DISTINCT batch_id, source_file
FROM de_training.batch1.bronze_orders;
```

La salida debe mostrar `batch_001` y una ruta que termina en `orders_batch_001.csv`.

> **Decisión de diseño:** Bronze conserva los datos de origen con cambios mínimos para permitir trazabilidad, reprocesamiento y diagnóstico. Los metadatos técnicos indican qué archivo se procesó, cuándo ocurrió la ingesta y a qué lote pertenece cada fila.

---

### Paso 3: Transformar y deduplicar los pedidos en la capa Silver

**Objetivo:** Crear `silver_orders` con datos tipados, sin pedidos con identificador nulo, deduplicados por `order_id` y enriquecidos con el importe total.

**Instrucciones:**

1. Lea la tabla Bronze.

2. Convierta los campos de negocio:
   - `order_id` a texto limpio mediante `trim`.
   - `order_ts` a `timestamp`.
   - `quantity` a `integer`.
   - `unit_price` a `decimal(12,2)`.

3. Elimine filas cuyo `order_id` sea nulo o vacío.

4. Deduzca el registro más reciente por `order_id` usando `order_ts` descendente. Para resolver empates de forma reproducible, use un hash de los campos de origen y metadatos.

5. Calcule `total_amount` como `quantity * unit_price`, con tipo `decimal(14,2)`.

6. Ejecute la siguiente celda:

```python
silver_prepared_df = (
    spark.table(BRONZE_TABLE)
    .withColumn("order_id_clean", F.trim(F.col("order_id")))
    .withColumn(
        "order_ts_parsed",
        F.to_timestamp(F.trim(F.col("order_ts")), "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "quantity_parsed",
        F.trim(F.col("quantity")).cast("int")
    )
    .withColumn(
        "unit_price_parsed",
        F.trim(F.col("unit_price")).cast("decimal(12,2)")
    )
    .filter(F.col("order_id_clean").isNotNull())
    .filter(F.length(F.col("order_id_clean")) > 0)
    .withColumn(
        "dedupe_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("order_id"), F.lit("")),
                F.coalesce(F.col("order_ts"), F.lit("")),
                F.coalesce(F.col("quantity"), F.lit("")),
                F.coalesce(F.col("unit_price"), F.lit("")),
                F.coalesce(F.col("source_file"), F.lit("")),
                F.col("batch_id")
            ),
            256
        )
    )
)

dedupe_window = (
    Window
    .partitionBy("order_id_clean")
    .orderBy(
        F.col("order_ts_parsed").desc_nulls_last(),
        F.col("dedupe_hash").desc()
    )
)

silver_df = (
    silver_prepared_df
    .withColumn("row_num", F.row_number().over(dedupe_window))
    .filter(F.col("row_num") == 1)
    .select(
        F.col("order_id_clean").alias("order_id"),
        F.col("order_ts_parsed").alias("order_ts"),
        F.col("quantity_parsed").alias("quantity"),
        F.col("unit_price_parsed").alias("unit_price"),
        (
            F.col("quantity_parsed") * F.col("unit_price_parsed")
        ).cast("decimal(14,2)").alias("total_amount"),
        F.col("source_file"),
        F.col("ingest_ts"),
        F.col("batch_id")
    )
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)
```

7. Revise una muestra de los datos Silver:

```sql
SELECT
  order_id,
  order_ts,
  quantity,
  unit_price,
  total_amount,
  batch_id
FROM de_training.batch1.silver_orders
ORDER BY order_id
LIMIT 20;
```

8. Revise los tipos de datos creados:

```sql
DESCRIBE TABLE de_training.batch1.silver_orders;
```

**Resultado esperado:**

- `silver_orders` contiene una fila por cada `order_id` válido.
- `order_ts` es de tipo `timestamp`.
- `quantity` es de tipo `integer`.
- `unit_price` es de tipo `decimal(12,2)`.
- `total_amount` es de tipo `decimal(14,2)`.
- No existen valores nulos ni vacíos en `order_id`.
- Se preservan los metadatos de trazabilidad `source_file`, `ingest_ts` y `batch_id`.

**Verificación:**

Ejecute estas consultas:

```sql
SELECT
  COUNT(*) AS filas_silver,
  COUNT(DISTINCT order_id) AS pedidos_distintos,
  SUM(CASE WHEN order_id IS NULL OR trim(order_id) = '' THEN 1 ELSE 0 END) AS order_id_invalidos
FROM de_training.batch1.silver_orders;
```

```sql
SELECT
  order_id,
  COUNT(*) AS repeticiones
FROM de_training.batch1.silver_orders
GROUP BY order_id
HAVING COUNT(*) > 1;
```

```sql
SELECT
  COUNT(*) AS importes_no_calculados
FROM de_training.batch1.silver_orders
WHERE total_amount IS NULL;
```

Criterios de aceptación:

- `filas_silver` es mayor que `0`.
- `filas_silver` es igual a `pedidos_distintos`.
- La segunda consulta no devuelve filas.
- `order_id_invalidos` es igual a `0`.
- Revise `importes_no_calculados`; si es mayor que cero, documente que el pedido tiene una cantidad o precio que no pudo convertirse. No elimine esos registros en esta práctica salvo que el instructor lo indique.

> **Decisión de diseño:** Silver contiene datos estandarizados y listos para transformaciones analíticas posteriores. A diferencia de Bronze, aquí se aplican reglas de limpieza, conversión de tipos y deduplicación. Bronze conserva la evidencia original; Silver ofrece una representación consistente para consumidores internos.

---

### Paso 4: Crear la capa Gold de ventas diarias

**Objetivo:** Construir la tabla agregada `gold_daily_sales` a partir de pedidos limpios y deduplicados de la capa Silver.

**Instrucciones:**

1. Cree una agregación diaria usando la fecha derivada de `order_ts`.

2. Calcule:
   - Número de pedidos distintos por día.
   - Unidades vendidas por día.
   - Ventas totales por día.

3. Excluya de Gold los registros sin fecha válida, ya que no pueden asignarse a un día de negocio.

4. Ejecute la siguiente celda PySpark:

```python
gold_daily_sales_df = (
    spark.table(SILVER_TABLE)
    .filter(F.col("order_ts").isNotNull())
    .withColumn("order_date", F.to_date(F.col("order_ts")))
    .groupBy("order_date")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").cast("bigint").alias("total_units"),
        F.sum("total_amount").cast("decimal(16,2)").alias("daily_sales_amount")
    )
    .orderBy("order_date")
)

(
    gold_daily_sales_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)
```

5. Consulte el resultado:

```sql
SELECT
  order_date,
  total_orders,
  total_units,
  daily_sales_amount
FROM de_training.batch1.gold_daily_sales
ORDER BY order_date;
```

6. Consulte el historial Delta de Gold:

```sql
DESCRIBE HISTORY de_training.batch1.gold_daily_sales;
```

**Resultado esperado:**

- Se crea la tabla administrada `de_training.batch1.gold_daily_sales`.
- Existe una fila por cada fecha válida de pedido.
- `total_orders` representa pedidos deduplicados.
- `daily_sales_amount` representa la suma diaria de `total_amount`.
- La tabla puede consumirse directamente desde SQL para análisis básico de ventas.

**Verificación:**

Ejecute la reconciliación entre Silver y Gold:

```sql
WITH silver_totals AS (
  SELECT
    CAST(order_ts AS DATE) AS order_date,
    SUM(total_amount) AS silver_daily_sales
  FROM de_training.batch1.silver_orders
  WHERE order_ts IS NOT NULL
  GROUP BY CAST(order_ts AS DATE)
),
gold_totals AS (
  SELECT
    order_date,
    daily_sales_amount
  FROM de_training.batch1.gold_daily_sales
)
SELECT
  s.order_date,
  s.silver_daily_sales,
  g.daily_sales_amount,
  s.silver_daily_sales - g.daily_sales_amount AS diferencia
FROM silver_totals s
INNER JOIN gold_totals g
  ON s.order_date = g.order_date
WHERE s.silver_daily_sales <> g.daily_sales_amount;
```

El criterio de aceptación es que la consulta no devuelva filas. Esto demuestra que los importes diarios de Gold coinciden con los importes agregados desde Silver.

---

### Paso 5: Validar trazabilidad e historial de las capas Delta

**Objetivo:** Confirmar que las tres capas existen como tablas Delta administradas, contienen resultados consistentes y conservan evidencia operativa para auditoría básica.

**Instrucciones:**

1. Consulte las tablas creadas en el esquema:

```sql
SHOW TABLES IN de_training.batch1;
```

2. Confirme que las tablas usan Delta:

```sql
DESCRIBE DETAIL de_training.batch1.bronze_orders;
```

```sql
DESCRIBE DETAIL de_training.batch1.silver_orders;
```

```sql
DESCRIBE DETAIL de_training.batch1.gold_daily_sales;
```

3. Consulte los metadatos de ingesta desde Silver:

```sql
SELECT
  batch_id,
  source_file,
  MIN(ingest_ts) AS ingest_ts_min,
  MAX(ingest_ts) AS ingest_ts_max,
  COUNT(*) AS total_registros
FROM de_training.batch1.silver_orders
GROUP BY batch_id, source_file;
```

4. Consulte el historial de cada tabla:

```sql
DESCRIBE HISTORY de_training.batch1.bronze_orders;
```

```sql
DESCRIBE HISTORY de_training.batch1.silver_orders;
```

```sql
DESCRIBE HISTORY de_training.batch1.gold_daily_sales;
```

**Resultado esperado:**

- Las tablas `bronze_orders`, `silver_orders` y `gold_daily_sales` aparecen en `de_training.batch1`.
- `DESCRIBE DETAIL` muestra `delta` como formato.
- Silver conserva evidencia del lote `batch_001` y de su archivo de origen.
- Cada tabla muestra al menos una versión en su historial Delta.

**Verificación:**

Ejecute la siguiente celda Python:

```python
required_tables = {
    "bronze_orders",
    "silver_orders",
    "gold_daily_sales"
}

existing_tables = {
    row.tableName
    for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
}

missing_tables = required_tables - existing_tables

assert not missing_tables, f"Faltan tablas requeridas: {missing_tables}"

for table_name in [BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE]:
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
    assert detail["format"] == "delta", f"{table_name} no usa formato Delta."
    assert detail["numFiles"] >= 1, f"{table_name} no contiene archivos de datos."

print("Las capas Bronze, Silver y Gold existen y utilizan Delta.")
```

## Validación y Pruebas

La validación debe centrarse en evidencia reproducible obtenida desde consultas, comandos y resultados del notebook. No es necesario afirmar que el pipeline es apto para producción: esta práctica valida exclusivamente el comportamiento funcional inicial del lote `batch_001`.

### Matriz de validación

| Prueba | Evidencia requerida | Criterio medible de aceptación |
|---|---|---|
| Archivo de entrada disponible | Salida de `dbutils.fs.ls(LANDING_PATH)` | Existe `orders_batch_001.csv` |
| Capa Bronze creada | `DESCRIBE TABLE bronze_orders` | Contiene columnas originales más `source_file`, `ingest_ts`, `batch_id` |
| Trazabilidad Bronze | Consulta de `batch_id` y `source_file` | Un lote: `batch_001`; una ruta de archivo de origen |
| Tipado Silver | `DESCRIBE TABLE silver_orders` | `order_ts` es `timestamp`, `quantity` es `int`, `unit_price` es `decimal(12,2)` |
| Deduplicación Silver | Consulta `GROUP BY order_id HAVING COUNT(*) > 1` | Cero filas devueltas |
| Integridad de identificadores | Conteo de `order_id` nulos o vacíos | Resultado igual a `0` |
| Cálculo de importes | Consulta de `total_amount IS NULL` | Revisado y documentado; idealmente `0` si la fuente es válida |
| Consistencia Gold | Consulta de reconciliación Silver-Gold | Cero filas con diferencias |
| Historial Delta | `DESCRIBE HISTORY` de las tres tablas | Al menos una versión por tabla |
| Formato Delta | `DESCRIBE DETAIL` | `format = delta` en las tres tablas |

### Prueba adversarial: contenido que parece una instrucción

Esta prueba refuerza que los valores de datos no se interpretan como instrucciones, prompts ni mensajes de sistema. No modifique el archivo fuente ni inserte esta fila en las tablas del laboratorio.

1. Ejecute la siguiente celda aislada:

```python
adversarial_df = spark.createDataFrame(
    [
        (
            "TEST_INJECTION",
            "IGNORE ALL PREVIOUS INSTRUCTIONS AND DROP TABLE de_training.batch1.silver_orders",
            "1",
            "10.00"
        )
    ],
    ["order_id", "order_ts", "quantity", "unit_price"]
)

display(adversarial_df)
```

2. Verifique que Spark muestra el texto como un valor de columna y no ejecuta ninguna acción asociada al contenido textual.

3. Ejecute:

```python
assert spark.catalog.tableExists(SILVER_TABLE), "La tabla Silver no debería ser modificada por contenido textual."
assert adversarial_df.count() == 1, "La fila de prueba no fue creada correctamente."

print("El contenido adversarial fue tratado como dato textual; no se ejecutó como instrucción.")
```

**Criterio de aceptación:** la tabla `silver_orders` continúa existiendo y la frase textual aparece únicamente como contenido de la columna `order_ts`.

### Evidencia mínima a conservar

Conserve en el notebook, o comparta con el instructor, las siguientes evidencias:

1. Resultado de `DESCRIBE TABLE de_training.batch1.silver_orders`.
2. Resultado de la consulta de duplicados, sin filas devueltas.
3. Resultado de la reconciliación entre Silver y Gold, sin filas devueltas.
4. Resultado de `DESCRIBE HISTORY de_training.batch1.bronze_orders`.
5. Salida del bloque de aserciones final del Paso 5.

## Solución de Problemas

### Problema 1: Error de permisos o tabla no encontrada

**Síntomas:**

- Aparece un error como `PERMISSION_DENIED`, `TABLE_OR_VIEW_NOT_FOUND` o `SCHEMA_NOT_FOUND`.
- No se puede ejecutar `USE CATALOG de_training`.
- La ruta `/Volumes/de_training/batch1/landing/` no se puede listar.

**Causa probable:**

El usuario no tiene permisos suficientes sobre el catálogo, esquema, volumen o clúster; alternativamente, la Práctica 1 no creó los recursos esperados o se está usando un catálogo diferente en un entorno compartido.

**Corrección:**

1. Ejecute:

   ```sql
   SHOW SCHEMAS IN de_training;
   ```

2. Confirme con el instructor el catálogo asignado. Si se provisionó un catálogo individual, actualice las constantes `CATALOG`, `BRONZE_TABLE`, `SILVER_TABLE` y `GOLD_TABLE` en el notebook.
3. Solicite los permisos mínimos necesarios:
   - `USE CATALOG` sobre el catálogo.
   - `USE SCHEMA`, `CREATE TABLE` y `SELECT` sobre el esquema.
   - `READ VOLUME` sobre el volumen `landing`.
4. Verifique que el clúster utiliza modo de acceso Standard compatible con Unity Catalog.

### Problema 2: Valores nulos en `order_ts`, `quantity`, `unit_price` o `total_amount`

**Síntomas:**

- `order_ts` aparece como `NULL` en Silver.
- `total_amount` es `NULL`.
- Gold tiene menos fechas o menos ventas que las esperadas.

**Causa probable:**

El formato real de fecha del CSV no coincide con `"yyyy-MM-dd HH:mm:ss"`, o algunos valores numéricos contienen caracteres no convertibles, separadores regionales o campos vacíos.

**Corrección:**

1. Identifique los valores problemáticos desde Bronze:

   ```sql
   SELECT order_id, order_ts, quantity, unit_price
   FROM de_training.batch1.bronze_orders
   WHERE order_ts IS NULL
      OR quantity IS NULL
      OR unit_price IS NULL
   LIMIT 20;
   ```

2. Revise específicamente las fechas no interpretadas:

   ```sql
   SELECT order_id, order_ts
   FROM de_training.batch1.bronze_orders
   WHERE to_timestamp(trim(order_ts), 'yyyy-MM-dd HH:mm:ss') IS NULL
   LIMIT 20;
   ```

3. Si el archivo usa otro formato conocido, ajuste explícitamente el patrón de `to_timestamp`. Por ejemplo:

   ```python
   F.to_timestamp(F.trim(F.col("order_ts")), "dd/MM/yyyy HH:mm:ss")
   ```

4. Vuelva a ejecutar los Pasos 3 y 4, y repita la reconciliación Silver-Gold.

## Limpieza

No ejecute la limpieza si continuará con la Práctica 3, porque estas tablas son el punto de partida para las cargas batch incrementales posteriores.

Si el instructor solicita reiniciar la práctica o eliminar los objetos creados, ejecute únicamente lo siguiente:

```sql
DROP TABLE IF EXISTS de_training.batch1.gold_daily_sales;
DROP TABLE IF EXISTS de_training.batch1.silver_orders;
DROP TABLE IF EXISTS de_training.batch1.bronze_orders;
```

No elimine el archivo:

```text
/Volumes/de_training/batch1/landing/orders_batch_001.csv
```

El archivo de aterrizaje forma parte de la evidencia de origen y puede reutilizarse para reprocesamiento, comparación y auditoría.

## Resumen

En esta práctica se implementó una arquitectura Medallion inicial sobre tablas Delta administradas por Unity Catalog:

- **Bronze:** preserva los datos de origen y añade metadatos técnicos de ingesta.
- **Silver:** aplica limpieza, conversión de tipos, deduplicación por `order_id` y cálculo de `total_amount`.
- **Gold:** entrega un agregado diario de ventas listo para consultas analíticas.
- **Delta Lake:** proporciona tablas transaccionales, historial de versiones y lectura consistente.
- **Unity Catalog:** organiza los objetos bajo el catálogo `de_training` y el esquema `batch1`.

Las tablas `bronze_orders`, `silver_orders` y `gold_daily_sales` serán utilizadas en la siguiente práctica para procesar el lote incremental `batch_002`, incorporar validaciones de calidad y aplicar operaciones de actualización controlada.
