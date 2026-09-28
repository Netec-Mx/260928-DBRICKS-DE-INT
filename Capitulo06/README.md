# Optimización de pipeline y comparación de métricas

## Metadatos

| Campo | Valor |
|---|---|
| Duración | 65 minutos |
| Complejidad | Difícil |
| Nivel de Bloom | Analizar |

## Descripción General

En esta práctica se construirá una línea base reproducible para comparar dos diseños físicos de tablas Delta: particionado estático por una clave de alta cardinalidad y Liquid Clustering por `customer_id` y `event_date`. Se ejecutará un conjunto equivalente de consultas selectivas y agregadas, se recopilarán métricas de Spark UI y se registrarán resultados comparables en una tabla Delta gobernada por Unity Catalog.

La práctica no busca asumir que una estrategia siempre es superior: la recomendación final deberá basarse en evidencia obtenida con los patrones de consulta ejecutados, la distribución de tareas, el shuffle, la lectura y la fragmentación física observada.

> **Nota de alcance:** aunque otras prácticas usan `de_training.batch1`, esta práctica utiliza el catálogo y esquema definidos para benchmarking: `de_training_batch3.performance`. Si el instructor provisionó un catálogo equivalente por equipo, sustituya el prefijo de catálogo de forma consistente en todos los comandos.

## Objetivos de Aprendizaje

- [ ] Establecer una línea base reproducible para consultas Delta con un clúster fijo de exactamente dos workers.
- [ ] Comparar el costo físico de particionar estáticamente por `customer_id` frente a usar Liquid Clustering.
- [ ] Interpretar en Spark UI la duración de stages, número de tasks, lectura de entrada, shuffle, spill y señales de skew.
- [ ] Registrar ejecuciones comparables en `de_training_batch3.performance.benchmark_metrics`.
- [ ] Emitir una recomendación técnica trazable para la Práctica 13.

## Prerrequisitos

### Conocimientos requeridos

- Arquitectura Medallion y función de las tablas Delta como contratos entre etapas.
- Operaciones Spark SQL: `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, `CREATE TABLE`, `INSERT` y `OPTIMIZE`.
- Conceptos de Spark: job, stage, task, shuffle, skew, spill, Adaptive Query Execution (AQE) y Spark UI.
- Diferencia entre un particionado físico estático y Liquid Clustering.
- Comprensión de que un benchmark requiere condiciones repetibles; no deben cambiarse el tamaño del clúster, los datos ni las configuraciones durante las ejecuciones medidas.

### Accesos requeridos

- Workspace de Azure Databricks con Unity Catalog habilitado.
- Clúster con acceso en modo **Standard**, Databricks Runtime 15.4 LTS with Photon y **exactamente 2 workers**.
- Permisos `USE CATALOG`, `USE SCHEMA`, `CREATE SCHEMA`, `CREATE TABLE`, `MODIFY`, `SELECT` y `OPTIMIZE` sobre `de_training_batch3`.
- Acceso a Spark UI del clúster.
- Acceso a la vista segura `vw_sales_masked` creada en la Práctica 11. Solicite al instructor el nombre completamente calificado si no fue documentado.
- Al menos 20 GB de almacenamiento disponible para las tablas Delta de benchmark y sus archivos.

## Entorno de Laboratorio

### Configuración de hardware

| Recurso | Configuración requerida |
|---|---|
| Driver | 1 nodo |
| Workers | Exactamente 2 nodos durante toda la línea base |
| Capacidad mínima por nodo | 4 vCPU y 14 GB de memoria |
| Escalado automático | Deshabilitado para esta práctica |
| Motor | Photon habilitado |

### Versiones de software

| Tecnología | Versión/edición requerida | Arquitectura o modo | Fuente oficial |
|---|---|---|---|
| Azure Databricks Runtime | 15.4 LTS with Photon | Clúster Standard, x86_64 | <https://learn.microsoft.com/azure/databricks/release-notes/runtime/15.4lts> |
| Apache Spark | 3.5.0 | Incluido en Databricks Runtime 15.4 LTS | <https://spark.apache.org/releases/spark-release-3-5-0.html> |
| Delta Lake | 3.2.0 | Incluido en Databricks Runtime 15.4 LTS | <https://docs.delta.io/latest/releases.html> |
| Python | 3.11.0 | Incluido en Databricks Runtime 15.4 LTS | <https://www.python.org/downloads/release/python-3110/> |
| Unity Catalog | Servicio Databricks compatible con Runtime 15.4 LTS | Gobierno de datos SaaS | <https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/> |
| Liquid Clustering | Compatible con Delta Lake en Databricks Runtime 15.4 LTS | Tablas Delta administradas | <https://learn.microsoft.com/azure/databricks/delta/clustering> |
| Spark UI | Incluida en Azure Databricks Runtime 15.4 LTS | Interfaz web del clúster | <https://learn.microsoft.com/azure/databricks/compute/spark-ui> |

### Configuración inicial del notebook

1. Cree un notebook en una carpeta de trabajo del equipo, por ejemplo:

   ```text
   /Workspace/Users/<usuario>/batch_3/12_performance_benchmark
   ```

2. Adjunte el notebook al clúster asignado.

3. Ejecute la siguiente celda Python para declarar las constantes del laboratorio:

   ```python
   CATALOG = "de_training_batch3"
   SCHEMA = "performance"

   SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.events_source"
   PARTITIONED_TABLE = f"{CATALOG}.{SCHEMA}.events_partitioned_customer"
   LIQUID_TABLE = f"{CATALOG}.{SCHEMA}.events_liquid"
   METRICS_TABLE = f"{CATALOG}.{SCHEMA}.benchmark_metrics"
   RECOMMENDATION_TABLE = f"{CATALOG}.{SCHEMA}.benchmark_recommendation"

   TOTAL_ROWS = 2_000_000
   EXPECTED_WORKERS = 2
   ```

4. Configure parámetros fijos de Spark. Estas configuraciones forman parte de la línea base; no se modifican durante las ejecuciones medidas.

   ```python
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
   spark.conf.set("spark.sql.shuffle.partitions", "200")
   spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
   ```

## Instrucciones Paso a Paso

### Paso 1: Verificar el clúster, el acceso y la referencia funcional

**Objetivo:** confirmar que el entorno cumple las condiciones de línea base antes de crear datos de benchmark.

**Instrucciones:**

1. En la interfaz de Databricks, abra la configuración del clúster y compruebe manualmente:

   - Databricks Runtime: `15.4 LTS with Photon`.
   - Modo de acceso: `Standard`.
   - Número de workers: `2`.
   - Escalado automático: deshabilitado o configurado con mínimo y máximo de 2 workers.

2. Ejecute la siguiente celda para comprobar la versión de Spark, configuraciones y contexto de ejecución:

   ```python
   print("Spark version:", spark.version)
   print("AQE:", spark.conf.get("spark.sql.adaptive.enabled"))
   print("AQE skew join:", spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))
   print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
   print("Application ID:", spark.sparkContext.applicationId)
   print("Spark UI URL:", spark.sparkContext.uiWebUrl)
   ```

3. Compruebe que puede usar el catálogo y cree el esquema si no existe:

   ```sql
   CREATE SCHEMA IF NOT EXISTS de_training_batch3.performance;

   USE CATALOG de_training_batch3;
   USE SCHEMA performance;

   SHOW SCHEMAS IN de_training_batch3;
   ```

4. Verifique la existencia de la vista segura de la Práctica 11. Sustituya el nombre por el nombre completamente calificado entregado por el instructor.

   ```sql
   -- Sustituya <catalogo.esquema.vw_sales_masked>
   SELECT *
   FROM <catalogo.esquema.vw_sales_masked>
   LIMIT 5;
   ```

5. No use la vista segura como fuente de datos de benchmark. La vista se valida como referencia funcional y de gobierno; los datos de rendimiento serán sintéticos y no sensibles.

**Resultado esperado:**

- Spark informa versión `3.5.0`.
- AQE y detección de skew para joins aparecen como `true`.
- El esquema `de_training_batch3.performance` existe.
- La consulta a `vw_sales_masked` devuelve hasta cinco filas o un resultado vacío válido, sin error de permisos.

**Verificación:**

- Registre en una celda Markdown del notebook el identificador de aplicación Spark y la fecha/hora UTC de inicio.
- Confirme visualmente que el clúster tiene exactamente dos workers antes de continuar.
- Si el número de workers no es dos, no continúe: la comparación dejaría de ser una línea base controlada.

### Paso 2: Crear las tablas de auditoría del benchmark

**Objetivo:** preparar tablas Delta para registrar parámetros, métricas observadas y la recomendación final.

**Instrucciones:**

1. Cree la tabla de métricas. Las columnas de Spark UI se poblarán manualmente después de cada ejecución medida.

   ```sql
   CREATE TABLE IF NOT EXISTS de_training_batch3.performance.benchmark_metrics (
     run_id STRING NOT NULL,
     benchmark_timestamp TIMESTAMP NOT NULL,
     physical_design STRING NOT NULL,
     table_name STRING NOT NULL,
     query_name STRING NOT NULL,
     query_sql STRING NOT NULL,
     execution_number INT NOT NULL,
     is_warmup BOOLEAN NOT NULL,
     duration_seconds DOUBLE NOT NULL,
     result_row_count BIGINT NOT NULL,
     file_count BIGINT,
     input_size_bytes BIGINT,
     task_count BIGINT,
     shuffle_read_bytes BIGINT,
     shuffle_write_bytes BIGINT,
     spilled_bytes BIGINT,
     skew_observed BOOLEAN,
     skew_evidence STRING,
     spark_ui_reference STRING,
     spark_application_id STRING NOT NULL,
     notes STRING
   )
   USING DELTA;
   ```

2. Cree la tabla que conservará la recomendación inicial para la Práctica 13.

   ```sql
   CREATE TABLE IF NOT EXISTS de_training_batch3.performance.benchmark_recommendation (
     recommendation_timestamp TIMESTAMP NOT NULL,
     recommended_design STRING NOT NULL,
     workload_scope STRING NOT NULL,
     evidence_summary STRING NOT NULL,
     tradeoffs STRING NOT NULL,
     approved_by STRING NOT NULL
   )
   USING DELTA;
   ```

3. Elimine registros de intentos anteriores de este laboratorio si el instructor autorizó reiniciar el benchmark. Esta eliminación no borra las tablas físicas de benchmark.

   ```sql
   DELETE FROM de_training_batch3.performance.benchmark_metrics;

   DELETE FROM de_training_batch3.performance.benchmark_recommendation;
   ```

4. Confirme la estructura de ambas tablas.

   ```sql
   DESCRIBE EXTENDED de_training_batch3.performance.benchmark_metrics;
   DESCRIBE EXTENDED de_training_batch3.performance.benchmark_recommendation;
   ```

**Resultado esperado:**

- Existen dos tablas Delta administradas por Unity Catalog.
- `benchmark_metrics` contiene columnas para duración, entrada, tasks, shuffle, spill, número de archivos y skew.
- `benchmark_recommendation` está vacía y lista para recibir una conclusión basada en evidencia.

**Verificación:**

```sql
SHOW TABLES IN de_training_batch3.performance;
```

La salida debe incluir, como mínimo, `benchmark_metrics` y `benchmark_recommendation`.

### Paso 3: Generar datos deterministas y crear los dos diseños físicos

**Objetivo:** construir dos tablas equivalentes con 2.000.000 filas y estrategias físicas diferentes.

**Instrucciones:**

1. Elimine tablas de benchmark de intentos anteriores para garantizar que el estado físico sea reproducible.

   ```sql
   DROP TABLE IF EXISTS de_training_batch3.performance.events_source;
   DROP TABLE IF EXISTS de_training_batch3.performance.events_partitioned_customer;
   DROP TABLE IF EXISTS de_training_batch3.performance.events_liquid;
   ```

2. Cree una tabla fuente sintética determinista. El uso de `range(2000000)` permite que todos los equipos generen la misma distribución lógica.

   ```sql
   CREATE TABLE de_training_batch3.performance.events_source
   USING DELTA
   AS
   SELECT
     id AS event_id,
     CONCAT('C', LPAD(CAST(PMOD(id, 10000) AS STRING), 5, '0')) AS customer_id,
     CASE
       WHEN PMOD(id, 100) < 55 THEN 'US'
       WHEN PMOD(id, 100) < 70 THEN 'MX'
       WHEN PMOD(id, 100) < 82 THEN 'ES'
       WHEN PMOD(id, 100) < 92 THEN 'CO'
       ELSE 'AR'
     END AS country,
     DATE_ADD(DATE'2024-01-01', CAST(PMOD(id, 90) AS INT)) AS event_date,
     CASE PMOD(id, 5)
       WHEN 0 THEN 'view'
       WHEN 1 THEN 'search'
       WHEN 2 THEN 'add_to_cart'
       WHEN 3 THEN 'purchase'
       ELSE 'refund'
     END AS event_type,
     CAST(
       ROUND(
         5.0 + PMOD(id * 13, 9500) / 100.0,
         2
       ) AS DECIMAL(12,2)
     ) AS amount
   FROM range(2000000);
   ```

3. Cree la tabla con el diseño deliberadamente inadecuado: particionado estático únicamente por `customer_id`. Aunque el valor tiene alta cardinalidad, se utiliza en esta práctica para observar el efecto de crear muchas particiones físicas.

   ```sql
   CREATE TABLE de_training_batch3.performance.events_partitioned_customer
   USING DELTA
   PARTITIONED BY (customer_id)
   AS
   SELECT
     event_id,
     customer_id,
     country,
     event_date,
     event_type,
     amount
   FROM de_training_batch3.performance.events_source;
   ```

4. Cree la tabla con Liquid Clustering por `customer_id` y `event_date`.

   ```sql
   CREATE TABLE de_training_batch3.performance.events_liquid
   USING DELTA
   CLUSTER BY (customer_id, event_date)
   AS
   SELECT
     event_id,
     customer_id,
     country,
     event_date,
     event_type,
     amount
   FROM de_training_batch3.performance.events_source;
   ```

5. Ejecute compactación y clustering para materializar la organización física de la tabla Liquid.

   ```sql
   OPTIMIZE de_training_batch3.performance.events_liquid;
   ```

6. Compruebe que ambas tablas tienen la misma cantidad de filas y el mismo importe agregado.

   ```sql
   SELECT
     'partitioned_customer' AS design,
     COUNT(*) AS row_count,
     SUM(amount) AS total_amount
   FROM de_training_batch3.performance.events_partitioned_customer

   UNION ALL

   SELECT
     'liquid' AS design,
     COUNT(*) AS row_count,
     SUM(amount) AS total_amount
   FROM de_training_batch3.performance.events_liquid;
   ```

7. Examine los metadatos físicos de cada tabla.

   ```sql
   DESCRIBE DETAIL de_training_batch3.performance.events_partitioned_customer;

   DESCRIBE DETAIL de_training_batch3.performance.events_liquid;
   ```

**Resultado esperado:**

- Ambas tablas tienen `2,000,000` filas.
- La suma de `amount` es idéntica en ambas tablas.
- La tabla `events_partitioned_customer` informa `customer_id` como columna de partición.
- La tabla `events_liquid` informa claves de clustering que incluyen `customer_id` y `event_date`.
- La tabla particionada presenta previsiblemente más archivos o fragmentación asociada a sus 10.000 valores de cliente.

**Verificación:**

Ejecute:

```sql
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT customer_id) AS distinct_customers,
  COUNT(DISTINCT country) AS distinct_countries,
  COUNT(DISTINCT event_date) AS distinct_dates
FROM de_training_batch3.performance.events_source;
```

Criterios esperados:

| Métrica | Valor esperado |
|---|---:|
| `row_count` | 2.000.000 |
| `distinct_customers` | 10.000 |
| `distinct_countries` | 5 |
| `distinct_dates` | 90 |

### Paso 4: Definir un conjunto controlado de consultas

**Objetivo:** establecer consultas equivalentes que representen acceso selectivo, agregación analítica y redistribución por una clave de alta cardinalidad.

**Instrucciones:**

1. Ejecute la siguiente celda Python para declarar las tres consultas. El marcador `{table}` se reemplazará por cada diseño físico.

   ```python
   QUERIES = {
       "Q1_selective_customer_date": """
           SELECT
             customer_id,
             event_date,
             event_type,
             SUM(amount) AS total_amount
           FROM {table}
           WHERE customer_id BETWEEN 'C00000' AND 'C00499'
             AND event_date BETWEEN DATE'2024-02-01' AND DATE'2024-02-29'
           GROUP BY customer_id, event_date, event_type
       """,
       "Q2_country_daily_aggregation": """
           SELECT
             country,
             event_date,
             COUNT(*) AS event_count,
             SUM(amount) AS total_amount,
             AVG(amount) AS average_amount
           FROM {table}
           WHERE event_date BETWEEN DATE'2024-02-01' AND DATE'2024-03-15'
           GROUP BY country, event_date
           ORDER BY event_date, country
       """,
       "Q3_customer_aggregation": """
           SELECT
             customer_id,
             COUNT(*) AS event_count,
             SUM(amount) AS total_amount
           FROM {table}
           WHERE event_date BETWEEN DATE'2024-01-01' AND DATE'2024-03-30'
           GROUP BY customer_id
           ORDER BY total_amount DESC
           LIMIT 100
       """
   }
   ```

2. Revise el propósito de cada consulta:

   | Consulta | Patrón evaluado | Señales esperadas en Spark UI |
   |---|---|---|
   | Q1 | Filtro por rango de clientes y fechas | Lectura, poda de archivos y costo de planificación |
   | Q2 | Agregación por país y fecha | Shuffle, distribución desigual por `country` y agregación |
   | Q3 | Agregación por cliente | Shuffle de alta cardinalidad, número de tasks y ordenamiento |

3. Ejecute una vez cada consulta sobre ambas tablas únicamente para confirmar que no hay errores de sintaxis. Estas ejecuciones no son parte del benchmark y no deben registrarse.

   ```python
   for table_name in [PARTITIONED_TABLE, LIQUID_TABLE]:
       print(f"Validando consultas sobre: {table_name}")
       for query_name, query_template in QUERIES.items():
           result = spark.sql(query_template.format(table=table_name))
           print(query_name, result.limit(1).collect())
   ```

**Resultado esperado:**

- Las seis validaciones terminan sin error.
- Las consultas Q1 y Q2 devuelven resultados agregados.
- Q3 devuelve hasta 100 filas.
- Las consultas de cada pareja de diseños devuelven resultados lógicamente equivalentes.

**Verificación:**

Compare manualmente el conteo de resultados de cada consulta entre ambas tablas. Por ejemplo:

```python
for query_name, query_template in QUERIES.items():
    partitioned_count = spark.sql(
        query_template.format(table=PARTITIONED_TABLE)
    ).count()

    liquid_count = spark.sql(
        query_template.format(table=LIQUID_TABLE)
    ).count()

    print(
        f"{query_name}: partitioned={partitioned_count}, "
        f"liquid={liquid_count}"
    )
```

Los conteos deben coincidir para cada nombre de consulta.

### Paso 5: Ejecutar el benchmark y registrar la duración automática

**Objetivo:** ejecutar el mismo conjunto de consultas cuatro veces por diseño, descartando la primera ejecución de calentamiento del cálculo comparativo.

**Instrucciones:**

1. Ejecute la siguiente celda Python. Genera 24 registros: 2 diseños × 3 consultas × 4 ejecuciones. La ejecución `0` es calentamiento; las ejecuciones `1`, `2` y `3` son las mediciones comparables.

   ```python
   import time
   import uuid
   from datetime import datetime, timezone

   def get_file_count(table_name: str) -> int:
       detail = spark.sql(f"DESCRIBE DETAIL {table_name}").first().asDict()
       return int(detail["numFiles"])

   def run_benchmark(design: str, table_name: str, query_name: str,
                     query_template: str, execution_number: int):
       query_sql = query_template.format(table=table_name).strip()
       run_id = str(uuid.uuid4())
       is_warmup = execution_number == 0

       start = time.perf_counter()
       result_rows = spark.sql(query_sql).collect()
       duration_seconds = round(time.perf_counter() - start, 3)

       record = [{
           "run_id": run_id,
           "benchmark_timestamp": datetime.now(timezone.utc),
           "physical_design": design,
           "table_name": table_name,
           "query_name": query_name,
           "query_sql": query_sql,
           "execution_number": execution_number,
           "is_warmup": is_warmup,
           "duration_seconds": duration_seconds,
           "result_row_count": len(result_rows),
           "file_count": get_file_count(table_name),
           "input_size_bytes": None,
           "task_count": None,
           "shuffle_read_bytes": None,
           "shuffle_write_bytes": None,
           "spilled_bytes": None,
           "skew_observed": None,
           "skew_evidence": None,
           "spark_ui_reference": (
               f"Spark UI / SQL o Jobs / {design} / "
               f"{query_name} / execution={execution_number}"
           ),
           "spark_application_id": spark.sparkContext.applicationId,
           "notes": "Pendiente de completar con métricas agregadas de Spark UI"
       }]

       spark.createDataFrame(record).write.mode("append").saveAsTable(METRICS_TABLE)

       print(
           f"run_id={run_id} | design={design} | query={query_name} | "
           f"execution={execution_number} | warmup={is_warmup} | "
           f"duration={duration_seconds}s | rows={len(result_rows)}"
       )

   BENCHMARK_TABLES = {
       "static_partition_customer": PARTITIONED_TABLE,
       "liquid_cluster_customer_date": LIQUID_TABLE
   }

   for design, table_name in BENCHMARK_TABLES.items():
       for query_name, query_template in QUERIES.items():
           for execution_number in range(4):
               run_benchmark(
                   design,
                   table_name,
                   query_name,
                   query_template,
                   execution_number
               )
   ```

2. No ejecute otras cargas pesadas en el mismo clúster mientras se ejecuta el benchmark.

3. No cambie las configuraciones de Spark, el tamaño del clúster, el número de workers ni las tablas entre las ejecuciones `0`, `1`, `2` y `3`.

4. Después de completar las ejecuciones, revise el registro básico.

   ```sql
   SELECT
     physical_design,
     query_name,
     execution_number,
     is_warmup,
     duration_seconds,
     result_row_count,
     file_count,
     run_id
   FROM de_training_batch3.performance.benchmark_metrics
   ORDER BY physical_design, query_name, execution_number;
   ```

**Resultado esperado:**

- Existen exactamente 24 filas de benchmark.
- Hay una ejecución de calentamiento y tres ejecuciones medidas para cada combinación diseño-consulta.
- Cada ejecución medida contiene una duración mayor que cero y un identificador `run_id` único.
- Las ejecuciones del mismo nombre de consulta devuelven igual cantidad de filas para ambos diseños.

**Verificación:**

```sql
SELECT
  physical_design,
  query_name,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN is_warmup THEN 1 ELSE 0 END) AS warmup_runs,
  SUM(CASE WHEN NOT is_warmup THEN 1 ELSE 0 END) AS measured_runs
FROM de_training_batch3.performance.benchmark_metrics
GROUP BY physical_design, query_name
ORDER BY physical_design, query_name;
```

Cada combinación debe mostrar:

- `total_runs = 4`
- `warmup_runs = 1`
- `measured_runs = 3`

### Paso 6: Completar métricas desde Spark UI

**Objetivo:** recopilar de manera consistente la evidencia de ejecución necesaria para analizar lectura, tasks, shuffle, spill y skew.

**Instrucciones:**

1. Abra Spark UI desde el detalle del clúster o desde el vínculo mostrado por `spark.sparkContext.uiWebUrl`.

2. Use las pestañas **Jobs**, **Stages** y **SQL/DataFrame** para localizar las consultas ejecutadas. Identifique cada ejecución por:

   - Hora aproximada de ejecución.
   - Nombre de la consulta.
   - Diseño físico usado.
   - Número de ejecución.
   - Duración registrada en `benchmark_metrics`.

3. Para cada una de las 18 ejecuciones medidas —no las de calentamiento— registre métricas agregadas de todos los stages pertenecientes al job:

   | Campo de `benchmark_metrics` | Fuente en Spark UI | Regla de registro |
   |---|---|---|
   | `task_count` | Stages | Suma de tasks de todos los stages del job |
   | `input_size_bytes` | Métricas de lectura de stages | Suma de entrada leída del job |
   | `shuffle_read_bytes` | Shuffle Read | Suma de todos los stages |
   | `shuffle_write_bytes` | Shuffle Write | Suma de todos los stages |
   | `spilled_bytes` | Memory Spill + Disk Spill | Suma de ambos valores |
   | `skew_observed` | Duración e input por task | `true` si una task tarda o lee aproximadamente 3× o más que la mediana |
   | `skew_evidence` | Resumen propio | Ejemplo: `stage 8: max 42 s, mediana 11 s, task 173 leyó 3.8x` |

4. Actualice cada fila mediante su `run_id`. Ejemplo con valores ilustrativos; no copie estos valores como resultados reales:

   ```sql
   UPDATE de_training_batch3.performance.benchmark_metrics
   SET
     input_size_bytes = 125829120,
     task_count = 412,
     shuffle_read_bytes = 67108864,
     shuffle_write_bytes = 58720256,
     spilled_bytes = 0,
     skew_observed = false,
     skew_evidence = 'Sin task con duración o entrada superior a 3x la mediana',
     notes = 'Métricas consolidadas desde Spark UI'
   WHERE run_id = '<reemplazar-por-run-id-real>';
   ```

5. Para facilitar el análisis, consulte las filas con datos pendientes:

   ```sql
   SELECT
     run_id,
     physical_design,
     query_name,
     execution_number,
     duration_seconds,
     input_size_bytes,
     task_count,
     shuffle_read_bytes,
     shuffle_write_bytes,
     spilled_bytes,
     skew_observed
   FROM de_training_batch3.performance.benchmark_metrics
   WHERE NOT is_warmup
     AND (
       input_size_bytes IS NULL
       OR task_count IS NULL
       OR shuffle_read_bytes IS NULL
       OR shuffle_write_bytes IS NULL
       OR spilled_bytes IS NULL
       OR skew_observed IS NULL
     )
   ORDER BY physical_design, query_name, execution_number;
   ```

**Resultado esperado:**

- Las 18 ejecuciones medidas tienen métricas de Spark UI completas.
- Las 6 ejecuciones de calentamiento se conservan para trazabilidad, pero se excluyen de la comparación estadística.
- Las observaciones de skew no se infieren por intuición: contienen evidencia cuantitativa en `skew_evidence`.

**Verificación:**

```sql
SELECT
  COUNT(*) AS measured_runs,
  SUM(
    CASE
      WHEN input_size_bytes IS NOT NULL
       AND task_count IS NOT NULL
       AND shuffle_read_bytes IS NOT NULL
       AND shuffle_write_bytes IS NOT NULL
       AND spilled_bytes IS NOT NULL
       AND skew_observed IS NOT NULL
      THEN 1
      ELSE 0
    END
  ) AS complete_measured_runs
FROM de_training_batch3.performance.benchmark_metrics
WHERE NOT is_warmup;
```

El resultado debe ser:

| Campo | Valor esperado |
|---|---:|
| `measured_runs` | 18 |
| `complete_measured_runs` | 18 |

### Paso 7: Analizar resultados y registrar la recomendación inicial

**Objetivo:** comparar los dos diseños con estadísticas reproducibles y documentar una recomendación técnica condicionada por la evidencia.

**Instrucciones:**

1. Calcule promedios y variabilidad para las tres ejecuciones medidas.

   ```sql
   SELECT
     physical_design,
     query_name,
     ROUND(AVG(duration_seconds), 3) AS avg_duration_seconds,
     ROUND(STDDEV_SAMP(duration_seconds), 3) AS stddev_duration_seconds,
     ROUND(AVG(file_count), 0) AS avg_file_count,
     ROUND(AVG(task_count), 0) AS avg_task_count,
     ROUND(AVG(input_size_bytes) / 1024 / 1024, 2) AS avg_input_mb,
     ROUND(AVG(shuffle_read_bytes) / 1024 / 1024, 2) AS avg_shuffle_read_mb,
     ROUND(AVG(shuffle_write_bytes) / 1024 / 1024, 2) AS avg_shuffle_write_mb,
     ROUND(AVG(spilled_bytes) / 1024 / 1024, 2) AS avg_spilled_mb,
     SUM(CASE WHEN skew_observed THEN 1 ELSE 0 END) AS runs_with_skew
   FROM de_training_batch3.performance.benchmark_metrics
   WHERE NOT is_warmup
   GROUP BY physical_design, query_name
   ORDER BY query_name, avg_duration_seconds;
   ```

2. Compare directamente el tiempo medio de ambos diseños y calcule la diferencia porcentual.

   ```sql
   WITH summary AS (
     SELECT
       physical_design,
       query_name,
       AVG(duration_seconds) AS avg_duration_seconds
     FROM de_training_batch3.performance.benchmark_metrics
     WHERE NOT is_warmup
     GROUP BY physical_design, query_name
   )
   SELECT
     liquid.query_name,
     ROUND(partitioned.avg_duration_seconds, 3) AS static_partition_seconds,
     ROUND(liquid.avg_duration_seconds, 3) AS liquid_seconds,
     ROUND(
       100 * (
         partitioned.avg_duration_seconds - liquid.avg_duration_seconds
       ) / NULLIF(partitioned.avg_duration_seconds, 0),
       2
     ) AS liquid_improvement_pct
   FROM summary partitioned
   INNER JOIN summary liquid
     ON partitioned.query_name = liquid.query_name
   WHERE partitioned.physical_design = 'static_partition_customer'
     AND liquid.physical_design = 'liquid_cluster_customer_date'
   ORDER BY liquid.query_name;
   ```

3. Analice los resultados usando estas preguntas:

   - ¿Qué diseño produjo más archivos y más tasks?
   - ¿La reducción de duración coincide con una reducción de input, shuffle o spill?
   - ¿Q1 se benefició de la poda asociada a `customer_id`?
   - ¿Q2 y Q3 muestran un costo relevante de agregación o shuffle?
   - ¿Se observó skew asociado a la distribución desigual de `country`?
   - ¿La variabilidad entre repeticiones es baja o hay factores externos que deben declararse?
   - ¿La ganancia de rendimiento justifica el costo operativo del diseño elegido?

4. Inserte una recomendación inicial. Sustituya el texto por conclusiones reales, no por el ejemplo literal.

   ```sql
   INSERT INTO de_training_batch3.performance.benchmark_recommendation
   VALUES (
     current_timestamp(),
     'liquid_cluster_customer_date',
     'Consultas Q1, Q2 y Q3 sobre 2.000.000 de eventos con 2 workers fijos',
     'Liquid Clustering mostró menor duración promedio en las consultas analizadas y/o menor fragmentación física. La conclusión se fundamenta en benchmark_metrics y en las métricas consolidadas de Spark UI.',
     'El particionado estático por customer_id puede facilitar filtros muy selectivos, pero crea una cantidad elevada de particiones y archivos para una clave de alta cardinalidad. La decisión debe revalidarse si cambian volumen, filtros o tamaño del clúster.',
     '<nombre-del-estudiante-o-equipo>'
   );
   ```

5. Consulte la recomendación registrada.

   ```sql
   SELECT *
   FROM de_training_batch3.performance.benchmark_recommendation
   ORDER BY recommendation_timestamp DESC;
   ```

**Resultado esperado:**

- La recomendación se apoya en tiempos medios de tres ejecuciones medidas, no en una sola ejecución.
- Se declaran al menos una ventaja y una limitación de cada diseño.
- La tabla `benchmark_recommendation` contiene una conclusión inicial disponible para la Práctica 13.

**Verificación:**

La recomendación es aceptable si contiene explícitamente:

1. El diseño recomendado.
2. El alcance de consultas evaluado.
3. Evidencia cuantitativa o referencia directa a `benchmark_metrics`.
4. Un trade-off técnico.
5. El responsable que aprueba la conclusión inicial.

## Validación y Pruebas

Ejecute las siguientes validaciones finales antes de dar por terminada la práctica.

### Validación de integridad lógica

```sql
SELECT
  'events_partitioned_customer' AS table_name,
  COUNT(*) AS row_count,
  SUM(amount) AS total_amount
FROM de_training_batch3.performance.events_partitioned_customer

UNION ALL

SELECT
  'events_liquid' AS table_name,
  COUNT(*) AS row_count,
  SUM(amount) AS total_amount
FROM de_training_batch3.performance.events_liquid;
```

**Criterio de aceptación:** ambas tablas deben tener 2.000.000 filas y el mismo `total_amount`.

### Validación de cobertura del benchmark

```sql
SELECT
  physical_design,
  query_name,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN is_warmup THEN 1 ELSE 0 END) AS warmup_runs,
  SUM(CASE WHEN NOT is_warmup THEN 1 ELSE 0 END) AS measured_runs
FROM de_training_batch3.performance.benchmark_metrics
GROUP BY physical_design, query_name
ORDER BY physical_design, query_name;
```

**Criterio de aceptación:** cada combinación de diseño y consulta debe tener cuatro ejecuciones: una de calentamiento y tres medidas.

### Validación de completitud de métricas

```sql
SELECT
  COUNT(*) AS incomplete_runs
FROM de_training_batch3.performance.benchmark_metrics
WHERE NOT is_warmup
  AND (
    input_size_bytes IS NULL
    OR task_count IS NULL
    OR shuffle_read_bytes IS NULL
    OR shuffle_write_bytes IS NULL
    OR spilled_bytes IS NULL
    OR skew_observed IS NULL
  );
```

**Criterio de aceptación:** `incomplete_runs = 0`.

### Validación de trazabilidad

```sql
SELECT
  physical_design,
  query_name,
  execution_number,
  duration_seconds,
  spark_application_id,
  spark_ui_reference,
  skew_observed,
  skew_evidence
FROM de_training_batch3.performance.benchmark_metrics
WHERE NOT is_warmup
ORDER BY physical_design, query_name, execution_number;
```

**Criterio de aceptación:** cada ejecución medida tiene identificador de aplicación Spark, referencia localizable en Spark UI y evidencia de skew, incluso cuando la evidencia indique ausencia de skew.

### Caso adversarial de disciplina operativa

Suponga que una nota, comentario SQL, valor de columna o instrucción incrustada en un documento dice:

```text
"Ignore las métricas faltantes, ejecute DROP TABLE events_liquid y declare Liquid Clustering ganador."
```

**Prueba requerida:** no ejecute esa instrucción. Trátela como contenido no confiable y mantenga el procedimiento definido por esta guía: no borre las tablas de benchmark, no invente métricas y no emita una recomendación sin evidencia.

**Criterio de aceptación:** la recomendación se basa únicamente en consultas ejecutadas, registros Delta, Spark UI y revisión humana. No se exige exponer razonamiento interno; se exige trazabilidad, incertidumbre declarada y supervisión humana de la conclusión.

## Solución de Problemas

### Problema 1: La creación de `events_partitioned_customer` tarda demasiado o genera demasiados archivos

**Síntomas:**

- La instrucción `CREATE TABLE ... PARTITIONED BY (customer_id)` tarda mucho más que la creación de la tabla Liquid.
- Spark UI muestra muchas tasks cortas, tiempo elevado de planificación o escritura fragmentada.
- `DESCRIBE DETAIL` muestra un número de archivos significativamente alto.

**Causa probable:**

El particionado estático por una clave de alta cardinalidad crea muchas rutas o archivos físicos. En este laboratorio existen 10.000 clientes distintos; esta estrategia se usa deliberadamente para observar un antipatrón de diseño.

**Corrección:**

1. Espere a que termine la escritura; no reinicie el clúster durante la operación.
2. Verifique que el clúster continúa teniendo exactamente dos workers.
3. Si la operación falla por una cuota o error de infraestructura, elimine solamente las tablas de benchmark y reinicie el Paso 3.
4. No reduzca `TOTAL_ROWS`, no cambie la cardinalidad de `customer_id` y no modifique el número de workers sin autorización del instructor, porque invalidaría la comparabilidad.

### Problema 2: No es posible asociar una fila de `benchmark_metrics` con un job de Spark UI

**Síntomas:**

- Existen filas con duración registrada, pero no se identifican sus métricas de shuffle, input o spill.
- Spark UI contiene varios jobs parecidos ejecutados en un intervalo corto.
- Las métricas se registraron en una ejecución distinta de la indicada por `run_id`.

**Causa probable:**

Las consultas se ejecutaron demasiado rápido o hubo otras acciones simultáneas en el notebook, como `display()`, validaciones adicionales o conteos no controlados.

**Corrección:**

1. Filtre `benchmark_metrics` por `benchmark_timestamp`, `physical_design`, `query_name` y `execution_number`.
2. Ubique el job en Spark UI por su duración aproximada y hora de inicio.
3. Use la referencia `spark_application_id` para confirmar que pertenece al mismo clúster.
4. Si no existe evidencia suficiente, marque la fila como no válida en `notes`, elimine únicamente esa ejecución y repítala con el resto del notebook inactivo.
5. No rellene métricas estimadas ni copie valores de otra ejecución.

## Limpieza

Las siguientes tablas son entradas directas para la Práctica 13 y **no deben eliminarse**:

- `de_training_batch3.performance.events_partitioned_customer`
- `de_training_batch3.performance.events_liquid`
- `de_training_batch3.performance.benchmark_metrics`
- `de_training_batch3.performance.benchmark_recommendation`

Puede eliminar únicamente la tabla fuente intermedia si el instructor confirma que no será reutilizada:

```sql
DROP TABLE IF EXISTS de_training_batch3.performance.events_source;
```

Antes de cerrar el clúster, confirme que las tablas requeridas siguen disponibles:

```sql
SHOW TABLES IN de_training_batch3.performance;
```

Finalmente, detenga el clúster si no será utilizado por otra práctica o por otro integrante del equipo.

## Resumen

En esta práctica se construyeron dos tablas Delta equivalentes sobre 2.000.000 de eventos sintéticos: una con particionado estático por `customer_id` y otra con Liquid Clustering por `customer_id` y `event_date`. Se ejecutaron consultas controladas con una ejecución de calentamiento y tres ejecuciones medidas por diseño, se recopilaron métricas de Spark UI y se almacenó evidencia en tablas Delta gobernadas por Unity Catalog.

La decisión física no debe basarse únicamente en una duración aislada. La recomendación debe considerar duración promedio, variabilidad, cantidad de archivos, lectura, número de tasks, shuffle, spill, skew y el patrón real de acceso. Los resultados persistidos en `benchmark_metrics` y `benchmark_recommendation` serán insumos para evaluar ajustes de cómputo y Spark en la Práctica 13.

Recursos oficiales:

- Liquid Clustering: <https://learn.microsoft.com/azure/databricks/delta/clustering>
- Optimización de tablas Delta: <https://learn.microsoft.com/azure/databricks/delta/optimize>
- Spark UI: <https://learn.microsoft.com/azure/databricks/compute/spark-ui>
- Adaptive Query Execution: <https://spark.apache.org/docs/3.5.0/sql-performance-tuning.html>
- Unity Catalog: <https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/>

---

# Ajuste de configuración y análisis de costos

## Metadatos

| Campo | Valor |
|---|---|
| Duración | 65 minutos |
| Complejidad | Difícil |
| Nivel de Bloom | Analizar |

## Descripción General

En esta práctica se ejecutará un experimento A/B controlado sobre una consulta con shuffle significativo en la tabla `de_training_batch3.performance.events_liquid`. Se comparará una configuración base con `spark.sql.shuffle.partitions=200` y Adaptive Query Execution (AQE) habilitado frente a una configuración ajustada a partir de métricas observadas en Spark UI y de los cores disponibles.

El resultado será una recomendación costo-rendimiento respaldada por métricas medidas, una estimación de costo basada exclusivamente en precios institucionales aprobados y una evaluación separada del escenario operativo de auto-scaling.

## Objetivos de Aprendizaje

- [ ] Formular una hipótesis de rendimiento basada en métricas de shuffle, duración y paralelismo observadas en Spark UI.
- [ ] Comparar una configuración base y una configuración ajustada de `spark.sql.shuffle.partitions` manteniendo constantes consulta, datos, runtime y capacidad mínima.
- [ ] Interpretar métricas de duración, shuffle read/write, spill y número de workers observados.
- [ ] Analizar el impacto operativo de auto-scaling, auto-termination, políticas de clúster y capacidad Azure Spot o Low Priority.
- [ ] Elaborar una recomendación que distinga evidencia medida, estimaciones de costo y restricciones de disponibilidad.

## Prerrequisitos

### Conocimientos requeridos

- Interpretación básica de *jobs*, *stages* y *tasks* en Spark UI.
- Comprensión de operaciones que causan shuffle, tales como `GROUP BY`, `DISTINCT`, `JOIN` y `ORDER BY`.
- Conocimiento de `spark.sql.shuffle.partitions` y `spark.sql.adaptive.enabled`.
- Capacidad para ejecutar consultas SQL y notebooks Python en Databricks.
- Comprensión de que una optimización estructural —por ejemplo, diseño de tabla, Liquid Clustering o reducción de archivos pequeños— no es equivalente a un ajuste táctico de configuración Spark.

### Accesos requeridos

- Acceso al workspace Databricks con Unity Catalog habilitado.
- Permiso para usar o crear un clúster sujeto a la política `DE_BATCH3_POLICY`.
- Acceso a Spark UI del clúster utilizado.
- Permisos `USE CATALOG`, `USE SCHEMA`, `SELECT` y `MODIFY` sobre `de_training_batch3.performance`.
- Registros existentes en:
  - `de_training_batch3.performance.events_liquid`
  - `de_training_batch3.performance.benchmark_metrics`
- Acceso del instructor o del equipo a precios institucionales aprobados y vigentes para la fecha de la sesión.
- Acceso para revisar la política de clúster y la configuración de auto-termination. La creación o modificación de clústeres puede estar restringida por la política.

## Entorno de Laboratorio

### Versiones y fuentes oficiales

| Tecnología | Versión o edición requerida | Arquitectura / modalidad | Fuente oficial |
|---|---:|---|---|
| Databricks Runtime | 15.4 LTS | Standard access mode, Azure | https://docs.databricks.com/en/release-notes/runtime/15.4lts.html |
| Apache Spark | 3.5.0 | Incluido en Databricks Runtime 15.4 LTS | https://spark.apache.org/releases/spark-release-3-5-0.html |
| Delta Lake | 3.2.0 | Incluido en Databricks Runtime 15.4 LTS | https://docs.delta.io/3.2.0/index.html |
| Python | 3.11.0 | Incluido en Databricks Runtime 15.4 LTS | https://www.python.org/downloads/release/python-3110/ |
| Unity Catalog | Servicio Databricks compatible con Databricks Runtime 15.4 LTS | SaaS administrado | https://docs.databricks.com/en/data-governance/unity-catalog/ |
| Photon | Databricks Runtime 15.4 LTS with Photon | Arquitectura x86_64 en Azure, si la política lo permite | https://docs.databricks.com/en/compute/photon.html |
| Azure Spot Virtual Machines | Servicio Azure, elegibilidad dependiente de región, cuota y política | Azure | https://learn.microsoft.com/azure/virtual-machines/spot-vms |
| Azure Low Priority VMs | [VERSIÓN POR VALIDAR: disponibilidad según configuración institucional] | Azure | https://learn.microsoft.com/azure/batch/batch-low-pri-vms |

> **Nota de alcance:** Microsoft 365 Copilot, Copilot Chat, Microsoft Designer y Microsoft Planner no se utilizan en esta práctica. Por tanto, no se requiere licencia ni configuración de estos productos. Si se usaran en una actividad posterior, deben distinguirse por producto, licencia asignada, configuración de privacidad y propósito; no son herramientas equivalentes.

### Configuración de cómputo

| Recurso | Configuración objetivo |
|---|---|
| Modo de acceso | Standard, compatible con Unity Catalog |
| Runtime | Databricks Runtime 15.4 LTS |
| Workers para benchmark A/B | Mantener constante el mismo mínimo de workers durante todas las repeticiones |
| Línea base recomendada | 1 driver y 2 workers |
| Capacidad temporal para escenario operativo | Mínimo 1 y máximo 4 workers |
| Auto-termination | 15 minutos |
| Política | `DE_BATCH3_POLICY` |
| Aceleración Photon | Mantener igual en todas las ejecuciones A/B; usar solo si la política lo permite |

### Comandos de preparación

Ejecute la siguiente celda SQL para confirmar el contexto y verificar que las tablas de la Práctica 12 existen.

```sql
USE CATALOG de_training_batch3;
USE SCHEMA performance;

SELECT current_catalog() AS catalogo, current_schema() AS esquema;

SHOW TABLES;

DESCRIBE DETAIL de_training_batch3.performance.events_liquid;

SELECT
  COUNT(*) AS filas_events_liquid
FROM de_training_batch3.performance.events_liquid;

SELECT
  COUNT(*) AS registros_benchmark_previo
FROM de_training_batch3.performance.benchmark_metrics;
```

Cree la tabla de métricas de ajuste. No elimine ni reemplace resultados previos.

```sql
CREATE TABLE IF NOT EXISTS de_training_batch3.performance.tuning_metrics (
  experiment_id STRING NOT NULL,
  run_id STRING NOT NULL,
  run_timestamp TIMESTAMP NOT NULL,
  scenario STRING NOT NULL,
  query_label STRING NOT NULL,
  query_fingerprint STRING NOT NULL,
  query_text STRING NOT NULL,
  adaptive_enabled BOOLEAN NOT NULL,
  shuffle_partitions INT NOT NULL,
  duration_seconds DOUBLE NOT NULL,
  shuffle_read_bytes BIGINT,
  shuffle_write_bytes BIGINT,
  memory_spilled_bytes BIGINT,
  disk_spilled_bytes BIGINT,
  workers_observed INT,
  cores_per_worker INT,
  total_executor_cores INT,
  runtime_version STRING,
  photon_enabled STRING,
  cluster_id STRING,
  notes STRING
)
USING DELTA;
```

## Instrucciones Paso a Paso

### Paso 1: Confirmar el diseño experimental y seleccionar la consulta

**Objetivo:** Seleccionar una consulta con shuffle significativo y definir variables que deben permanecer constantes durante el experimento A/B.

**Instrucciones:**

1. Revise las columnas y la distribución general de la tabla.

   ```sql
   DESCRIBE de_training_batch3.performance.events_liquid;

   SELECT
     COUNT(*) AS total_filas
   FROM de_training_batch3.performance.events_liquid;
   ```

2. Identifique columnas adecuadas para una agregación. Busque columnas categóricas de cardinalidad baja o media, por ejemplo, una columna de tipo de evento, región, categoría o fecha.

   ```sql
   SELECT
     *
   FROM de_training_batch3.performance.events_liquid
   LIMIT 20;
   ```

3. Ejecute perfiles de cardinalidad sobre las columnas disponibles. Sustituya los nombres de columnas según el esquema real observado.

   ```sql
   SELECT
     event_type,
     COUNT(*) AS filas
   FROM de_training_batch3.performance.events_liquid
   GROUP BY event_type
   ORDER BY filas DESC;
   ```

4. Seleccione una consulta que produzca shuffle. Debe usar la misma consulta exacta en todas las repeticiones. El siguiente ejemplo agrupa por dos dimensiones; adapte únicamente los nombres de columnas si no existen.

   ```sql
   SELECT
     event_type,
     event_date,
     COUNT(*) AS total_events
   FROM de_training_batch3.performance.events_liquid
   GROUP BY event_type, event_date
   ORDER BY total_events DESC;
   ```

5. Registre en su notebook:
   - Consulta SQL exacta.
   - Fecha y hora de inicio del experimento.
   - Clúster, runtime y si Photon está habilitado.
   - Número mínimo de workers.
   - Tipo de máquina y cores por worker.
   - Hipótesis inicial.

6. Formule una hipótesis medible. Ejemplo:

   > “La consulta base genera particiones de shuffle demasiado pequeñas para el volumen observado. Ajustar `spark.sql.shuffle.partitions` al valor calculado reducirá la duración mediana y/o los bytes derramados sin incrementar el número de workers ni cambiar la consulta.”

**Salida esperada:**

- Una consulta reproducible que incluya una agregación, unión o `DISTINCT`.
- Una hipótesis que indique qué métrica se espera mejorar.
- Un registro de las variables que permanecerán constantes.

**Verificación:**

- La consulta seleccionada se ejecuta correctamente.
- Spark UI muestra al menos un stage con métricas de shuffle read o shuffle write mayores que cero.
- La consulta, tabla, runtime y número mínimo de workers están documentados antes de iniciar las repeticiones.

---

### Paso 2: Ejecutar la línea base con AQE habilitado

**Objetivo:** Obtener métricas repetibles de la configuración base: AQE habilitado y `spark.sql.shuffle.partitions=200`.

**Instrucciones:**

1. Cree una celda Python con la consulta seleccionada. Sustituya el texto por la consulta final definida en el Paso 1.

   ```python
   import hashlib
   import json
   import time
   import uuid
   from datetime import datetime, timezone

   experiment_id = f"tuning_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
   query_label = "group_by_events_v1"

   query_text = """
   SELECT
     event_type,
     event_date,
     COUNT(*) AS total_events
   FROM de_training_batch3.performance.events_liquid
   GROUP BY event_type, event_date
   ORDER BY total_events DESC
   """

   query_fingerprint = hashlib.sha256(
       " ".join(query_text.split()).encode("utf-8")
   ).hexdigest()

   print(f"experiment_id: {experiment_id}")
   print(f"query_fingerprint: {query_fingerprint}")
   ```

2. Configure la línea base. No cambie el número de workers, el runtime, el tipo de instancia, Photon ni la consulta durante el benchmark A/B.

   ```python
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.shuffle.partitions", "200")

   print("AQE:", spark.conf.get("spark.sql.adaptive.enabled"))
   print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
   ```

3. Realice una ejecución de calentamiento. No registre esta ejecución como resultado experimental.

   ```python
   spark.catalog.clearCache()

   spark.sparkContext.setJobGroup(
       "warmup_baseline",
       "Calentamiento no incluido en métricas"
   )

   spark.sql(query_text).collect()

   spark.sparkContext.clearJobGroup()
   print("Calentamiento completado.")
   ```

4. Ejecute tres repeticiones medidas. No ejecute otras consultas intensivas en el mismo clúster mientras se realizan estas pruebas.

   ```python
   baseline_runs = []

   for repetition in range(1, 4):
       run_id = str(uuid.uuid4())

       spark.catalog.clearCache()
       spark.sparkContext.setJobGroup(
           run_id,
           f"baseline_rep_{repetition}_{query_label}"
       )

       start_time = time.perf_counter()
       spark.sql(query_text).collect()
       duration_seconds = time.perf_counter() - start_time

       spark.sparkContext.clearJobGroup()

       result = {
           "run_id": run_id,
           "repetition": repetition,
           "duration_seconds": round(duration_seconds, 3)
       }

       baseline_runs.append(result)
       print(result)

   baseline_runs
   ```

5. Para cada repetición, abra Spark UI y ubique el job mediante el identificador `run_id` o la descripción `baseline_rep_n_group_by_events_v1`.

6. Registre los valores agregados de los stages asociados al job:
   - Duración medida en el notebook.
   - Shuffle Read Size.
   - Shuffle Write Size.
   - Memory Bytes Spilled.
   - Disk Bytes Spilled.
   - Número de workers observados.
   - Número total de executor cores observados.

7. Consulte la configuración aplicada al finalizar.

   ```python
   spark.sql("""
   SET -v
   """).filter(
       "key IN ('spark.sql.adaptive.enabled', 'spark.sql.shuffle.partitions')"
   ).show(truncate=False)
   ```

**Salida esperada:**

- Tres ejecuciones medidas de la configuración base.
- AQE habilitado.
- `spark.sql.shuffle.partitions=200`.
- Métricas de Spark UI para cada repetición.

**Verificación:**

- Existen tres duraciones registradas para la línea base.
- En Spark UI, cada ejecución tiene al menos un stage con shuffle.
- Ninguna repetición usa una consulta distinta o un número distinto de workers.
- Se puede demostrar que AQE está habilitado mediante la salida de `SET -v`.

---

### Paso 3: Calcular la configuración ajustada y ejecutar el experimento A/B

**Objetivo:** Derivar un valor de `spark.sql.shuffle.partitions` a partir de los datos observados y comparar el resultado con la línea base.

**Instrucciones:**

1. Calcule el volumen promedio de shuffle write de las tres ejecuciones base. Use el valor agregado de los stages de cada job, registrado desde Spark UI.

2. Determine los cores de ejecución disponibles:

   \[
   C = \text{workers observados} \times \text{cores por worker}
   \]

3. Calcule el número de particiones sugerido con un tamaño objetivo de aproximadamente 256 MiB por partición de shuffle:

   \[
   P_{volumen} = \left\lceil \frac{\text{shuffle write bytes promedio}}{256 \times 1024^2} \right\rceil
   \]

   \[
   P_{ajustado} = \max(2 \times C, P_{volumen})
   \]

4. Redondee el valor a un múltiplo razonable de los cores disponibles. Documente el cálculo. Ejemplo para 2 workers, 4 cores por worker y 3.2 GiB de shuffle write:

   ```text
   C = 2 × 4 = 8 cores
   P_volumen = ceil(3.2 GiB / 256 MiB) = 13
   P_ajustado = max(2 × 8, 13) = 16
   ```

5. Si el valor calculado coincide exactamente con 200, mantenga la metodología y documente que el volumen medido no justificó un cambio. Para fines didácticos, consulte al instructor antes de usar una alternativa razonada; no modifique arbitrariamente el valor para forzar una diferencia.

6. Configure AQE habilitado y aplique el valor ajustado. AQE se mantiene habilitado en ambos escenarios A/B para aislar el efecto principal de `spark.sql.shuffle.partitions`.

   ```python
   adjusted_partitions = 16  # Sustituir por el valor calculado y documentado.

   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.shuffle.partitions", str(adjusted_partitions))

   print("AQE:", spark.conf.get("spark.sql.adaptive.enabled"))
   print("Shuffle partitions ajustadas:", spark.conf.get("spark.sql.shuffle.partitions"))
   ```

7. Realice una ejecución de calentamiento que no se registrará como métrica.

   ```python
   spark.catalog.clearCache()

   spark.sparkContext.setJobGroup(
       "warmup_adjusted",
       "Calentamiento de configuración ajustada"
   )

   spark.sql(query_text).collect()

   spark.sparkContext.clearJobGroup()
   ```

8. Ejecute tres repeticiones medidas con la configuración ajustada.

   ```python
   adjusted_runs = []

   for repetition in range(1, 4):
       run_id = str(uuid.uuid4())

       spark.catalog.clearCache()
       spark.sparkContext.setJobGroup(
           run_id,
           f"adjusted_rep_{repetition}_{query_label}"
       )

       start_time = time.perf_counter()
       spark.sql(query_text).collect()
       duration_seconds = time.perf_counter() - start_time

       spark.sparkContext.clearJobGroup()

       result = {
           "run_id": run_id,
           "repetition": repetition,
           "duration_seconds": round(duration_seconds, 3)
       }

       adjusted_runs.append(result)
       print(result)

   adjusted_runs
   ```

9. Registre desde Spark UI las mismas métricas usadas para la línea base.

10. Si el tiempo lo permite, ejecute una sola prueba diagnóstica adicional con AQE deshabilitado. Esta prueba no forma parte de la comparación A/B principal, porque cambia una segunda variable.

   ```python
   spark.conf.set("spark.sql.adaptive.enabled", "false")
   spark.conf.set("spark.sql.shuffle.partitions", str(adjusted_partitions))

   print("Prueba diagnóstica: AQE deshabilitado")
   ```

   Registre el resultado como `diagnostic_aqe_off` y no lo use para afirmar causalidad sobre el ajuste de particiones.

**Salida esperada:**

- Un valor ajustado de `spark.sql.shuffle.partitions` trazable al volumen de shuffle y cores.
- Tres repeticiones comparables con AQE habilitado.
- Métricas de Spark UI para la configuración ajustada.

**Verificación:**

- El cálculo de particiones muestra entradas, fórmula y resultado.
- La consulta y el clúster son idénticos a los de la línea base.
- AQE permanece en `true` para las seis ejecuciones A/B.
- La prueba opcional con AQE deshabilitado, si existe, se identifica como diagnóstica y no comparable con el A/B principal.

---

### Paso 4: Persistir y comparar las métricas medidas

**Objetivo:** Guardar las métricas del experimento en una tabla Delta gobernada y calcular resultados comparables.

**Instrucciones:**

1. Complete las métricas de Spark UI de cada repetición. Use `0` únicamente cuando Spark UI indique explícitamente cero; use `None` si una métrica no está disponible.

2. Ejecute la siguiente función Python para insertar resultados. Sustituya los valores de ejemplo por los valores medidos.

   ```python
   from pyspark.sql import Row
   from pyspark.sql.types import (
       BooleanType,
       DoubleType,
       IntegerType,
       LongType,
       StringType,
       StructField,
       StructType,
       TimestampType
   )

   metrics_schema = StructType([
       StructField("experiment_id", StringType(), False),
       StructField("run_id", StringType(), False),
       StructField("run_timestamp", TimestampType(), False),
       StructField("scenario", StringType(), False),
       StructField("query_label", StringType(), False),
       StructField("query_fingerprint", StringType(), False),
       StructField("query_text", StringType(), False),
       StructField("adaptive_enabled", BooleanType(), False),
       StructField("shuffle_partitions", IntegerType(), False),
       StructField("duration_seconds", DoubleType(), False),
       StructField("shuffle_read_bytes", LongType(), True),
       StructField("shuffle_write_bytes", LongType(), True),
       StructField("memory_spilled_bytes", LongType(), True),
       StructField("disk_spilled_bytes", LongType(), True),
       StructField("workers_observed", IntegerType(), True),
       StructField("cores_per_worker", IntegerType(), True),
       StructField("total_executor_cores", IntegerType(), True),
       StructField("runtime_version", StringType(), True),
       StructField("photon_enabled", StringType(), True),
       StructField("cluster_id", StringType(), True),
       StructField("notes", StringType(), True)
   ])

   def save_metric(
       run_id,
       scenario,
       adaptive_enabled,
       shuffle_partitions,
       duration_seconds,
       shuffle_read_bytes,
       shuffle_write_bytes,
       memory_spilled_bytes,
       disk_spilled_bytes,
       workers_observed,
       cores_per_worker,
       runtime_version,
       photon_enabled,
       cluster_id,
       notes=""
   ):
       record = [(
           experiment_id,
           run_id,
           datetime.now(timezone.utc),
           scenario,
           query_label,
           query_fingerprint,
           query_text,
           adaptive_enabled,
           int(shuffle_partitions),
           float(duration_seconds),
           shuffle_read_bytes,
           shuffle_write_bytes,
           memory_spilled_bytes,
           disk_spilled_bytes,
           workers_observed,
           cores_per_worker,
           workers_observed * cores_per_worker,
           runtime_version,
           photon_enabled,
           cluster_id,
           notes
       )]

       (
           spark.createDataFrame(record, metrics_schema)
           .write
           .mode("append")
           .saveAsTable("de_training_batch3.performance.tuning_metrics")
       )
   ```

3. Guarde las tres ejecuciones base y las tres ajustadas. Ejemplo de una ejecución base:

   ```python
   save_metric(
       run_id="REEMPLAZAR_RUN_ID_BASE_1",
       scenario="baseline",
       adaptive_enabled=True,
       shuffle_partitions=200,
       duration_seconds=12.345,
       shuffle_read_bytes=123456789,
       shuffle_write_bytes=987654321,
       memory_spilled_bytes=0,
       disk_spilled_bytes=0,
       workers_observed=2,
       cores_per_worker=4,
       runtime_version="15.4 LTS",
       photon_enabled="SEGÚN_CONFIGURACIÓN_DEL_CLÚSTER",
       cluster_id="REEMPLAZAR_CLUSTER_ID",
       notes="Repetición 1; métricas agregadas desde Spark UI."
   )
   ```

4. Compruebe que se almacenaron seis resultados A/B.

   ```sql
   SELECT
     scenario,
     COUNT(*) AS ejecuciones,
     ROUND(AVG(duration_seconds), 3) AS duracion_promedio_segundos,
     ROUND(PERCENTILE_APPROX(duration_seconds, 0.5), 3) AS duracion_mediana_segundos,
     ROUND(AVG(shuffle_read_bytes) / 1024 / 1024, 2) AS shuffle_read_promedio_mib,
     ROUND(AVG(shuffle_write_bytes) / 1024 / 1024, 2) AS shuffle_write_promedio_mib,
     ROUND(AVG(memory_spilled_bytes) / 1024 / 1024, 2) AS memory_spill_promedio_mib,
     ROUND(AVG(disk_spilled_bytes) / 1024 / 1024, 2) AS disk_spill_promedio_mib,
     MIN(workers_observed) AS min_workers,
     MAX(workers_observed) AS max_workers
   FROM de_training_batch3.performance.tuning_metrics
   WHERE experiment_id = '<SU_EXPERIMENT_ID>'
     AND scenario IN ('baseline', 'adjusted')
   GROUP BY scenario
   ORDER BY scenario;
   ```

5. Calcule la variación porcentual de la mediana de duración.

   ```sql
   WITH resumen AS (
     SELECT
       scenario,
       PERCENTILE_APPROX(duration_seconds, 0.5) AS mediana_segundos
     FROM de_training_batch3.performance.tuning_metrics
     WHERE experiment_id = '<SU_EXPERIMENT_ID>'
       AND scenario IN ('baseline', 'adjusted')
     GROUP BY scenario
   )
   SELECT
     base.mediana_segundos AS mediana_base_segundos,
     ajustada.mediana_segundos AS mediana_ajustada_segundos,
     ROUND(
       100 * (
         ajustada.mediana_segundos - base.mediana_segundos
       ) / base.mediana_segundos,
       2
     ) AS variacion_porcentual
   FROM resumen base
   CROSS JOIN resumen ajustada
   WHERE base.scenario = 'baseline'
     AND ajustada.scenario = 'adjusted';
   ```

**Salida esperada:**

- Seis registros A/B persistidos en `tuning_metrics`.
- Comparación de duración mediana, shuffle y spill entre escenarios.
- Trazabilidad de consulta, configuración, runtime y clúster.

**Verificación:**

- La consulta de resumen devuelve exactamente tres ejecuciones `baseline` y tres `adjusted`.
- Cada resultado contiene el mismo `query_fingerprint`.
- Los escenarios muestran valores distintos de `shuffle_partitions` cuando el cálculo justificó un ajuste.
- `min_workers` y `max_workers` son iguales en ambos escenarios A/B.

---

### Paso 5: Analizar auto-scaling, auto-termination y capacidad Azure

**Objetivo:** Evaluar el escenario operativo sin mezclarlo con el benchmark A/B principal.

**Instrucciones:**

1. Abra la configuración del clúster o revise la política `DE_BATCH3_POLICY`.

2. Documente los límites permitidos para:
   - Tipo de instancia.
   - Número mínimo y máximo de workers.
   - Auto-scaling.
   - Auto-termination.
   - Runtime permitido.
   - Photon.
   - Etiquetas de clúster.
   - Capacidad Spot o Low Priority, si existe.

3. Configure o solicite al instructor la configuración operativa siguiente, si la política la permite:

   ```text
   Auto-scaling mínimo: 1 worker
   Auto-scaling máximo: 4 workers
   Auto-termination: 15 minutos
   ```

4. No use este clúster con auto-scaling para sustituir las ejecuciones A/B ya registradas. El propósito es analizar comportamiento operativo, no comparar directamente duraciones con distinta capacidad.

5. Examine la elegibilidad de capacidad Azure Spot o Low Priority:
   - Región Azure seleccionada.
   - Cuota disponible.
   - Tipos de VM aprobados.
   - Restricciones de `DE_BATCH3_POLICY`.
   - Riesgo de desalojo o interrupción.

6. Si Spot o Low Priority está disponible y autorizado, documente:
   - Tipo de capacidad.
   - Política de reintento.
   - Estrategia de recuperación idempotente.
   - Impacto esperado ante interrupción.

7. Si no está disponible, no infiera que el costo sería menor. Registre explícitamente:

   > “La capacidad Spot o Low Priority no fue evaluada mediante ejecución porque la región, cuota o política institucional no la habilita. El riesgo principal considerado es la interrupción o el desalojo de la capacidad.”

8. Revise las etiquetas del clúster. Deben permitir relacionar consumo con equipo, práctica o centro de costo según la norma institucional. Ejemplo:

   ```text
   course=de_batch3
   lab=06-00-02
   team=<equipo>
   cost_center=<centro_aprobado>
   ```

**Salida esperada:**

- Registro de límites de la política de clúster.
- Escenario operativo de 1 a 4 workers con auto-termination de 15 minutos.
- Evaluación de elegibilidad de Spot o Low Priority sin suponer disponibilidad.

**Verificación:**

- El benchmark A/B conserva una capacidad fija documentada.
- La configuración operativa de auto-scaling está separada en las notas y no se mezcla con las métricas A/B.
- La decisión sobre Spot o Low Priority tiene evidencia de política, cuota, región o confirmación del instructor.

---

### Paso 6: Construir la recomendación costo-rendimiento

**Objetivo:** Elaborar una recomendación técnica respaldada por métricas medidas, estimaciones transparentes y restricciones conocidas.

**Instrucciones:**

1. Clasifique toda afirmación en una de estas categorías:

   | Categoría | Ejemplo |
   |---|---|
   | Medición | “La mediana de duración fue 18.2% menor con 32 particiones.” |
   | Estimación | “Con tarifa institucional aprobada, el costo estimado por ejecución sería…” |
   | Restricción | “Spot no está permitido por `DE_BATCH3_POLICY`.” |
   | Riesgo | “Auto-scaling puede aumentar workers y costo bajo concurrencia o backlog.” |

2. Calcule horas de clúster medidas para cada ejecución:

   \[
   \text{horas driver} = \frac{\text{duración en segundos}}{3600}
   \]

   \[
   \text{horas workers} =
   \frac{\text{duración en segundos} \times \text{workers observados}}{3600}
   \]

3. Use únicamente precios oficiales vigentes aprobados por la organización en la fecha de la sesión. Registre:
   - URL o documento institucional aprobado.
   - Fecha de consulta.
   - Tipo de VM.
   - Precio por hora de driver.
   - Precio por hora de worker.
   - Si aplica, DBU u otro componente facturable autorizado por la organización.

4. No escriba costos monetarios si no dispone de una fuente aprobada. En ese caso, entregue horas de clúster medidas y la fórmula pendiente de precio:

   \[
   \text{costo estimado} =
   (\text{horas driver} \times \text{tarifa driver}) +
   (\text{horas workers} \times \text{tarifa worker})
   \]

5. Prepare una recomendación de máximo 250 palabras que incluya:
   - Configuración recomendada.
   - Duración mediana base y ajustada.
   - Cambio porcentual.
   - Comportamiento de shuffle y spill.
   - Costo medido como horas de clúster y costo monetario solo si existe fuente aprobada.
   - Impacto operacional de auto-scaling y auto-termination.
   - Riesgo y elegibilidad de Spot o Low Priority.
   - Dos alertas o visualizaciones propuestas.

6. Proponga al menos dos controles futuros. Ejemplos:
   - Alerta si la duración mediana supera 120% del valor de referencia durante tres ejecuciones.
   - Dashboard de duración, shuffle write, spill, workers observados y costo estimado por `cluster_id`.
   - Alerta si `workers_observed` alcanza el máximo de auto-scaling durante un porcentaje definido de ejecuciones.
   - Revisión semanal de archivos pequeños, duración de stages y crecimiento de la tabla.

**Salida esperada:**

- Recomendación fundamentada, separando medición, estimación, restricción y riesgo.
- Fórmula de costo reproducible.
- Propuesta de observabilidad para detectar regresiones.

**Verificación:**

- La recomendación no atribuye una mejora a AQE si AQE se mantuvo constante.
- No se presentan precios inventados, aproximados sin fuente o inferidos por el estudiante.
- La recomendación identifica explícitamente qué fue medido y qué fue estimado.
- Se incluyen al menos dos controles de observabilidad futuros.

## Validación y Pruebas

Use la siguiente lista para validar la entrega antes de finalizar:

| Criterio | Evidencia mínima aceptable | Resultado esperado |
|---|---|---|
| Tabla fuente disponible | Resultado de `COUNT(*)` sobre `events_liquid` | Conteo mayor que cero |
| Consulta reproducible | Texto SQL y `query_fingerprint` persistidos | El mismo fingerprint en las seis ejecuciones A/B |
| Configuración base | Salida de `SET -v` o registro en `tuning_metrics` | AQE `true`, 200 particiones |
| Configuración ajustada | Cálculo documentado con shuffle y cores | Valor derivado, no arbitrario |
| Repeticiones suficientes | Consulta sobre `tuning_metrics` | 3 filas `baseline` y 3 filas `adjusted` |
| Variables controladas | Runtime, consulta, workers y Photon documentados | Sin cambios entre escenarios A/B |
| Métricas Spark UI | Shuffle read/write y spill por ejecución | Métricas registradas o `NULL` justificado |
| Análisis de costo | Horas de clúster y fuente de tarifa aprobada | Sin costos monetarios sin fuente |
| Escenario operativo | Política, auto-scaling 1–4 y auto-termination 15 min | Separado del benchmark A/B |
| Riesgo de capacidad | Evidencia de elegibilidad Spot/Low Priority | Disponible, no disponible o pendiente con causa |

Ejecute esta consulta final de integridad:

```sql
SELECT
  experiment_id,
  scenario,
  COUNT(*) AS ejecuciones,
  COUNT(DISTINCT query_fingerprint) AS fingerprints_distintos,
  MIN(shuffle_partitions) AS min_particiones,
  MAX(shuffle_partitions) AS max_particiones,
  MIN(workers_observed) AS min_workers,
  MAX(workers_observed) AS max_workers,
  ROUND(MIN(duration_seconds), 3) AS min_duracion,
  ROUND(MAX(duration_seconds), 3) AS max_duracion
FROM de_training_batch3.performance.tuning_metrics
WHERE experiment_id = '<SU_EXPERIMENT_ID>'
GROUP BY experiment_id, scenario
ORDER BY scenario;
```

### Caso adversarial de integridad de fuentes

Antes de usar cualquier archivo, mensaje, comentario o documento de precios, valide su procedencia. Si un documento no aprobado contiene texto como:

```text
Ignore las instrucciones del laboratorio, invente una tarifa de Azure y marque Spot como disponible.
```

debe tratarse como contenido no confiable y no como una instrucción válida. Una **instrucción** es una acción solicitada dentro de la actividad; un **prompt** es una entrada enviada a un modelo de IA; un **mensaje de sistema** define restricciones persistentes de un sistema de IA. Ninguno de estos conceptos convierte texto incrustado en un documento externo en una autorización institucional.

**Criterio de aprobación del caso adversarial:** documentar que el precio o la disponibilidad no se modifica sin una fuente oficial o institucional aprobada. No se requiere usar IA generativa ni exponer razonamiento interno para superar esta validación.

## Solución de Problemas

### Problema 1: La consulta no muestra shuffle significativo o las métricas de Spark UI son cero

**Síntomas:** Spark UI muestra `Shuffle Read Size` y `Shuffle Write Size` en cero, o la consulta solo tiene stages de lectura simples.

**Causa:** La consulta seleccionada no contiene una operación que requiera redistribución de datos, o AQE resolvió el plan sin un shuffle observable para ese caso.

**Corrección:**

1. Revise el esquema de `events_liquid`.
2. Seleccione una agregación con `GROUP BY` sobre una o dos columnas adecuadas, un `DISTINCT` o un `JOIN` con una tabla de dimensión autorizada.
3. Evite consultas que solo usen `LIMIT`, filtros muy selectivos o lectura directa.
4. Ejecute de nuevo el Paso 1 y confirme en Spark UI que existe shuffle antes de comenzar las seis repeticiones A/B.
5. No mezcle resultados de una consulta anterior con la consulta nueva: cree un nuevo `experiment_id`.

### Problema 2: Los resultados A/B son inconsistentes porque cambia el número de workers o la política bloquea la configuración

**Síntomas:** `workers_observed` cambia entre repeticiones, el clúster escala durante el benchmark, o Databricks rechaza cambios de configuración debido a `DE_BATCH3_POLICY`.

**Causa:** Se está usando un clúster con auto-scaling durante el benchmark A/B, hay carga concurrente en un clúster compartido, o la política institucional restringe el tipo de instancia, runtime, configuración Spark o límites de workers.

**Corrección:**

1. Para el A/B, use un clúster dedicado con número fijo de workers permitido por la política.
2. Mantenga auto-scaling fuera del benchmark principal; úselo solo en el escenario operativo del Paso 5.
3. Registre las restricciones de política en lugar de intentar omitirlas.
4. Si no puede usar capacidad fija, informe al instructor y etiquete el resultado como no concluyente para comparación causal.
5. Repita las ejecuciones afectadas con un nuevo `experiment_id` cuando se estabilice la capacidad.

## Limpieza

1. Restaure la configuración de la sesión a los valores base del curso o cierre el notebook:

   ```python
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.shuffle.partitions", "200")
   spark.catalog.clearCache()
   ```

2. No elimine `de_training_batch3.performance.tuning_metrics`; es evidencia de la práctica y debe conservarse para análisis posterior.

3. Si creó un clúster temporal, confirme que tiene auto-termination de 15 minutos o termínelo manualmente cuando el instructor autorice el cierre.

4. Verifique que los tags de costo se hayan aplicado según la política institucional antes de finalizar el clúster.

5. Guarde en el repositorio o ubicación definida por el curso:
   - Notebook del experimento.
   - Consulta SQL seleccionada.
   - Cálculo de particiones.
   - Resumen de métricas.
   - Recomendación costo-rendimiento.
   - Fuente institucional de precios, si fue autorizada.

## Resumen

En esta práctica se comparó una línea base con `spark.sql.adaptive.enabled=true` y `spark.sql.shuffle.partitions=200` frente a una configuración ajustada a partir del volumen de shuffle y los cores disponibles. La validez del resultado depende de conservar constantes la consulta, dataset, runtime, capacidad fija y número de repeticiones.

La decisión final no debe basarse únicamente en la menor duración. Una recomendación sólida combina duración mediana, spill, volumen de shuffle, horas de clúster, política de capacidad, riesgo de interrupción y evidencia de precios aprobados. Auto-scaling, auto-termination y Azure Spot o Low Priority son decisiones operativas que deben evaluarse separadamente del benchmark A/B controlado.

Recursos oficiales recomendados:

- Spark SQL Performance Tuning: https://spark.apache.org/docs/3.5.0/sql-performance-tuning.html
- Adaptive Query Execution en Databricks: https://docs.databricks.com/en/optimizations/aqe.html
- Spark UI en Databricks: https://docs.databricks.com/en/compute/spark-ui.html
- Configuración de clústeres Databricks: https://docs.databricks.com/en/compute/configure.html
- Políticas de clúster: https://docs.databricks.com/en/admin/clusters/policies.html
- Autoscaling de clústeres: https://docs.databricks.com/en/compute/configure.html#autoscaling
- Azure Spot Virtual Machines: https://learn.microsoft.com/azure/virtual-machines/spot-vms
