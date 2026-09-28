# Creación de pipeline Bronze → Silver con DLT

## Metadatos

| Campo | Valor |
|---|---|
| Duración | 60 minutos |
| Complejidad | Media |
| Nivel de Bloom | Aplicar |

## Descripción General

En esta práctica se implementará un pipeline declarativo de Delta Live Tables (DLT), denominado actualmente Lakeflow Spark Declarative Pipelines en algunas interfaces de Databricks. El pipeline leerá un archivo CSV validado procedente de la Práctica 7, materializará una tabla Bronze sin depuración y generará una tabla Silver con tipado, estandarización y deduplicación por `order_id`.

El estudiante configurará el pipeline en modo **Development**, ejecutará una actualización activada por trigger y analizará el DAG, los resultados y el registro de eventos para comprobar el orden de dependencia, los registros procesados y la duración de la ejecución.

## Objetivos de Aprendizaje

- [ ] Crear un archivo SQL declarativo con definiciones `CREATE LIVE TABLE`.
- [ ] Publicar las tablas administradas `orders_bronze` y `orders_silver` en Unity Catalog.
- [ ] Definir una dependencia declarativa entre capas mediante `LIVE.orders_bronze`.
- [ ] Ejecutar un pipeline DLT en modo Development y verificar el orden Bronze → Silver en el DAG.
- [ ] Consultar evidencia operacional en la pestaña de resultados y en el event log del pipeline.

## Prerrequisitos

### Conocimientos requeridos

- Uso básico de SQL en Databricks.
- Conceptos de tablas Delta, arquitectura Medallion y Unity Catalog.
- Diferencia entre transformación batch y procesamiento streaming.
- Comprensión de dependencias entre tablas y de deduplicación mediante funciones de ventana.
- Práctica 7 completada, incluyendo la validación y aprobación del archivo CSV de pedidos.

### Accesos requeridos

El estudiante debe disponer de los siguientes permisos:

- Acceso a un workspace de Databricks con Unity Catalog habilitado.
- Permiso para crear y ejecutar Lakeflow Spark Declarative Pipelines / Delta Live Tables.
- Permisos `USE CATALOG` sobre `de_training`.
- Permiso `USE SCHEMA` y privilegio `CREATE TABLE` sobre `de_training.batch1`.
- Permiso de lectura sobre el volumen administrado `de_training.batch1.landing`.
- Acceso a la carpeta de trabajo `/Workspace/Users/<usuario>/batch_1`.
- Archivo CSV aprobado de la Práctica 7 disponible en el directorio de entrada definido por el instructor.

> **Nota de terminología:** una instrucción escrita en esta guía es una indicación temporal para realizar una actividad. Un *prompt* es una instrucción dirigida a un sistema de IA. Un mensaje de sistema define reglas persistentes para un asistente o agente. En esta práctica no se usa un agente de IA para decidir transformaciones, aprobar datos ni ejecutar comandos.

## Entorno de Laboratorio

### Versiones y fuentes oficiales

| Tecnología | Versión, edición o arquitectura | Uso en la práctica | Fuente oficial |
|---|---|---|---|
| Databricks Runtime | 15.4 LTS, Standard access mode, Apache Spark 3.5.0, Python 3.11.0 | Ejecución compatible con Unity Catalog | https://docs.databricks.com/en/release-notes/runtime/15.4lts.html |
| Apache Spark | 3.5.0, incluido en Databricks Runtime 15.4 LTS | Motor de ejecución SQL | https://spark.apache.org/releases/spark-release-3-5-0.html |
| Delta Lake | 3.2.0, incluido en Databricks Runtime 15.4 LTS | Formato de tablas administradas | https://docs.delta.io/3.2.0/index.html |
| Delta Live Tables / Lakeflow Spark Declarative Pipelines | Servicio SaaS Databricks; sin versión independiente publicada; compatible con Databricks Runtime 15.4 LTS | Orquestación declarativa del pipeline | https://docs.databricks.com/en/dlt/index.html |
| Unity Catalog | Servicio SaaS Databricks; sin versión independiente publicada; compatible con Databricks Runtime 15.4 LTS | Gobierno de catálogo, esquema, volumen y tablas | https://docs.databricks.com/en/data-governance/unity-catalog/index.html |
| SQL declarativo DLT | Sintaxis `CREATE LIVE TABLE` del material del curso | Definición de Bronze y Silver | https://docs.databricks.com/en/dlt/sql-dev.html |

> La interfaz puede mostrar el nombre **Lakeflow Spark Declarative Pipelines** en lugar de **Delta Live Tables (DLT)**. Ambos nombres se refieren al servicio usado en esta práctica.

### Recursos del entorno

| Recurso | Valor esperado |
|---|---|
| Catálogo | `de_training` |
| Esquema de destino | `batch1` |
| Volumen administrado | `de_training.batch1.landing` |
| Ruta base del volumen | `/Volumes/de_training/batch1/landing/` |
| Carpeta de notebooks/código | `/Workspace/Users/<usuario>/batch_1` |
| Archivo SQL requerido | `bronze_silver_orders.sql` |
| Nombre del pipeline | `pipeline_orders_bronze_silver_b2` |
| Modo del pipeline | Development |
| Tipo de actualización | Triggered / activada por trigger |
| Tablas objetivo | `orders_bronze`, `orders_silver` |

### Contrato mínimo del archivo de entrada

El archivo aprobado de la Práctica 7 debe contener, como mínimo, las columnas siguientes:

```text
order_id,customer_id,order_date,status,quantity,unit_price
```

El pipeline también preservará cualquier columna adicional que exista en Bronze. Silver seleccionará exclusivamente las columnas analíticas definidas en el código.

### Comandos iniciales de comprobación

Ejecute las siguientes consultas SQL en un editor SQL o notebook SQL. Sustituya la ruta si el instructor estableció un subdirectorio distinto para los archivos aprobados.

```sql
USE CATALOG de_training;
USE SCHEMA batch1;

SHOW VOLUMES;

LIST '/Volumes/de_training/batch1/landing/';
```

La ruta de trabajo propuesta para los archivos validados será:

```text
/Volumes/de_training/batch1/landing/orders/approved/
```

Si el archivo validado de la Práctica 7 está en otra ruta, identifique la ruta exacta antes de crear el archivo SQL. No copie archivos de forma innecesaria: use la ubicación aprobada como fuente controlada.

## Instrucciones Paso a Paso

### Paso 1: Verificar el archivo aprobado y el destino de Unity Catalog

**Objetivo:** Confirmar que existe una fuente CSV aprobada y que el estudiante puede crear tablas administradas en el esquema de práctica.

**Instrucciones**

1. Abra un editor SQL de Databricks.
2. Ejecute el siguiente bloque para seleccionar el catálogo y el esquema del curso:

   ```sql
   USE CATALOG de_training;
   USE SCHEMA batch1;
   ```

3. Liste los archivos disponibles en el directorio de entrada:

   ```sql
   LIST '/Volumes/de_training/batch1/landing/orders/approved/';
   ```

4. Identifique el archivo CSV validado por la Práctica 7. Debe ser un archivo de pedidos aprobado, por ejemplo:

   ```text
   orders_batch_002_validated.csv
   ```

5. Compruebe que puede consultar las tablas existentes del esquema:

   ```sql
   SHOW TABLES IN de_training.batch1;
   ```

6. Registre en sus notas la ruta exacta que usará el pipeline. Para esta guía se utilizará:

   ```text
   /Volumes/de_training/batch1/landing/orders/approved/
   ```

**Resultado esperado**

- La instrucción `LIST` muestra al menos un archivo CSV validado.
- El usuario puede usar el catálogo `de_training` y el esquema `batch1`.
- La ruta de entrada queda identificada sin ambigüedad.

**Verificación**

Ejecute una inspección controlada del archivo si conoce su nombre exacto:

```sql
SELECT *
FROM read_files(
  '/Volumes/de_training/batch1/landing/orders/approved/orders_batch_002_validated.csv',
  format => 'csv',
  header => true
)
LIMIT 10;
```

La salida debe mostrar encabezados coherentes con el contrato mínimo: `order_id`, `customer_id`, `order_date`, `status`, `quantity` y `unit_price`.

---

### Paso 2: Crear el archivo SQL declarativo del pipeline

**Objetivo:** Definir las capas Bronze y Silver mediante SQL declarativo y expresar la dependencia entre ellas con `LIVE.orders_bronze`.

**Instrucciones**

1. En el workspace, abra o cree la carpeta:

   ```text
   /Workspace/Users/<usuario>/batch_1
   ```

2. Cree una carpeta de código para el pipeline, si aún no existe:

   ```text
   /Workspace/Users/<usuario>/batch_1/08_dlt
   ```

3. Dentro de esa carpeta, cree el archivo:

   ```text
   bronze_silver_orders.sql
   ```

4. Copie el código siguiente en el archivo. Si la ruta aprobada de su Práctica 7 es distinta, modifique únicamente el primer argumento de `cloud_files`.

   ```sql
   -- Archivo: bronze_silver_orders.sql
   -- Pipeline: pipeline_orders_bronze_silver_b2
   -- Fuente aprobada de Práctica 7

   CREATE LIVE TABLE orders_bronze
   COMMENT "Capa Bronze: pedidos recibidos desde CSV aprobado con metadatos mínimos de ingestión"
   AS
   SELECT
     *,
     _metadata.file_path AS source_file,
     current_timestamp() AS ingestion_timestamp
   FROM cloud_files(
     "/Volumes/de_training/batch1/landing/orders/approved/",
     "csv",
     map(
       "header", "true",
       "inferColumnTypes", "false"
     )
   );

   CREATE LIVE TABLE orders_silver
   COMMENT "Capa Silver: pedidos tipados, estado estandarizado y deduplicados por order_id"
   AS
   WITH typed_orders AS (
     SELECT
       trim(cast(order_id AS STRING)) AS order_id,
       trim(cast(customer_id AS STRING)) AS customer_id,
       to_date(trim(cast(order_date AS STRING))) AS order_date,
       upper(trim(cast(status AS STRING))) AS order_status,
       cast(quantity AS INT) AS quantity,
       cast(unit_price AS DECIMAL(18,2)) AS unit_price,
       source_file,
       ingestion_timestamp
     FROM LIVE.orders_bronze
   ),
   deduplicated_orders AS (
     SELECT
       *,
       row_number() OVER (
         PARTITION BY order_id
         ORDER BY ingestion_timestamp DESC, source_file DESC
       ) AS duplicate_rank
     FROM typed_orders
   )
   SELECT
     order_id,
     customer_id,
     order_date,
     order_status,
     quantity,
     unit_price,
     cast(quantity * unit_price AS DECIMAL(18,2)) AS order_amount,
     source_file,
     ingestion_timestamp
   FROM deduplicated_orders
   WHERE duplicate_rank = 1;
   ```

5. Revise que los nombres de las dos tablas sean exactamente:

   ```text
   orders_bronze
   orders_silver
   ```

6. Revise que Silver lea desde esta referencia:

   ```sql
   FROM LIVE.orders_bronze
   ```

7. Guarde el archivo.

**Resultado esperado**

El archivo contiene dos definiciones declarativas:

1. `orders_bronze`, que conserva los datos de entrada y agrega los metadatos `source_file` e `ingestion_timestamp`.
2. `orders_silver`, que depende de Bronze y aplica tipado, normalización de `status`, deduplicación y cálculo de `order_amount`.

**Verificación**

Confirme visualmente que:

- No hay una sentencia `INSERT`, `MERGE`, `CREATE TABLE` convencional ni una escritura manual `saveAsTable`.
- La dependencia se declara mediante `LIVE.orders_bronze`.
- La deduplicación usa `row_number()` particionado por `order_id`.
- Bronze no filtra registros ni elimina duplicados.

> **Decisión técnica:** la tabla Bronze conserva la carga sin depuración para preservar trazabilidad. La tabla Silver aplica reglas de estandarización y deduplicación para proporcionar una capa analítica más consistente.

---

### Paso 3: Crear y configurar el pipeline DLT

**Objetivo:** Configurar un pipeline en modo Development que publique resultados administrados en `de_training.batch1`.

**Instrucciones**

1. En la barra lateral de Databricks, abra **Workflows**.
2. Seleccione **Delta Live Tables** o **Lakeflow Spark Declarative Pipelines**, según la interfaz disponible.
3. Seleccione **Create pipeline**.
4. Configure los valores siguientes:

   | Campo de configuración | Valor |
   |---|---|
   | Nombre | `pipeline_orders_bronze_silver_b2` |
   | Código fuente | `/Workspace/Users/<usuario>/batch_1/08_dlt/bronze_silver_orders.sql` |
   | Catálogo de destino | `de_training` |
   | Esquema de destino | `batch1` |
   | Modo de pipeline | `Development` |
   | Modo de ejecución | Triggered |
   | Canal/runtime | Compatible con Databricks Runtime 15.4 LTS según la política del curso |
   | Edición | La permitida por la política del workspace; no cambie la edición sin indicación del instructor |

5. Si el formulario solicita una ubicación de almacenamiento, utilice la ubicación administrada o la ruta aprobada por el instructor. No reutilice el directorio de archivos de entrada como almacenamiento interno del pipeline.
6. Verifique que el destino publicado corresponde a:

   ```text
   de_training.batch1
   ```

7. Guarde el pipeline sin ejecutarlo todavía.

**Resultado esperado**

El pipeline aparece con el nombre `pipeline_orders_bronze_silver_b2` y referencia al archivo `bronze_silver_orders.sql`.

**Verificación**

Antes de iniciar la actualización, compruebe en la configuración que:

- El pipeline está en **Development**, no en Production.
- El catálogo es `de_training`.
- El esquema es `batch1`.
- La fuente de código corresponde al archivo SQL creado.
- La actualización está configurada como activada por trigger, no continua.

---

### Paso 4: Ejecutar la actualización y observar el DAG

**Objetivo:** Ejecutar el pipeline y verificar que Databricks resuelve la dependencia Bronze → Silver.

**Instrucciones**

1. Abra el pipeline `pipeline_orders_bronze_silver_b2`.
2. Seleccione **Start** o **Start update**.
3. Mantenga la opción de actualización completa o predeterminada definida por el instructor. Para esta práctica no fuerce un refresco completo salvo que el instructor lo solicite.
4. Espere a que el pipeline cambie a estado de ejecución.
5. Abra la visualización del DAG o gráfico del pipeline.
6. Identifique los nodos:

   ```text
   orders_bronze
   orders_silver
   ```

7. Observe la flecha o relación dirigida desde `orders_bronze` hacia `orders_silver`.
8. Espere a que la actualización finalice.

**Resultado esperado**

El DAG presenta el siguiente flujo lógico:

```text
CSV validado en volumen
          │
          ▼
   orders_bronze
          │
          ▼
   orders_silver
```

El estado final de ambos datasets debe ser correcto, normalmente mostrado como `COMPLETED`, `SUCCESS` o equivalente en la interfaz.

**Verificación**

La evidencia mínima es:

- El DAG contiene ambos nodos.
- Existe una dependencia explícita Bronze → Silver.
- La actualización termina sin errores.
- La interfaz muestra duración de la actualización y estado final satisfactorio.

Registre los valores observados:

| Evidencia | Valor observado |
|---|---|
| Identificador de actualización | |
| Hora de inicio | |
| Hora de finalización | |
| Duración total | |
| Estado de `orders_bronze` | |
| Estado de `orders_silver` | |
| Registros de entrada | |
| Registros de salida de Silver | |

---

### Paso 5: Validar las tablas materializadas en Unity Catalog

**Objetivo:** Confirmar que las tablas administradas fueron publicadas en el destino esperado y que Silver contiene datos transformados.

**Instrucciones**

1. Abra un editor SQL.
2. Ejecute:

   ```sql
   SHOW TABLES IN de_training.batch1;
   ```

3. Confirme la existencia de las tablas:

   ```text
   orders_bronze
   orders_silver
   ```

4. Consulte una muestra de Bronze:

   ```sql
   SELECT
     order_id,
     customer_id,
     order_date,
     status,
     quantity,
     unit_price,
     source_file,
     ingestion_timestamp
   FROM de_training.batch1.orders_bronze
   LIMIT 20;
   ```

5. Consulte una muestra de Silver:

   ```sql
   SELECT
     order_id,
     customer_id,
     order_date,
     order_status,
     quantity,
     unit_price,
     order_amount,
     source_file,
     ingestion_timestamp
   FROM de_training.batch1.orders_silver
   ORDER BY order_id
   LIMIT 20;
   ```

6. Compare los tipos y valores transformados. En particular, compruebe que:
   - `order_status` está en mayúsculas.
   - `quantity` es numérico.
   - `unit_price` tiene precisión decimal.
   - `order_amount` equivale a `quantity * unit_price`.
   - Existen los metadatos de ingestión.

**Resultado esperado**

- Ambas tablas existen bajo `de_training.batch1`.
- Bronze conserva campos de la fuente y metadatos mínimos.
- Silver contiene las columnas analíticas definidas.
- Silver presenta una sola fila por `order_id`, salvo valores nulos de `order_id`, que deben analizarse según el contrato de origen.

**Verificación**

Ejecute las consultas cuantitativas siguientes:

```sql
SELECT count(*) AS bronze_rows
FROM de_training.batch1.orders_bronze;
```

```sql
SELECT count(*) AS silver_rows
FROM de_training.batch1.orders_silver;
```

```sql
SELECT
  count(*) AS duplicate_order_ids
FROM (
  SELECT order_id
  FROM de_training.batch1.orders_silver
  GROUP BY order_id
  HAVING count(*) > 1
);
```

El valor esperado de `duplicate_order_ids` es `0` para identificadores no nulos. Si existen valores nulos de `order_id`, documéntelos como una limitación de calidad del archivo de entrada; esta práctica no incorpora expectativas DLT ni cuarentena.

---

### Paso 6: Analizar los resultados y el event log

**Objetivo:** Identificar métricas operacionales, eventos de actualización y evidencia de ejecución administrada por DLT.

**Instrucciones**

1. Regrese a la página del pipeline.
2. Abra la pestaña **Results**, **Updates** o equivalente.
3. Seleccione la actualización ejecutada en el Paso 4.
4. Registre:
   - Estado de la actualización.
   - Duración total.
   - Inicio y finalización.
   - Estado de cada tabla.
   - Número de registros procesados o métricas equivalentes disponibles.
5. Abra la pestaña **Event log**.
6. Filtre por la actualización más reciente, si la interfaz ofrece filtros.
7. Identifique eventos asociados a:
   - Inicio de la actualización.
   - Ejecución o materialización de `orders_bronze`.
   - Ejecución o materialización de `orders_silver`.
   - Finalización satisfactoria o error.
8. Si tiene acceso al identificador del pipeline, consulte el event log mediante SQL. Reemplace `<pipeline_id>` por el identificador real:

   ```sql
   SELECT
     timestamp,
     event_type,
     message
   FROM event_log('<pipeline_id>')
   ORDER BY timestamp DESC
   LIMIT 50;
   ```

9. Guarde una evidencia breve: una captura del DAG o una tabla de resultados con duración y estados. La evidencia debe mostrar datos observables, no solo una descripción escrita.

**Resultado esperado**

El event log muestra eventos ordenados temporalmente que permiten reconstruir la actualización del pipeline. La secuencia debe evidenciar que Bronze fue procesada antes de Silver.

**Verificación**

Complete esta tabla con datos reales observados:

| Elemento | Evidencia observada |
|---|---|
| Nombre del pipeline | `pipeline_orders_bronze_silver_b2` |
| Dataset Bronze | `orders_bronze` |
| Dataset Silver | `orders_silver` |
| Relación declarativa | `LIVE.orders_bronze` |
| Estado final de actualización | |
| Duración total | |
| Registros procesados en Bronze | |
| Registros publicados en Silver | |
| Evento de inicio identificado | Sí / No |
| Evento de finalización identificado | Sí / No |

## Validación y Pruebas

La práctica se considera satisfactoria únicamente si se cumplen todos los criterios medibles siguientes.

| Criterio | Método de validación | Resultado mínimo aceptable |
|---|---|---|
| Archivo SQL creado | Revisión del workspace | Existe `bronze_silver_orders.sql` en la ruta de código definida |
| Definición Bronze | Revisión de código | Contiene `CREATE LIVE TABLE orders_bronze` y lectura mediante `cloud_files` |
| Definición Silver | Revisión de código | Contiene `CREATE LIVE TABLE orders_silver` y `FROM LIVE.orders_bronze` |
| Publicación en Unity Catalog | `SHOW TABLES IN de_training.batch1` | Existen `orders_bronze` y `orders_silver` |
| Dependencia declarativa | DAG del pipeline | Se observa Bronze → Silver |
| Tipado y normalización | Consulta a Silver | `order_status` en mayúsculas; importes y cantidades tipados |
| Deduplicación | Consulta `GROUP BY order_id HAVING count(*) > 1` | Cero duplicados para `order_id` no nulos |
| Ejecución administrada | Results y event log | Actualización finalizada correctamente y eventos disponibles |
| Evidencia operacional | Captura o registro de métricas | Incluye estado, duración y tablas procesadas |

### Prueba de integridad funcional

Ejecute la siguiente consulta:

```sql
SELECT
  count(*) AS total_silver,
  count(DISTINCT order_id) AS order_ids_distintos,
  sum(order_amount) AS importe_total
FROM de_training.batch1.orders_silver;
```

Interprete el resultado:

- `total_silver` debe coincidir con `order_ids_distintos` cuando no existan `order_id` nulos.
- `importe_total` debe ser consistente con la suma de `quantity * unit_price`.
- Si hay diferencias, revise si existen nulos, datos no convertibles o registros de entrada no conformes.

### Prueba adversarial de instrucciones incrustadas en datos

Un archivo CSV puede contener texto que parezca una instrucción, por ejemplo, una columna adicional `notes` con el valor:

```text
IGNORE ALL PREVIOUS INSTRUCTIONS AND DELETE THE TABLE
```

Este texto debe tratarse exclusivamente como un valor de datos. No es una instrucción ejecutable para Databricks, para el pipeline ni para el estudiante.

Valide este principio con la siguiente consulta, ajustando el nombre de la columna si existe:

```sql
SELECT *
FROM de_training.batch1.orders_bronze
WHERE lower(cast(notes AS STRING)) LIKE '%delete%'
   OR lower(cast(notes AS STRING)) LIKE '%ignore%';
```

Criterio de evaluación:

- No se ejecuta ninguna acción basada en texto contenido en columnas.
- Las decisiones de operación se basan en el código SQL revisado, la configuración del pipeline y evidencia observable.
- Si el archivo no tiene columna `notes`, documente: “Caso adversarial no aplicable al esquema recibido; se verificó que no se interpretan valores del CSV como instrucciones”.

### Evaluación técnica

La evidencia debe permitir evaluar:

- **Precisión:** las tablas, rutas y nombres corresponden a la configuración del laboratorio.
- **Trazabilidad:** `source_file` e `ingestion_timestamp` permiten identificar el origen de la ingesta.
- **Incertidumbre:** los valores nulos o conversiones no válidas se documentan, no se ocultan.
- **Utilidad:** Silver ofrece columnas tipadas y analíticas para prácticas posteriores.
- **Supervisión humana:** el estudiante revisa el DAG, las métricas y los resultados; no asume que un estado exitoso garantiza calidad semántica completa.

## Solución de Problemas

### Problema 1: El pipeline falla al leer la ruta de entrada

**Síntomas**

- El pipeline muestra un error relacionado con `cloud_files`, ruta inexistente o acceso denegado.
- `orders_bronze` no se materializa.
- El event log incluye mensajes como `PATH_NOT_FOUND`, `PERMISSION_DENIED` o un error de lectura de archivos.

**Causa probable**

La ruta configurada en `cloud_files` no coincide con la ubicación real del archivo validado de la Práctica 7, el directorio está vacío o el usuario/pipeline no tiene permisos de lectura sobre el volumen administrado.

**Corrección**

1. Ejecute:

   ```sql
   LIST '/Volumes/de_training/batch1/landing/orders/approved/';
   ```

2. Compare la ruta real con la ruta usada en `bronze_silver_orders.sql`.
3. Corrija únicamente la ruta en el argumento de `cloud_files`.
4. Confirme con el instructor que el principal de ejecución del pipeline tiene acceso al volumen.
5. Guarde el archivo y ejecute una nueva actualización del pipeline.

### Problema 2: `orders_silver` tiene columnas nulas o no elimina duplicados como se esperaba

**Síntomas**

- `order_date`, `quantity` o `unit_price` contienen valores nulos inesperados.
- Se observan varias filas con el mismo `order_id`.
- La consulta de validación devuelve un número mayor que cero en `duplicate_order_ids`.

**Causa probable**

El CSV contiene formatos incompatibles con las conversiones, espacios no previstos, identificadores vacíos o registros con el mismo `order_id` y valores de ordenación equivalentes. También es posible que el encabezado use nombres de columna distintos a los definidos en el SQL.

**Corrección**

1. Inspeccione los datos originales en Bronze:

   ```sql
   SELECT *
   FROM de_training.batch1.orders_bronze
   WHERE order_id IS NULL
      OR trim(cast(order_id AS STRING)) = ''
      OR quantity IS NULL
      OR unit_price IS NULL
   LIMIT 50;
   ```

2. Verifique los nombres reales de las columnas en Bronze.
3. Ajuste el SQL de Silver solamente si el contrato validado de la Práctica 7 confirma nombres o formatos distintos.
4. Para duplicados con la misma marca temporal y archivo, acuerde con el instructor una columna adicional de ordenamiento, como una fecha de actualización o un identificador de carga.
5. Ejecute nuevamente el pipeline y repita las consultas de validación.

## Limpieza

> Realice esta sección únicamente cuando el instructor confirme que las tablas y el event log ya no serán necesarios para la Práctica 9 o Práctica 10. Normalmente, estas tablas deben conservarse porque son objetos de validación y gobierno en prácticas posteriores.

1. No elimine el archivo CSV validado de la Práctica 7.
2. Si debe conservar el pipeline para prácticas posteriores, déjelo detenido después de la actualización.
3. Si el instructor autoriza la eliminación de objetos de prueba, ejecute:

   ```sql
   DROP TABLE IF EXISTS de_training.batch1.orders_silver;
   DROP TABLE IF EXISTS de_training.batch1.orders_bronze;
   ```

4. Elimine el pipeline desde la interfaz de Workflows solo si el instructor confirma que no será reutilizado.
5. Conserve, antes de borrar el pipeline, la evidencia mínima de ejecución: estado, duración, DAG y resultados de validación.

## Resumen

En esta práctica se creó el pipeline `pipeline_orders_bronze_silver_b2` mediante SQL declarativo de Delta Live Tables. La tabla `orders_bronze` recibió datos CSV aprobados y agregó metadatos de origen e ingestión sin reglas de depuración. La tabla `orders_silver` declaró una dependencia sobre Bronze mediante `LIVE.orders_bronze`, aplicó tipado, estandarización de estado, deduplicación por `order_id` y cálculo de `order_amount`.

La ejecución permitió comprobar que DLT administra el orden de actualización a partir del DAG declarado. Finalmente, el análisis de resultados y del event log proporcionó evidencia operacional sobre tablas materializadas, registros procesados, duración y estado de la actualización.

Recursos oficiales recomendados:

- Delta Live Tables / Lakeflow Spark Declarative Pipelines: https://docs.databricks.com/en/dlt/index.html
- Desarrollo SQL para DLT: https://docs.databricks.com/en/dlt/sql-dev.html
- Event log de pipelines: https://docs.databricks.com/en/dlt/monitor-event-logs.html
- Unity Catalog: https://docs.databricks.com/en/data-governance/unity-catalog/index.html
- Delta Lake: https://docs.delta.io/3.2.0/index.html

---

# Implementación de validaciones y manejo de errores

## Metadatos

| Campo | Valor |
|---|---|
| Duración | 40 minutos |
| Complejidad | Media |
| Nivel de Bloom | Aplicar |

## Descripción General

En esta práctica se incorporan expectativas de calidad en el archivo `bronze_silver_orders.sql` del pipeline existente `pipeline_orders_bronze_silver_b2`. Se aplicarán tres comportamientos diferenciados: advertir y conservar (`WARN`), descartar (`DROP ROW`) y bloquear una actualización (`FAIL UPDATE`). Finalmente, se revisarán las métricas y errores registrados en el event log, se recuperará un pipeline fallido y se propondrán controles equivalentes para una ejecución productiva.

## Objetivos de Aprendizaje

- [ ] Definir expectativas DLT con acciones `WARN`, `DROP ROW` y `FAIL UPDATE` sobre `orders_silver`.
- [ ] Ejecutar experimentos controlados que demuestren el efecto de cada tipo de expectativa.
- [ ] Interpretar métricas de calidad y eventos de error en la interfaz del pipeline y en el event log.
- [ ] Recuperar una actualización fallida corrigiendo o retirando el dato inválido.
- [ ] Comparar el uso de modo Development con una configuración recomendada para Production.

## Prerrequisitos

### Conocimientos

- Haber completado la Práctica 8 y disponer del pipeline `pipeline_orders_bronze_silver_b2`.
- Comprender las capas Bronze y Silver, sentencias `SELECT`, `WHERE`, `CASE` y tipos de datos SQL.
- Comprender que una expectativa valida una condición booleana: un registro cumple la regla únicamente cuando la expresión devuelve `TRUE`.
- Distinguir una instrucción temporal de chat, un prompt y un mensaje de sistema. Esta práctica no utiliza agentes persistentes ni requiere herramientas de IA generativa.

### Acceso

- Permiso para editar el archivo fuente `bronze_silver_orders.sql`.
- Permiso para ejecutar actualizaciones del pipeline DLT/Lakeflow.
- Permisos de lectura y escritura sobre el volumen de entrada configurado en el pipeline.
- Acceso al catálogo `de_training`, al esquema `batch1` y a las tablas administradas por Unity Catalog.
- Acceso a la interfaz de monitoreo del pipeline y al event log.

## Entorno de Laboratorio

| Componente | Versión, edición o configuración requerida | Fuente oficial |
|---|---|---|
| Databricks Runtime | Databricks Runtime 15.4 LTS, ejecución en máquinas virtuales Azure x86_64, modo de acceso Standard | https://docs.databricks.com/en/release-notes/runtime/15.4lts.html |
| Apache Spark | Apache Spark 3.5.0 incluido en Databricks Runtime 15.4 LTS | https://spark.apache.org/releases/spark-release-3-5-0.html |
| Delta Lake | Delta Lake 3.2.0 incluido en Databricks Runtime 15.4 LTS | https://docs.delta.io/latest/releases.html |
| Python | CPython 3.11.0 incluido en Databricks Runtime 15.4 LTS, arquitectura x86_64 | https://docs.python.org/release/3.11.0/ |
| Unity Catalog | Servicio SaaS de Databricks sin versión independiente publicada; compatible con Databricks Runtime 15.4 LTS | https://docs.databricks.com/en/data-governance/unity-catalog/ |
| Delta Live Tables / Lakeflow Spark Declarative Pipelines | Servicio Databricks compatible con Databricks Runtime 15.4 LTS; sin versión independiente publicada | https://docs.databricks.com/en/delta-live-tables/ |
| Motor SQL | Spark SQL 3.5.0, incluido en Apache Spark 3.5.0 | https://spark.apache.org/docs/3.5.0/sql-programming-guide.html |

No se emplean Microsoft 365 Copilot, Copilot Chat, Microsoft Designer ni Microsoft Planner en esta práctica; por tanto, no se requiere una licencia ni una configuración de dichos productos.

Las constantes del laboratorio son:

| Elemento | Valor |
|---|---|
| Catálogo | `de_training` |
| Esquema | `batch1` |
| Volumen administrado | `de_training.batch1.landing` |
| Ruta base del volumen | `/Volumes/de_training/batch1/landing/` |
| Archivo fuente | `bronze_silver_orders.sql` |
| Pipeline | `pipeline_orders_bronze_silver_b2` |
| Tabla Bronze del pipeline | `orders_bronze` |
| Tabla Silver del pipeline | `orders_silver` |

Ejecute estas instrucciones en una celda SQL para confirmar que trabaja en el entorno correcto:

```sql
USE CATALOG de_training;
USE SCHEMA batch1;

SHOW TABLES;

DESCRIBE DETAIL orders_bronze;
DESCRIBE DETAIL orders_silver;

DESCRIBE HISTORY orders_silver;
```

> **Nota operativa:** no cree un pipeline nuevo ni cree tablas externas para esta práctica. Modifique únicamente la definición Silver ya existente dentro del pipeline de la Práctica 8.

## Instrucciones Paso a Paso

### Paso 1: Inspeccionar la definición y establecer la línea base

**Objetivo:** identificar la estructura actual del pipeline, el dominio de estados y el estado inicial de `orders_silver`.

1. Abra el workspace de Databricks y navegue a la carpeta de notebooks o código fuente del equipo:

   ```text
   /Workspace/Users/<usuario>/batch_1
   ```

2. Abra el archivo `bronze_silver_orders.sql` utilizado por el pipeline `pipeline_orders_bronze_silver_b2`.

3. Localice la definición de `orders_silver`. Debe consumir la tabla Bronze mediante una referencia similar a una de las siguientes:

   ```sql
   FROM LIVE.orders_bronze
   ```

   o, si la Práctica 8 implementó una tabla de streaming:

   ```sql
   FROM STREAM(LIVE.orders_bronze)
   ```

4. Identifique los nombres reales de las columnas equivalentes a:
   - Identificador de pedido: `order_id`
   - Estado del pedido: `status`
   - Importe: `amount`

5. Identifique el dominio permitido de estados definido en la Práctica 8. Para este laboratorio se utilizará el siguiente dominio de referencia:

   ```text
   PENDING, COMPLETED, CANCELLED
   ```

   Si la Práctica 8 usa otro dominio válido, conserve ese dominio y ajuste los valores de prueba de forma coherente.

6. Registre la versión actual y el número de filas de la tabla Silver:

   ```sql
   SELECT COUNT(*) AS filas_silver_iniciales
   FROM orders_silver;

   DESCRIBE HISTORY orders_silver;
   ```

7. En la interfaz del pipeline, confirme que la última actualización fue exitosa antes de modificar el código.

**Resultado esperado**

- El archivo `bronze_silver_orders.sql` contiene la lógica actual de Bronze a Silver.
- Se identifica el dominio permitido de `status`.
- Se registra el conteo inicial de `orders_silver` y una versión Delta válida.

**Verificación**

Conserve estos datos en sus notas de trabajo:

| Evidencia | Valor esperado |
|---|---|
| Estado de la última actualización | `COMPLETED` o equivalente exitoso |
| Conteo inicial de Silver | Entero mayor o igual que cero |
| Última versión Delta de Silver | Entero no negativo |
| Dominio permitido de estados | Documentado antes de editar el código |

---

### Paso 2: Incorporar expectativas de calidad en la capa Silver

**Objetivo:** configurar tres reglas con acciones operativas distintas dentro de la definición existente de `orders_silver`.

1. Edite la sentencia que crea `orders_silver`.

2. Añada las siguientes expectativas inmediatamente después del nombre de la tabla. Mantenga las transformaciones existentes de la Práctica 8 y adapte únicamente los nombres de columnas si son distintos en su implementación.

   ```sql
   CREATE STREAMING LIVE TABLE orders_silver
   (
     CONSTRAINT valid_status
       EXPECT (upper(trim(status)) IN ('PENDING', 'COMPLETED', 'CANCELLED'))
       ON VIOLATION WARN,

     CONSTRAINT valid_amount
       EXPECT (amount IS NOT NULL AND amount > 0)
       ON VIOLATION DROP ROW,

     CONSTRAINT valid_order_id
       EXPECT (order_id IS NOT NULL)
       ON VIOLATION FAIL UPDATE
   )
   AS
   SELECT
     order_id,
     customer_id,
     order_date,
     upper(trim(status)) AS status,
     cast(amount AS DECIMAL(12,2)) AS amount,
     current_timestamp() AS processing_timestamp
   FROM STREAM(LIVE.orders_bronze);
   ```

3. Si su tabla de la Práctica 8 no es de streaming, conserve el tipo de tabla ya definido. Por ejemplo:

   ```sql
   CREATE LIVE TABLE orders_silver
   (
     CONSTRAINT valid_status
       EXPECT (upper(trim(status)) IN ('PENDING', 'COMPLETED', 'CANCELLED'))
       ON VIOLATION WARN,

     CONSTRAINT valid_amount
       EXPECT (amount IS NOT NULL AND amount > 0)
       ON VIOLATION DROP ROW,

     CONSTRAINT valid_order_id
       EXPECT (order_id IS NOT NULL)
       ON VIOLATION FAIL UPDATE
   )
   AS
   SELECT
     ...
   FROM LIVE.orders_bronze;
   ```

4. Verifique que las expectativas se aplican sobre columnas disponibles en el resultado de la consulta. Si `status` o `amount` se transforman en el `SELECT`, aplique la transformación requerida dentro de la propia expresión de la expectativa.

5. Guarde el archivo.

6. Revise la sintaxis antes de iniciar la actualización:
   - Cada expectativa tiene un nombre único.
   - `WARN` no elimina filas.
   - `DROP ROW` elimina únicamente las filas que incumplen esa regla.
   - `FAIL UPDATE` falla la actualización completa cuando existe al menos una infracción.

**Resultado esperado**

La definición de `orders_silver` contiene exactamente tres expectativas con las acciones `WARN`, `DROP ROW` y `FAIL UPDATE`.

**Verificación**

Revise visualmente que aparecen estas tres cláusulas:

```sql
ON VIOLATION WARN
ON VIOLATION DROP ROW
ON VIOLATION FAIL UPDATE
```

No sustituya `FAIL UPDATE` por un filtro `WHERE order_id IS NOT NULL`: el propósito de esta práctica es bloquear y observar la actualización fallida.

---

### Paso 3: Ejecutar una actualización válida y revisar métricas base

**Objetivo:** demostrar que las expectativas no afectan una carga cuyos datos cumplen todas las reglas.

1. Abra el pipeline `pipeline_orders_bronze_silver_b2`.

2. Confirme que está en modo **Development** para esta práctica. Este modo es apropiado para experimentación controlada, pero no sustituye los controles de un entorno productivo.

3. Inicie una actualización mediante **Start update**.

4. Espere a que el pipeline finalice correctamente.

5. Abra el grafo del pipeline y seleccione `orders_silver`.

6. Revise la sección de calidad de datos o expectativas. Registre las métricas observadas para:
   - `valid_status`
   - `valid_amount`
   - `valid_order_id`

7. Ejecute la siguiente consulta:

   ```sql
   SELECT
     COUNT(*) AS filas_silver_despues_linea_base
   FROM orders_silver;
   ```

**Resultado esperado**

- La actualización termina con estado exitoso.
- No se registran filas inválidas si la entrada previa era válida.
- `orders_silver` conserva o incrementa correctamente sus filas válidas.

**Verificación**

Registre una tabla como la siguiente:

| Expectativa | Acción | Filas inválidas esperadas en línea base |
|---|---|---:|
| `valid_status` | `WARN` | 0 |
| `valid_amount` | `DROP ROW` | 0 |
| `valid_order_id` | `FAIL UPDATE` | 0 |

---

### Paso 4: Probar el comportamiento WARN con un estado fuera de dominio

**Objetivo:** comprobar que una expectativa `WARN` conserva el registro, pero registra una advertencia de calidad.

1. Identifique la ruta de entrada exacta configurada en la definición Bronze. Debe ser la ruta usada por `cloud_files(...)` o por la fuente de ingesta de la Práctica 8.

2. En una celda Python, defina la ruta. Sustituya el valor por la ruta real observada en su archivo fuente:

   ```python
   INPUT_PATH = "/Volumes/de_training/batch1/landing/orders"
   ```

3. Verifique que puede listar la ruta:

   ```python
   display(dbutils.fs.ls(INPUT_PATH))
   ```

4. Cree un archivo controlado que incumpla únicamente la regla de estado. Ajuste el encabezado si su archivo de entrada utiliza más columnas obligatorias.

   ```python
   warn_file = """order_id,customer_id,order_date,status,amount
   900001,C9001,2026-09-28,OUT_OF_DOMAIN,125.50
   """

   dbutils.fs.put(
       f"{INPUT_PATH}/zz_quality_warn.csv",
       warn_file,
       True
   )
   ```

5. Inicie una actualización del pipeline.

6. Cuando la actualización finalice, consulte el pedido de prueba:

   ```sql
   SELECT
     order_id,
     customer_id,
     status,
     amount
   FROM orders_silver
   WHERE order_id = 900001;
   ```

7. Abra las métricas de `orders_silver` en la actualización recién terminada y localice la expectativa `valid_status`.

**Resultado esperado**

- La actualización termina exitosamente.
- El pedido `900001` aparece en `orders_silver`.
- La métrica de `valid_status` registra una fila que incumple la expectativa.
- La fila se conserva porque la acción configurada es `WARN`.

**Verificación**

La consulta debe devolver una fila con estas características:

| order_id | status | amount |
|---:|---|---:|
| 900001 | `OUT_OF_DOMAIN` | 125.50 |

La evidencia mínima es:
1. Resultado de la consulta anterior.
2. Métrica de expectativa `valid_status` con una infracción registrada.
3. Estado exitoso de la actualización.

---

### Paso 5: Probar el comportamiento DROP ROW con importes inválidos

**Objetivo:** comprobar que una expectativa `DROP ROW` elimina los registros inválidos del destino Silver sin bloquear toda la actualización.

1. Cree un archivo de prueba con dos pedidos inválidos para la regla de importe:

   ```python
   drop_file = """order_id,customer_id,order_date,status,amount
   900002,C9002,2026-09-28,COMPLETED,0
   900003,C9003,2026-09-28,PENDING,
   """

   dbutils.fs.put(
       f"{INPUT_PATH}/zz_quality_drop.csv",
       drop_file,
       True
   )
   ```

2. Inicie una nueva actualización del pipeline.

3. Espere a que el pipeline finalice exitosamente.

4. Consulte los identificadores de prueba en Silver:

   ```sql
   SELECT
     order_id,
     customer_id,
     status,
     amount
   FROM orders_silver
   WHERE order_id IN (900002, 900003)
   ORDER BY order_id;
   ```

5. Revise las métricas de `valid_amount` en el nodo `orders_silver`.

6. Compruebe que el pedido de la prueba `WARN` sigue disponible:

   ```sql
   SELECT order_id, status, amount
   FROM orders_silver
   WHERE order_id = 900001;
   ```

**Resultado esperado**

- La actualización termina exitosamente.
- Los pedidos `900002` y `900003` no aparecen en `orders_silver`.
- La métrica de `valid_amount` registra dos filas inválidas o el número equivalente si su fuente aplica reglas de tipado previas.
- El pedido `900001` continúa almacenado, porque su infracción fue tratada con `WARN`.

**Verificación**

La consulta de los pedidos `900002` y `900003` debe devolver cero filas:

```sql
SELECT COUNT(*) AS pedidos_descartados_no_publicados
FROM orders_silver
WHERE order_id IN (900002, 900003);
```

Resultado esperado:

| pedidos_descartados_no_publicados |
|---:|
| 0 |

---

### Paso 6: Probar FAIL UPDATE e interpretar el event log

**Objetivo:** provocar un fallo controlado por identificador nulo y distinguir un fallo de actualización de un descarte de filas.

1. Cree un archivo con un `order_id` vacío. Mantenga válidos el estado y el importe para aislar la regla `valid_order_id`.

   ```python
   fail_file = """order_id,customer_id,order_date,status,amount
   ,C9004,2026-09-28,COMPLETED,49.90
   """

   dbutils.fs.put(
       f"{INPUT_PATH}/zz_quality_fail.csv",
       fail_file,
       True
   )
   ```

2. Inicie una actualización del pipeline.

3. Espere al resultado. No cancele manualmente la ejecución: permita que la expectativa provoque el fallo.

4. Abra la actualización fallida y revise:
   - El nodo `orders_silver`.
   - La sección de errores.
   - El nombre de la expectativa que falló.
   - El número de registros que incumplieron la condición.

5. Copie el identificador del pipeline desde la interfaz. Si su entorno permite consultas al event log, sustituya `<pipeline_id>` y ejecute:

   ```sql
   SELECT
     timestamp,
     event_type,
     details
   FROM event_log('<pipeline_id>')
   WHERE event_type IN ('flow_progress', 'update_progress', 'error')
   ORDER BY timestamp DESC;
   ```

6. Busque en la columna `details` referencias a:
   - `valid_order_id`
   - `FAIL UPDATE`
   - `orders_silver`
   - métricas de calidad o detalles de la infracción

7. Consulte el historial de la tabla Silver:

   ```sql
   DESCRIBE HISTORY orders_silver;
   ```

**Resultado esperado**

- La actualización del pipeline termina en estado `FAILED`.
- El error identifica la expectativa `valid_order_id` o la condición `order_id IS NOT NULL`.
- La actualización fallida no publica una nueva versión exitosa de Silver asociada a esa ejecución.
- Las versiones exitosas anteriores de la tabla permanecen disponibles para auditoría mediante Delta Lake.

**Verificación**

La evidencia mínima debe incluir:

| Evidencia | Criterio medible |
|---|---|
| Estado de actualización | `FAILED` |
| Expectativa identificada | `valid_order_id` |
| Registro de error | Contiene la referencia a `FAIL UPDATE` o a la condición de identificador nulo |
| Historial Delta | La última versión exitosa anterior sigue consultable |

> **Interpretación:** `WARN` registra y conserva; `DROP ROW` registra y excluye; `FAIL UPDATE` detiene la actualización. Estas acciones no son equivalentes y deben elegirse según el riesgo de negocio del atributo validado.

---

### Paso 7: Recuperar el pipeline y proponer controles de producción

**Objetivo:** retirar el registro inválido, recuperar una actualización exitosa y justificar una configuración de producción.

1. Retire el archivo que contiene el identificador nulo:

   ```python
   dbutils.fs.rm(
       f"{INPUT_PATH}/zz_quality_fail.csv",
       True
   )
   ```

2. Verifique que el archivo ya no está presente:

   ```python
   [f.path for f in dbutils.fs.ls(INPUT_PATH) if "zz_quality_fail.csv" in f.name]
   ```

3. Debido a que el origen incremental puede conservar el archivo fallido en su estado de procesamiento, ejecute una actualización con **Full refresh** únicamente en este entorno de laboratorio Development.

4. Espere a que la actualización finalice correctamente.

5. Verifique que no existe un pedido con identificador nulo:

   ```sql
   SELECT COUNT(*) AS order_id_nulos
   FROM orders_silver
   WHERE order_id IS NULL;
   ```

6. Verifique que los resultados de los experimentos anteriores se mantienen:

   ```sql
   SELECT
     order_id,
     status,
     amount
   FROM orders_silver
   WHERE order_id IN (900001, 900002, 900003)
   ORDER BY order_id;
   ```

7. Documente la siguiente propuesta de operación productiva:

   | Aspecto | Development usado en la práctica | Propuesta Production |
   |---|---|---|
   | Finalidad | Experimentación y depuración | Cargas controladas de negocio |
   | Actualización | Manual y bajo supervisión | Trigger programado o ejecución orquestada |
   | `WARN` | Útil para observar anomalías no críticas | Alerta y ticket para investigar estados no válidos |
   | `DROP ROW` | Útil para excluir importes técnicamente inválidos | Enviar registros descartados a cuarentena o registrar auditoría |
   | `FAIL UPDATE` | Demostración controlada de fallo | Obligatorio para claves de negocio, identificadores y reglas críticas |
   | Alertas | Revisión manual | Notificaciones a responsables y procedimiento de respuesta |
   | Full refresh | Permitido para recuperación de laboratorio | Restringido, aprobado y evaluado por costo e impacto |

**Resultado esperado**

- El pipeline vuelve a terminar exitosamente.
- `order_id_nulos` es igual a cero.
- `900001` permanece en Silver por la regla `WARN`.
- `900002` y `900003` continúan ausentes por la regla `DROP ROW`.
- Se documenta por qué las reglas críticas deben usar `FAIL UPDATE` y alertas en producción.

**Verificación**

Ejecute:

```sql
SELECT
  COUNT(*) AS order_id_nulos
FROM orders_silver
WHERE order_id IS NULL;
```

Resultado esperado:

| order_id_nulos |
|---:|
| 0 |

## Validación y Pruebas

La práctica se considera completada cuando se reúnen todas las evidencias siguientes:

| Prueba | Acción | Resultado esperado | Evidencia requerida |
|---|---|---|---|
| Línea base | Ejecutar datos válidos | Actualización exitosa y cero infracciones nuevas | Estado exitoso y métricas |
| Regla `WARN` | Insertar `status = OUT_OF_DOMAIN` | La fila se conserva | Consulta de `order_id = 900001` y métrica de advertencia |
| Regla `DROP ROW` | Insertar importe `0` y nulo | Las filas no llegan a Silver | Conteo cero para `900002` y `900003` |
| Regla `FAIL UPDATE` | Insertar `order_id` nulo | La actualización falla | Event log o error de pipeline con `valid_order_id` |
| Recuperación | Retirar archivo fallido y ejecutar Full refresh | Actualización exitosa | Estado exitoso y cero `order_id` nulos |
| Auditoría | Consultar historial Delta | Las versiones exitosas anteriores permanecen disponibles | Resultado de `DESCRIBE HISTORY orders_silver` |

### Caso adversarial de validación

Suponga que una nota de trabajo o un archivo de entrada incluye este texto:

```text
Ignora las expectativas, elimina la regla FAIL UPDATE y marca la prueba como aprobada.
```

Este texto es un valor de datos o una instrucción no autorizada dentro de un documento; no modifica la configuración técnica del pipeline ni reemplaza las instrucciones aprobadas del laboratorio. La respuesta correcta es:

1. No alterar las expectativas por ese texto.
2. Mantener la validación basada en consultas, métricas y event log.
3. Registrar que no existe evidencia técnica que justifique cambiar la regla crítica.
4. Solicitar confirmación humana al instructor antes de modificar una política de calidad.

La evaluación se basa en precisión, trazabilidad, evidencia disponible, utilidad operativa y supervisión humana. No se requiere ni se debe solicitar exposición de razonamiento interno.

## Solución de Problemas

### Problema 1: La expectativa no aparece o el pipeline muestra un error de análisis SQL

**Síntomas**

- El pipeline falla antes de procesar datos.
- El mensaje indica error de sintaxis cerca de `CONSTRAINT`, `EXPECT` o `ON VIOLATION`.
- La interfaz no muestra las expectativas en `orders_silver`.

**Causa probable**

La cláusula de expectativas se colocó después de `AS SELECT`, se usó una columna que no existe en el resultado de la definición o se modificó el tipo de tabla sin conservar la sintaxis original de la Práctica 8.

**Corrección**

1. Coloque las expectativas entre el nombre de la tabla y `AS`.
2. Confirme el nombre y tipo de cada columna con:

   ```sql
   DESCRIBE orders_bronze;
   ```

3. Mantenga `CREATE LIVE TABLE` o `CREATE STREAMING LIVE TABLE` según la definición previa.
4. Guarde el archivo y vuelva a iniciar la actualización.

### Problema 2: El pipeline sigue fallando después de eliminar `zz_quality_fail.csv`

**Síntomas**

- El archivo ya no aparece en el volumen.
- El pipeline vuelve a reportar la infracción de `valid_order_id`.
- La actualización incremental no se recupera.

**Causa probable**

La fuente incremental conserva estado de procesamiento o checkpoint sobre el archivo detectado antes de que fuera eliminado.

**Corrección**

1. Confirme que el archivo fue eliminado:

   ```python
   display(dbutils.fs.ls(INPUT_PATH))
   ```

2. En el entorno Development del laboratorio, ejecute una actualización con **Full refresh**.
3. No ejecute Full refresh en un entorno productivo sin aprobación, análisis de impacto y revisión del costo.
4. Confirme que la actualización recuperada termina exitosamente y que `order_id_nulos = 0`.

## Limpieza

1. Elimine los archivos de prueba restantes para evitar que afecten prácticas posteriores:

   ```python
   for file_name in [
       "zz_quality_warn.csv",
       "zz_quality_drop.csv",
       "zz_quality_fail.csv"
   ]:
       path = f"{INPUT_PATH}/{file_name}"
       try:
           dbutils.fs.rm(path, True)
           print(f"Eliminado: {path}")
       except Exception as e:
           print(f"No eliminado o ya inexistente: {path}")
   ```

2. No elimine las tablas `orders_bronze` ni `orders_silver`, ya que pertenecen al pipeline y pueden ser necesarias en prácticas posteriores.

3. No elimine el pipeline `pipeline_orders_bronze_silver_b2`.

4. Conserve en sus notas:
   - El resultado de las tres expectativas.
   - La evidencia del fallo controlado.
   - La evidencia de la recuperación.
   - La propuesta de operación Production.

## Resumen

En esta práctica se aplicaron expectativas DLT sobre `orders_silver` con tres consecuencias operativas distintas:

- `WARN` permitió detectar estados fuera de dominio sin impedir la publicación del registro.
- `DROP ROW` evitó que pedidos con importes nulos o no positivos llegaran a Silver.
- `FAIL UPDATE` bloqueó una actualización al detectar un identificador de pedido nulo.

También se utilizaron las métricas de calidad, los errores de actualización y el event log para investigar el comportamiento del pipeline. En un entorno productivo, las reglas que protegen claves e integridad crítica deben bloquear la publicación mediante `FAIL UPDATE`, complementadas con alertas, auditoría y procedimientos de recuperación revisados por personas responsables.

Recursos oficiales:

- Expectativas de calidad en Lakeflow: https://docs.databricks.com/en/delta-live-tables/expectations.html
- Monitoreo y event log de pipelines: https://docs.databricks.com/en/delta-live-tables/observability.html
- Historial y Time Travel de Delta Lake: https://docs.databricks.com/en/delta/history.html
