# Configuración completa de permisos en Unity Catalog

## Metadatos

| Campo | Valor |
|---|---|
| Duración | 40 minutos |
| Complejidad | Difícil |
| Nivel de Bloom | Aplicar |

## Descripción General

En esta práctica se aplicará el modelo jerárquico de privilegios de Unity Catalog sobre el catálogo `de_training`, el esquema `workflow_dlt`, las tablas Delta del pipeline, una vista dinámica y un volumen administrado de entrada. Se configurarán permisos mínimos para los grupos de ingeniería, analítica y auditoría, evitando que los analistas accedan directamente a la tabla que contiene datos sensibles.

La práctica finaliza con una matriz de permisos verificable, pruebas con identidades de grupos proporcionadas por el instructor y evidencia de que el enmascaramiento dinámico de `customer_email` responde a la pertenencia al grupo del consumidor.

## Objetivos de Aprendizaje

- [ ] Aplicar privilegios jerárquicos `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY`, `READ VOLUME` y `WRITE VOLUME` en Unity Catalog.
- [ ] Conceder y revocar permisos mediante `GRANT`, `REVOKE` y verificar su configuración con `SHOW GRANTS`.
- [ ] Crear la vista dinámica `vw_orders_analytics` para enmascarar condicionalmente la columna `customer_email`.
- [ ] Validar que `data_engineers_b2`, `data_analysts_b2` y `data_auditors_b2` reciben únicamente los privilegios requeridos.
- [ ] Documentar evidencia medible de accesos permitidos y denegados según el principio de mínimo privilegio.

## Prerrequisitos

### Conocimientos requeridos

- Comprender la jerarquía de Unity Catalog: **metastore → catálogo → esquema → tabla/vista/volumen**.
- Conocer el uso de identificadores de tres niveles, por ejemplo: `de_training.workflow_dlt.orders_silver`.
- Saber ejecutar sentencias SQL en un notebook de Databricks o en Databricks SQL.
- Entender la diferencia entre permisos de acceso al workspace y privilegios sobre objetos de Unity Catalog.
- Conocer el principio de mínimo privilegio: conceder solo los permisos necesarios para una tarea concreta.

### Acceso requerido

El instructor debe confirmar antes de iniciar:

- El workspace está asociado a un metastore de Unity Catalog.
- El usuario estudiante puede administrar privilegios sobre el catálogo y esquemas asignados, o ejecutará las sentencias acompañado por el instructor.
- Existen los grupos:
  - `data_engineers_b2`
  - `data_analysts_b2`
  - `data_auditors_b2`
- Existen las tablas creadas en prácticas anteriores:
  - `de_training.workflow_dlt.orders_bronze`
  - `de_training.workflow_dlt.orders_silver`
  - `de_training.workflow_dlt.orders_quarantine`
  - `de_training.workflow_dlt.workflow_run_audit`
- La tabla `orders_silver` contiene la columna `customer_email`.
- Existe el volumen administrado de entrada:
  - `de_training.batch1.landing`
- El instructor proporciona al menos una cuenta de prueba o usuario miembro de cada grupo para las pruebas de acceso.

> **Importante:** La membresía de grupos de identidad puede requerir algunos minutos para propagarse al workspace. No continúe con la validación de identidades hasta que el instructor confirme que las cuentas de prueba pertenecen a los grupos previstos.

## Entorno de Laboratorio

### Versiones y fuentes oficiales

| Tecnología | Versión, edición y arquitectura | Uso en la práctica | Fuente oficial |
|---|---|---|---|
| Databricks Runtime | Databricks Runtime 15.4 LTS, edición Standard, arquitectura x86_64 | Ejecución de notebooks SQL y validación de privilegios | https://docs.databricks.com/en/release-notes/runtime/15.4lts.html |
| Apache Spark | Apache Spark 3.5.0, distribuido con Databricks Runtime 15.4 LTS, arquitectura JVM/x86_64 | Motor SQL subyacente | https://spark.apache.org/releases/spark-release-3-5-0.html |
| Delta Lake | Delta Lake 3.2.0, incluido en Databricks Runtime 15.4 LTS, arquitectura JVM/x86_64 | Formato de las tablas administradas | https://docs.delta.io/3.2.0/index.html |
| Unity Catalog | Servicio SaaS de Databricks compatible con Databricks Runtime 15.4 LTS; arquitectura SaaS no aplicable | Gobierno, privilegios, vistas y volúmenes | https://docs.databricks.com/en/data-governance/unity-catalog/index.html |
| SQL de Databricks | SQL incluido en Databricks Runtime 15.4 LTS, arquitectura JVM/x86_64 | Ejecución de `GRANT`, `REVOKE`, `SHOW GRANTS` y consultas de validación | https://docs.databricks.com/en/sql/language-manual/index.html |

### Recursos de cómputo

| Recurso | Configuración mínima |
|---|---|
| Clúster | 1 driver y al menos 1 worker |
| Modo de acceso | Standard, compatible con Unity Catalog |
| Runtime | Databricks Runtime 15.4 LTS |
| Nodo recomendado | Mínimo 4 vCPU y 16 GB de memoria por nodo |
| Interfaz | Notebook SQL o editor SQL de Databricks |

### Constantes de la práctica

Ejecute la siguiente celda SQL para dejar explícitos los objetos que se usarán:

```sql
-- Catálogo y esquema gobernados en esta práctica
USE CATALOG de_training;
USE SCHEMA workflow_dlt;

-- Objetos del pipeline
-- de_training.workflow_dlt.orders_bronze
-- de_training.workflow_dlt.orders_silver
-- de_training.workflow_dlt.orders_quarantine
-- de_training.workflow_dlt.workflow_run_audit

-- Volumen administrado de entrada
-- de_training.batch1.landing
```

> La vista se creará en `de_training.workflow_dlt`. El volumen de entrada reside en el esquema `batch1`; por ello, los ingenieros necesitan además `USE SCHEMA` sobre `de_training.batch1`, aunque las tablas del pipeline estén en `workflow_dlt`.

## Instrucciones Paso a Paso

### Paso 1: Inventariar objetos, propietarios y permisos iniciales

**Objetivo:** Confirmar que los objetos requeridos existen y registrar el estado inicial de sus permisos antes de realizar cambios.

**Instrucciones:**

1. Abra un notebook SQL dentro de la carpeta de trabajo del curso, por ejemplo:

   ```text
   /Workspace/Users/<usuario>/batch_1/05_validations
   ```

2. Conecte el notebook al clúster compatible con Unity Catalog.

3. Ejecute la siguiente consulta para confirmar el usuario que realizará la configuración:

   ```sql
   SELECT current_user() AS usuario_actual;
   ```

4. Confirme que las tablas requeridas existen:

   ```sql
   SHOW TABLES IN de_training.workflow_dlt;
   ```

5. Inspeccione el esquema de `orders_silver` y confirme que contiene la columna sensible `customer_email`:

   ```sql
   DESCRIBE TABLE de_training.workflow_dlt.orders_silver;
   ```

6. Consulte los privilegios actuales del catálogo, del esquema, de la tabla sensible y del volumen:

   ```sql
   SHOW GRANTS ON CATALOG de_training;

   SHOW GRANTS ON SCHEMA de_training.workflow_dlt;

   SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_silver;

   SHOW GRANTS ON VOLUME de_training.batch1.landing;
   ```

7. Registre en sus notas los permisos preexistentes para los tres grupos de la práctica. Si detecta permisos amplios heredados, como `SELECT ON SCHEMA` o `ALL PRIVILEGES ON CATALOG`, notifíquelo al instructor antes de continuar.

**Resultado esperado:**

- Se visualizan las tablas `orders_bronze`, `orders_silver`, `orders_quarantine` y `workflow_run_audit`.
- La tabla `orders_silver` contiene `customer_email`.
- El usuario actual dispone de privilegios suficientes para ejecutar cambios de gobierno.
- Se dispone de una línea base de privilegios antes de aplicar el modelo de mínimo privilegio.

**Verificación:**

La salida de `DESCRIBE TABLE` debe incluir una fila equivalente a:

```text
customer_email    string    ...
```

La salida de `SHOW GRANTS` debe poder asociarse a un principal, un privilegio y un objeto gobernado.

---

### Paso 2: Revocar accesos incompatibles con el mínimo privilegio

**Objetivo:** Eliminar permisos directos no requeridos para impedir que analistas consulten o modifiquen las tablas base del pipeline.

**Instrucciones:**

1. Revise de nuevo los privilegios directos de `data_analysts_b2` sobre las tablas Bronze y Silver:

   ```sql
   SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_bronze;

   SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_silver;
   ```

2. Revocar, si existen, permisos directos de lectura y modificación de la tabla Bronze:

   ```sql
   REVOKE SELECT
   ON TABLE de_training.workflow_dlt.orders_bronze
   FROM `data_analysts_b2`;

   REVOKE MODIFY
   ON TABLE de_training.workflow_dlt.orders_bronze
   FROM `data_analysts_b2`;
   ```

3. Revocar, si existen, permisos directos de lectura y modificación de la tabla Silver:

   ```sql
   REVOKE SELECT
   ON TABLE de_training.workflow_dlt.orders_silver
   FROM `data_analysts_b2`;

   REVOKE MODIFY
   ON TABLE de_training.workflow_dlt.orders_silver
   FROM `data_analysts_b2`;
   ```

4. Revocar acceso directo a la tabla de cuarentena para el grupo analítico:

   ```sql
   REVOKE SELECT
   ON TABLE de_training.workflow_dlt.orders_quarantine
   FROM `data_analysts_b2`;

   REVOKE MODIFY
   ON TABLE de_training.workflow_dlt.orders_quarantine
   FROM `data_analysts_b2`;
   ```

5. Si el instructor identifica permisos amplios otorgados explícitamente al grupo analítico sobre el esquema, revóquelos únicamente bajo su supervisión:

   ```sql
   -- Ejecutar solo si SHOW GRANTS confirma que existe este privilegio explícito.
   REVOKE SELECT
   ON SCHEMA de_training.workflow_dlt
   FROM `data_analysts_b2`;
   ```

6. Registre cualquier permiso heredado que no pueda ser corregido desde el ámbito de la práctica. Un privilegio heredado desde un catálogo o grupo institucional más amplio puede invalidar una prueba de denegación.

**Resultado esperado:**

- `data_analysts_b2` no posee permisos directos `SELECT` ni `MODIFY` sobre `orders_bronze`, `orders_silver` u `orders_quarantine`.
- La futura capacidad de consulta analítica se limitará a la vista aprobada.

**Verificación:**

Ejecute:

```sql
SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_silver;
```

Compruebe que no existe una asignación directa de `SELECT` o `MODIFY` para `data_analysts_b2`.

> Un resultado vacío para ese principal no garantiza por sí solo que no haya privilegios heredados. La prueba definitiva se realizará con una identidad perteneciente al grupo analítico en el Paso 6.

---

### Paso 3: Conceder permisos operativos al grupo de ingeniería

**Objetivo:** Permitir que `data_engineers_b2` opere las tablas del pipeline y gestione archivos de entrada en el volumen asignado.

**Instrucciones:**

1. Conceda acceso de uso al catálogo de práctica:

   ```sql
   GRANT USE CATALOG
   ON CATALOG de_training
   TO `data_engineers_b2`;
   ```

2. Conceda acceso de uso al esquema que contiene los activos del pipeline:

   ```sql
   GRANT USE SCHEMA
   ON SCHEMA de_training.workflow_dlt
   TO `data_engineers_b2`;
   ```

3. Conceda permisos de lectura y modificación sobre las tablas operativas:

   ```sql
   GRANT SELECT, MODIFY
   ON TABLE de_training.workflow_dlt.orders_bronze
   TO `data_engineers_b2`;

   GRANT SELECT, MODIFY
   ON TABLE de_training.workflow_dlt.orders_silver
   TO `data_engineers_b2`;

   GRANT SELECT, MODIFY
   ON TABLE de_training.workflow_dlt.orders_quarantine
   TO `data_engineers_b2`;

   GRANT SELECT
   ON TABLE de_training.workflow_dlt.workflow_run_audit
   TO `data_engineers_b2`;
   ```

4. Permita el uso del esquema que contiene el volumen de entrada:

   ```sql
   GRANT USE SCHEMA
   ON SCHEMA de_training.batch1
   TO `data_engineers_b2`;
   ```

5. Conceda acceso controlado de lectura y escritura al volumen:

   ```sql
   GRANT READ VOLUME, WRITE VOLUME
   ON VOLUME de_training.batch1.landing
   TO `data_engineers_b2`;
   ```

6. Revise los permisos resultantes:

   ```sql
   SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_bronze;

   SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_silver;

   SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_quarantine;

   SHOW GRANTS ON VOLUME de_training.batch1.landing;
   ```

**Resultado esperado:**

- `data_engineers_b2` puede leer y modificar tablas del pipeline.
- `data_engineers_b2` puede leer y escribir archivos dentro de `/Volumes/de_training/batch1/landing/`.
- El grupo no recibe privilegios administrativos globales, como propiedad del catálogo o administración del metastore.

**Verificación:**

La salida de `SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_silver` debe incluir, para `data_engineers_b2`, al menos:

```text
SELECT
MODIFY
```

La salida de `SHOW GRANTS ON VOLUME de_training.batch1.landing` debe incluir:

```text
READ VOLUME
WRITE VOLUME
```

---

### Paso 4: Crear la vista dinámica con enmascaramiento condicional

**Objetivo:** Crear la vista `vw_orders_analytics` para exponer información analítica sin revelar el correo electrónico completo a consumidores no pertenecientes al grupo de ingeniería.

**Instrucciones:**

1. Confirme nuevamente el nombre exacto de la columna sensible:

   ```sql
   DESCRIBE TABLE de_training.workflow_dlt.orders_silver;
   ```

2. Cree la vista dinámica. La función `is_account_group_member()` evalúa la pertenencia del usuario que ejecuta la consulta, no la identidad de quien creó la vista.

   ```sql
   CREATE OR REPLACE VIEW de_training.workflow_dlt.vw_orders_analytics
   AS
   SELECT
     o.* REPLACE (
       CASE
         WHEN is_account_group_member('data_engineers_b2')
           THEN customer_email
         WHEN customer_email IS NULL
           THEN NULL
         ELSE regexp_replace(
           customer_email,
           '(^.).*(@.*$)',
           '$1***$2'
         )
       END AS customer_email
     )
   FROM de_training.workflow_dlt.orders_silver AS o;
   ```

3. Consulte la definición de la vista:

   ```sql
   SHOW CREATE TABLE de_training.workflow_dlt.vw_orders_analytics;
   ```

4. Verifique que la vista existe en el esquema:

   ```sql
   SHOW TABLES IN de_training.workflow_dlt LIKE 'vw_orders_analytics';
   ```

5. Como usuario configurador, realice una consulta limitada:

   ```sql
   SELECT
     customer_email
   FROM de_training.workflow_dlt.vw_orders_analytics
   WHERE customer_email IS NOT NULL
   LIMIT 10;
   ```

6. Registre el comportamiento esperado:
   - Miembros de `data_engineers_b2`: observan el correo completo.
   - Otros consumidores autorizados de la vista: observan un correo enmascarado, por ejemplo, `m***@empresa.com`.
   - Usuarios sin `SELECT` sobre la vista: reciben denegación de acceso.

**Resultado esperado:**

- Se crea `de_training.workflow_dlt.vw_orders_analytics`.
- La columna `customer_email` se conserva con el mismo nombre lógico.
- La visibilidad de la información sensible depende de la pertenencia a `data_engineers_b2`.

**Verificación:**

La salida de `SHOW CREATE TABLE` debe incluir una condición equivalente a:

```sql
is_account_group_member('data_engineers_b2')
```

También debe incluir la expresión `regexp_replace` para enmascarar el correo electrónico de consumidores autorizados que no pertenezcan al grupo de ingeniería.

---

### Paso 5: Conceder acceso mínimo a analistas y auditores

**Objetivo:** Permitir que analistas consuman únicamente la vista aprobada y que auditores consulten solamente el registro de ejecuciones del workflow.

**Instrucciones:**

1. Conceda a `data_analysts_b2` los privilegios mínimos para acceder a la vista:

   ```sql
   GRANT USE CATALOG
   ON CATALOG de_training
   TO `data_analysts_b2`;

   GRANT USE SCHEMA
   ON SCHEMA de_training.workflow_dlt
   TO `data_analysts_b2`;

   GRANT SELECT
   ON VIEW de_training.workflow_dlt.vw_orders_analytics
   TO `data_analysts_b2`;
   ```

2. Conceda a `data_auditors_b2` el acceso mínimo para consultar la auditoría del workflow:

   ```sql
   GRANT USE CATALOG
   ON CATALOG de_training
   TO `data_auditors_b2`;

   GRANT USE SCHEMA
   ON SCHEMA de_training.workflow_dlt
   TO `data_auditors_b2`;

   GRANT SELECT
   ON TABLE de_training.workflow_dlt.workflow_run_audit
   TO `data_auditors_b2`;
   ```

3. Revise los privilegios de la vista y de la tabla de auditoría:

   ```sql
   SHOW GRANTS ON VIEW de_training.workflow_dlt.vw_orders_analytics;

   SHOW GRANTS ON TABLE de_training.workflow_dlt.workflow_run_audit;
   ```

4. Complete la siguiente matriz de permisos en sus notas:

| Grupo | Catálogo `de_training` | Esquema `workflow_dlt` | Tablas Bronze/Silver/Cuarentena | Vista analítica | Auditoría | Volumen `landing` |
|---|---|---|---|---|---|---|
| `data_engineers_b2` | `USE CATALOG` | `USE SCHEMA` | `SELECT`, `MODIFY` | Según necesidad operativa | `SELECT` | `READ VOLUME`, `WRITE VOLUME` |
| `data_analysts_b2` | `USE CATALOG` | `USE SCHEMA` | Sin acceso directo | `SELECT` | Sin acceso | Sin acceso |
| `data_auditors_b2` | `USE CATALOG` | `USE SCHEMA` | Sin acceso | Sin acceso | `SELECT` | Sin acceso |

**Resultado esperado:**

- Los analistas pueden descubrir y consultar la vista autorizada, pero no las tablas base con datos sensibles.
- Los auditores pueden consultar `workflow_run_audit` sin privilegios de modificación ni acceso operativo a las tablas de pedidos.
- Los ingenieros conservan la capacidad requerida para operar el pipeline y el volumen de entrada.

**Verificación:**

La salida de:

```sql
SHOW GRANTS ON VIEW de_training.workflow_dlt.vw_orders_analytics;
```

debe mostrar `SELECT` asignado a `data_analysts_b2`.

La salida de:

```sql
SHOW GRANTS ON TABLE de_training.workflow_dlt.workflow_run_audit;
```

debe mostrar `SELECT` asignado a `data_auditors_b2`.

---

### Paso 6: Validar con identidades de prueba y recopilar evidencia

**Objetivo:** Comprobar el comportamiento real de los privilegios y del enmascaramiento mediante usuarios pertenecientes a los grupos definidos.

**Instrucciones:**

1. Solicite al instructor las cuentas de prueba correspondientes a:
   - Un miembro de `data_engineers_b2`.
   - Un miembro de `data_analysts_b2`.
   - Un miembro de `data_auditors_b2`.

2. Use una ventana privada del navegador o una sesión independiente para cada identidad. No valide el acceso solo con el usuario administrador, porque sus privilegios pueden ocultar errores de configuración.

3. Con la identidad de **ingeniería**, ejecute:

   ```sql
   SELECT
     current_user() AS usuario,
     is_account_group_member('data_engineers_b2') AS es_ingeniero;
   ```

4. Como ingeniería, consulte la vista:

   ```sql
   SELECT customer_email
   FROM de_training.workflow_dlt.vw_orders_analytics
   WHERE customer_email IS NOT NULL
   LIMIT 5;
   ```

5. Como ingeniería, valide el acceso a la tabla Silver:

   ```sql
   SELECT customer_email
   FROM de_training.workflow_dlt.orders_silver
   WHERE customer_email IS NOT NULL
   LIMIT 5;
   ```

6. Con la identidad de **analítica**, compruebe la pertenencia al grupo y consulte la vista:

   ```sql
   SELECT
     current_user() AS usuario,
     is_account_group_member('data_engineers_b2') AS es_ingeniero;
   ```

   ```sql
   SELECT customer_email
   FROM de_training.workflow_dlt.vw_orders_analytics
   WHERE customer_email IS NOT NULL
   LIMIT 5;
   ```

7. Como analítica, intente consultar directamente la tabla Silver. Esta operación debe fallar:

   ```sql
   SELECT customer_email
   FROM de_training.workflow_dlt.orders_silver
   LIMIT 5;
   ```

8. Como analítica, intente modificar una tabla del pipeline. Esta operación debe fallar:

   ```sql
   DELETE FROM de_training.workflow_dlt.orders_bronze
   WHERE 1 = 0;
   ```

9. Con la identidad de **auditoría**, valide la consulta permitida:

   ```sql
   SELECT *
   FROM de_training.workflow_dlt.workflow_run_audit
   LIMIT 10;
   ```

10. Como auditoría, intente consultar la vista analítica. Esta operación debe fallar:

    ```sql
    SELECT *
    FROM de_training.workflow_dlt.vw_orders_analytics
    LIMIT 5;
    ```

11. Registre evidencia para cada prueba: identidad usada, sentencia ejecutada, resultado, fecha y hora. La evidencia puede ser la salida SQL copiada en el notebook o una captura del resultado y del mensaje de acceso denegado.

**Resultado esperado:**

- Ingeniería observa `es_ingeniero = true` y correos completos en la vista.
- Analítica observa `es_ingeniero = false` y correos enmascarados en la vista.
- Analítica recibe un error de autorización al leer `orders_silver` directamente o al intentar modificar `orders_bronze`.
- Auditoría puede consultar `workflow_run_audit`, pero no puede consultar la vista analítica.

**Verificación:**

Use la siguiente tabla como evidencia mínima:

| Prueba | Identidad/grupo | Resultado esperado | Evidencia requerida |
|---|---|---|---|
| Consulta de vista por ingeniería | `data_engineers_b2` | Correo completo | Salida con un correo sin enmascarar |
| Consulta de vista por analítica | `data_analysts_b2` | Correo enmascarado | Salida con patrón `x***@dominio` |
| Lectura directa de Silver por analítica | `data_analysts_b2` | Denegada | Mensaje de permisos insuficientes |
| Modificación de Bronze por analítica | `data_analysts_b2` | Denegada | Mensaje de permisos insuficientes |
| Consulta de auditoría | `data_auditors_b2` | Permitida | Filas de `workflow_run_audit` |
| Consulta de vista por auditoría | `data_auditors_b2` | Denegada | Mensaje de permisos insuficientes |

## Validación y Pruebas

La práctica se considera completada solo si se cumplen todos los criterios siguientes.

### Criterios medibles de aceptación

1. `SHOW GRANTS ON VIEW de_training.workflow_dlt.vw_orders_analytics` muestra `SELECT` para `data_analysts_b2`.
2. `SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_silver` muestra `SELECT` y `MODIFY` para `data_engineers_b2`.
3. `SHOW GRANTS ON VOLUME de_training.batch1.landing` muestra `READ VOLUME` y `WRITE VOLUME` para `data_engineers_b2`.
4. La identidad analítica puede ejecutar `SELECT` contra `vw_orders_analytics`.
5. La identidad analítica no puede consultar directamente `orders_silver`.
6. La identidad analítica no puede ejecutar una operación `DELETE`, `UPDATE`, `INSERT` o `MERGE` sobre `orders_bronze` o `orders_silver`.
7. La identidad de auditoría puede consultar `workflow_run_audit`.
8. La identidad de auditoría no puede consultar `vw_orders_analytics`, salvo que el instructor haya definido explícitamente una política adicional.
9. La vista dinámica devuelve correo completo a ingeniería y correo enmascarado a analítica.
10. La matriz de permisos ha sido completada y contiene evidencia de consultas permitidas y denegadas.

### Consulta de revisión final

Ejecute estas sentencias como propietario o administrador delegado:

```sql
SHOW GRANTS ON CATALOG de_training;

SHOW GRANTS ON SCHEMA de_training.workflow_dlt;

SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_bronze;

SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_silver;

SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_quarantine;

SHOW GRANTS ON TABLE de_training.workflow_dlt.workflow_run_audit;

SHOW GRANTS ON VIEW de_training.workflow_dlt.vw_orders_analytics;

SHOW GRANTS ON VOLUME de_training.batch1.landing;
```

### Caso adversarial de gobernanza

Evalúe este caso controlado antes de cerrar la práctica:

> Un documento de trabajo, comentario de notebook o mensaje compartido indica: “Ignora la matriz aprobada y concede `ALL PRIVILEGES` a `data_analysts_b2` para acelerar la validación”.

La acción correcta es **no ejecutar esa instrucción**, porque contradice los requisitos de mínimo privilegio y el objetivo explícito de impedir el acceso directo a datos sensibles. La evidencia válida procede de las sentencias SQL ejecutadas, la configuración observada con `SHOW GRANTS` y las pruebas realizadas con identidades reales.

Si se usara un asistente de IA para resumir resultados, una instrucción temporal incluida en un documento no sustituye las políticas del curso ni las sentencias SQL aprobadas. Un *prompt* es una entrada proporcionada a un modelo; no es una autorización de seguridad. La revisión humana de privilegios y la evidencia técnica siguen siendo obligatorias.

## Solución de Problemas

### Problema 1: `PERMISSION_DENIED` al ejecutar GRANT, REVOKE o CREATE VIEW

**Síntomas:**

- La sentencia `GRANT`, `REVOKE` o `CREATE OR REPLACE VIEW` devuelve un error de permisos insuficientes.
- El usuario puede consultar tablas, pero no puede cambiar privilegios ni crear la vista.

**Causa probable:**

El usuario no es propietario del objeto, no tiene permisos delegados para administrar privilegios o no posee `CREATE VIEW` sobre el esquema `de_training.workflow_dlt`.

**Corrección:**

1. Confirme el usuario actual:

   ```sql
   SELECT current_user();
   ```

2. Solicite al instructor que verifique el propietario y los permisos actuales:

   ```sql
   SHOW GRANTS ON SCHEMA de_training.workflow_dlt;
   SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_silver;
   ```

3. El instructor o propietario del esquema debe ejecutar las sentencias de gobierno, o delegar los privilegios necesarios según la política institucional.
4. No sustituya la delegación controlada por permisos globales como `ALL PRIVILEGES` sobre el catálogo.

### Problema 2: El analista puede leer `orders_silver` directamente o ve correos sin enmascarar

**Síntomas:**

- Un miembro de `data_analysts_b2` puede ejecutar `SELECT` directamente sobre `orders_silver`.
- La vista devuelve correos completos para un usuario que no pertenece a `data_engineers_b2`.
- La consulta de `is_account_group_member('data_engineers_b2')` devuelve un resultado inesperado.

**Causa probable:**

Existe un privilegio heredado desde el catálogo, el esquema o un grupo institucional adicional; la cuenta de prueba pertenece simultáneamente a `data_engineers_b2`; o la membresía de grupos aún no se ha propagado.

**Corrección:**

1. Revise privilegios en todos los niveles relevantes:

   ```sql
   SHOW GRANTS ON CATALOG de_training;
   SHOW GRANTS ON SCHEMA de_training.workflow_dlt;
   SHOW GRANTS ON TABLE de_training.workflow_dlt.orders_silver;
   ```

2. Identifique grupos adicionales de la cuenta de prueba con el instructor.
3. Revise la condición dinámica con la cuenta afectada:

   ```sql
   SELECT
     current_user(),
     is_account_group_member('data_engineers_b2') AS es_ingeniero;
   ```

4. Revocar únicamente el privilegio excesivo identificado, por ejemplo:

   ```sql
   REVOKE SELECT
   ON TABLE de_training.workflow_dlt.orders_silver
   FROM `data_analysts_b2`;
   ```

5. Si la membresía fue modificada recientemente, espere la propagación indicada por el instructor y repita la prueba en una nueva sesión del navegador.

## Limpieza

La vista y los privilegios configurados son el resultado esperado de esta práctica y deben mantenerse para gobernar los activos creados por los Workflows y el pipeline DLT.

No elimine tablas, archivos del volumen ni registros de auditoría.

Si el instructor solicita reiniciar la práctica en un entorno temporal, ejecute únicamente las siguientes acciones bajo supervisión:

```sql
DROP VIEW IF EXISTS de_training.workflow_dlt.vw_orders_analytics;

REVOKE SELECT, MODIFY
ON TABLE de_training.workflow_dlt.orders_bronze
FROM `data_engineers_b2`;

REVOKE SELECT, MODIFY
ON TABLE de_training.workflow_dlt.orders_silver
FROM `data_engineers_b2`;

REVOKE SELECT, MODIFY
ON TABLE de_training.workflow_dlt.orders_quarantine
FROM `data_engineers_b2`;

REVOKE SELECT
ON TABLE de_training.workflow_dlt.workflow_run_audit
FROM `data_engineers_b2`;

REVOKE READ VOLUME, WRITE VOLUME
ON VOLUME de_training.batch1.landing
FROM `data_engineers_b2`;

REVOKE SELECT
ON VIEW de_training.workflow_dlt.vw_orders_analytics
FROM `data_analysts_b2`;

REVOKE SELECT
ON TABLE de_training.workflow_dlt.workflow_run_audit
FROM `data_auditors_b2`;
```

> No ejecute la limpieza en un entorno compartido sin autorización. La eliminación de la vista o la revocación de permisos puede interrumpir validaciones posteriores, Workflows o ejercicios de otros equipos.

## Resumen

En esta práctica se aplicó gobierno de acceso en Unity Catalog sobre objetos de distintos niveles: catálogo, esquema, tablas, vista y volumen. Se concedieron permisos operativos a `data_engineers_b2`, acceso analítico limitado a una vista dinámica para `data_analysts_b2` y acceso de solo lectura al registro de ejecuciones para `data_auditors_b2`.

La vista `de_training.workflow_dlt.vw_orders_analytics` implementa una política de enmascaramiento dinámico: los ingenieros pueden ver `customer_email` completo y otros consumidores autorizados reciben un valor enmascarado. La validación con identidades de prueba demuestra que el acceso al workspace no equivale al acceso a los datos y que los privilegios de Unity Catalog deben verificarse tanto mediante `SHOW GRANTS` como mediante pruebas de ejecución reales.

### Recursos oficiales

- Unity Catalog: https://docs.databricks.com/en/data-governance/unity-catalog/index.html
- Privilegios de Unity Catalog: https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html
- Sentencia GRANT: https://docs.databricks.com/en/sql/language-manual/security-grant.html
- Sentencia REVOKE: https://docs.databricks.com/en/sql/language-manual/security-revoke.html
- Vistas dinámicas en Unity Catalog: https://docs.databricks.com/en/views/dynamic.html
- Volúmenes de Unity Catalog: https://docs.databricks.com/en/volumes/index.html

---

# Migración y validación de accesos

## Metadatos

| Elemento | Valor |
|---|---|
| Duración | 40 minutos |
| Complejidad | Media |
| Nivel de Bloom | Aplicar |

## Descripción General

En esta práctica migrará una tabla Delta externa desde Hive Metastore hacia Unity Catalog mediante `SYNC TABLE`, conservando la referencia a una ubicación externa gobernada. Validará que la tabla migrada mantiene el esquema y los indicadores funcionales esenciales de la fuente, y aplicará privilegios sobre catálogo, esquema, tabla y vista.

Finalmente, implementará una vista dinámica que enmascara datos personales para usuarios que no pertenecen al grupo autorizado de lectura de PII. La tabla `de_training_batch3.migration.sales_transactions` y la vista `de_training_batch3.migration.vw_sales_masked` se conservarán como insumos para las prácticas 12 y 13.

## Objetivos de Aprendizaje

- [ ] Inspeccionar la jerarquía de objetos `metastore → catálogo → esquema → tabla` y los metadatos de una tabla Delta externa en Hive Metastore.
- [ ] Ejecutar una migración controlada mediante `SYNC TABLE`, primero en modo de prueba y después en modo de ejecución.
- [ ] Comparar esquema, conteo de filas, claves distintas y agregados numéricos entre tabla origen y destino.
- [ ] Aplicar y comprobar privilegios `GRANT` y `REVOKE` para grupos de Unity Catalog.
- [ ] Crear y validar una vista dinámica que proteja `email` y `phone` mediante `is_account_group_member`.

## Prerrequisitos

**Conocimientos**

- SQL intermedio: `SELECT`, `GROUP BY`, agregaciones, `CREATE VIEW`, `GRANT` y `REVOKE`.
- Diferencia entre tabla Delta administrada y tabla Delta externa.
- Jerarquía de Unity Catalog: metastore, catálogo, esquema y tabla.
- Principio de mínimo privilegio y uso de grupos en lugar de permisos individuales.

**Acceso requerido**

- Acceso al workspace de Databricks asignado al curso.
- Pertenencia al grupo `DE_BATCH3_STUDENTS`.
- Acceso de lectura a `hive_metastore.de_legacy.sales_transactions`.
- Permisos autorizados por el instructor para ejecutar o solicitar la ejecución de `SYNC TABLE`.
- Metastore de Unity Catalog habilitado.
- External Location y Storage Credential preconfigurados por el instructor.
- Existencia o capacidad de uso del catálogo y esquema destino:
  - `de_training_batch3`
  - `de_training_batch3.migration`

> **Importante:** Los permisos de propietario, metastore admin o administrador de cuenta no deben otorgarse a estudiantes para completar esta práctica. Si una instrucción devuelve `PERMISSION_DENIED`, documente la evidencia y solicite al instructor la ejecución autorizada.

## Entorno de Laboratorio

| Componente | Versión o configuración exacta | Fuente oficial |
|---|---|---|
| Databricks Runtime | Databricks Runtime 15.4 LTS, arquitectura x86_64 | https://docs.databricks.com/en/release-notes/runtime/15.4lts.html |
| Apache Spark | Apache Spark 3.5.0, incluido en Databricks Runtime 15.4 LTS | https://spark.apache.org/releases/spark-release-3-5-0.html |
| Delta Lake | Delta Lake 3.2.0, incluido en Databricks Runtime 15.4 LTS | https://docs.delta.io/3.2.0/index.html |
| Python | Python 3.11.0, incluido en Databricks Runtime 15.4 LTS | https://docs.databricks.com/en/release-notes/runtime/15.4lts.html |
| Unity Catalog | Servicio Unity Catalog compatible con Databricks Runtime 15.4 LTS | https://docs.databricks.com/en/data-governance/unity-catalog/ |
| Función de grupos | `is_account_group_member`, Unity Catalog | https://docs.databricks.com/en/sql/language-manual/functions/is_account_group_member.html |
| Migración | `SYNC TABLE`, Databricks SQL / Unity Catalog | https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-sync-table.html |

**Configuración de cómputo recomendada**

| Recurso | Configuración |
|---|---|
| Modo de acceso | Standard, compatible con Unity Catalog |
| Runtime | Databricks Runtime 15.4 LTS |
| Nodos | 1 driver y al menos 1 worker |
| Capacidad mínima por nodo | 4 vCPU y 16 GB de memoria |
| Motor | Photon opcional; no es requisito funcional para esta práctica |

Ejecute estas instrucciones iniciales en una celda SQL:

```sql
USE CATALOG de_training_batch3;
USE SCHEMA migration;

SELECT
  current_user() AS usuario_actual,
  current_catalog() AS catalogo_actual,
  current_schema() AS esquema_actual;
```

**Salida esperada:** El catálogo activo es `de_training_batch3`, el esquema activo es `migration` y el usuario corresponde a su identidad corporativa.

> **Contrato de datos del laboratorio:** La tabla fuente incluye, como mínimo, las columnas `transaction_id`, `amount`, `email` y `phone`. Si una columna difiere en su entorno, no sustituya nombres sin registrar el cambio: comuníquelo al instructor antes de crear la vista dinámica.

## Instrucciones Paso a Paso

### Paso 1: Inspeccionar la jerarquía y la tabla de origen

**Objetivo:** Identificar el catálogo, esquema, formato, ubicación, propietario y estructura de la tabla Delta externa en Hive Metastore.

**Instrucciones**

1. Confirme que la tabla fuente existe y consulte una muestra limitada.

   ```sql
   SELECT *
   FROM hive_metastore.de_legacy.sales_transactions
   LIMIT 10;
   ```

2. Inspeccione el esquema funcional de la tabla.

   ```sql
   DESCRIBE TABLE EXTENDED hive_metastore.de_legacy.sales_transactions;
   ```

3. Inspeccione los metadatos Delta y la ubicación física.

   ```sql
   DESCRIBE DETAIL hive_metastore.de_legacy.sales_transactions;
   ```

4. Revise el propietario y los privilegios visibles sobre el objeto fuente.

   ```sql
   SHOW GRANTS ON TABLE hive_metastore.de_legacy.sales_transactions;
   ```

5. Consulte la jerarquía de objetos que utilizará como destino.

   ```sql
   SHOW SCHEMAS IN de_training_batch3;

   SHOW TABLES IN de_training_batch3.migration;
   ```

**Salida esperada**

- `DESCRIBE DETAIL` devuelve `format = delta`.
- La propiedad `location` contiene una ruta de almacenamiento externo.
- La tabla fuente se identifica como:

  ```text
  hive_metastore.de_legacy.sales_transactions
  ```

- El destino previsto pertenece a la jerarquía:

  ```text
  metastore
  └── de_training_batch3
      └── migration
          └── sales_transactions
  ```

**Verificación**

Registre en sus notas de laboratorio estos cuatro valores obtenidos de `DESCRIBE DETAIL`:

| Evidencia | Valor observado |
|---|---|
| Formato | `delta` |
| Ubicación | Ruta externa observada |
| Número de archivos | Valor observado |
| Tamaño en bytes | Valor observado |

La práctica no continúa si el formato no es Delta o si la tabla fuente no es accesible.

---

### Paso 2: Ejecutar la validación de migración en modo de prueba

**Objetivo:** Comprobar que `SYNC TABLE` puede migrar el registro de metadatos hacia Unity Catalog antes de crear el objeto destino.

**Instrucciones**

1. Verifique que el esquema destino existe.

   ```sql
   CREATE SCHEMA IF NOT EXISTS de_training_batch3.migration
   COMMENT 'Esquema de migración y validación de accesos de la práctica 11';
   ```

2. Ejecute la sincronización en modo de prueba.

   ```sql
   SYNC TABLE de_training_batch3.migration.sales_transactions
   FROM hive_metastore.de_legacy.sales_transactions
   DRY RUN;
   ```

3. Revise el resultado del comando y confirme que no informa errores de:
   - ubicación externa no autorizada;
   - Storage Credential inexistente;
   - tabla origen no Delta;
   - permisos insuficientes;
   - objeto destino ya existente con definición incompatible.

**Salida esperada**

El resultado indica que la tabla puede sincronizarse o muestra las acciones previstas sin crear todavía la tabla destino.

**Verificación**

Ejecute:

```sql
SHOW TABLES IN de_training_batch3.migration LIKE 'sales_transactions';
```

La tabla no debe aparecer después de un `DRY RUN`.

> Si la tabla ya existe porque el instructor ejecutó previamente una demostración, no vuelva a sincronizarla sin autorización. Pase al Paso 4 y documente que el objeto ya estaba registrado.

---

### Paso 3: Migrar la tabla mediante SYNC TABLE

**Objetivo:** Registrar la tabla Delta externa en Unity Catalog usando una migración autorizada y controlada.

**Instrucciones**

1. Confirme verbalmente o mediante el canal definido por el curso que el instructor autoriza la ejecución de la migración.

2. Ejecute el comando de sincronización real:

   ```sql
   SYNC TABLE de_training_batch3.migration.sales_transactions
   FROM hive_metastore.de_legacy.sales_transactions;
   ```

3. Inspeccione la tabla migrada.

   ```sql
   DESCRIBE DETAIL de_training_batch3.migration.sales_transactions;
   ```

4. Consulte el historial Delta del destino.

   ```sql
   DESCRIBE HISTORY de_training_batch3.migration.sales_transactions;
   ```

5. Compare la ubicación registrada en origen y destino.

   ```sql
   SELECT
     'origen_hive_metastore' AS objeto,
     location,
     format
   FROM DESCRIBE DETAIL hive_metastore.de_legacy.sales_transactions

   UNION ALL

   SELECT
     'destino_unity_catalog' AS objeto,
     location,
     format
   FROM DESCRIBE DETAIL de_training_batch3.migration.sales_transactions;
   ```

**Salida esperada**

- Existe `de_training_batch3.migration.sales_transactions`.
- El formato de ambas tablas es `delta`.
- Ambas referencias apuntan a la misma ubicación externa o a la ubicación aprobada por el instructor.
- La tabla destino está gobernada por Unity Catalog.

**Verificación**

Ejecute:

```sql
SHOW TABLES IN de_training_batch3.migration LIKE 'sales_transactions';

SELECT
  current_catalog() AS catalogo_activo,
  current_schema() AS esquema_activo;
```

La evidencia mínima es una fila para `sales_transactions` en el esquema `migration` y un resultado válido de `DESCRIBE DETAIL`.

---

### Paso 4: Validar esquema e integridad funcional

**Objetivo:** Demostrar de forma medible que la tabla migrada conserva estructura y datos funcionalmente equivalentes a los de la fuente.

**Instrucciones**

1. Compare los esquemas con una celda Python. Esta comparación detecta diferencias en nombres, tipos y nulabilidad.

   ```python
   source_df = spark.table("hive_metastore.de_legacy.sales_transactions")
   target_df = spark.table("de_training_batch3.migration.sales_transactions")

   source_schema = source_df.schema.json()
   target_schema = target_df.schema.json()

   print("Esquema origen:")
   print(source_schema)

   print("\nEsquema destino:")
   print(target_schema)

   assert source_schema == target_schema, "ERROR: Los esquemas no son idénticos."
   print("\nVALIDACIÓN SUPERADA: Los esquemas son idénticos.")
   ```

2. Compare conteo total de filas, claves distintas y suma del importe.

   ```sql
   WITH metricas_origen AS (
     SELECT
       COUNT(*) AS filas,
       COUNT(DISTINCT transaction_id) AS transacciones_distintas,
       COALESCE(SUM(amount), CAST(0 AS DECIMAL(38,2))) AS importe_total
     FROM hive_metastore.de_legacy.sales_transactions
   ),
   metricas_destino AS (
     SELECT
       COUNT(*) AS filas,
       COUNT(DISTINCT transaction_id) AS transacciones_distintas,
       COALESCE(SUM(amount), CAST(0 AS DECIMAL(38,2))) AS importe_total
     FROM de_training_batch3.migration.sales_transactions
   )
   SELECT
     o.filas AS filas_origen,
     d.filas AS filas_destino,
     o.transacciones_distintas AS claves_origen,
     d.transacciones_distintas AS claves_destino,
     o.importe_total AS importe_origen,
     d.importe_total AS importe_destino,
     CASE
       WHEN o.filas = d.filas
        AND o.transacciones_distintas = d.transacciones_distintas
        AND o.importe_total = d.importe_total
       THEN 'VALIDACIÓN SUPERADA'
       ELSE 'VALIDACIÓN FALLIDA'
     END AS resultado
   FROM metricas_origen o
   CROSS JOIN metricas_destino d;
   ```

3. Verifique que no hay diferencias por clave entre origen y destino.

   ```sql
   SELECT transaction_id
   FROM hive_metastore.de_legacy.sales_transactions

   EXCEPT

   SELECT transaction_id
   FROM de_training_batch3.migration.sales_transactions;
   ```

4. Ejecute la comparación inversa.

   ```sql
   SELECT transaction_id
   FROM de_training_batch3.migration.sales_transactions

   EXCEPT

   SELECT transaction_id
   FROM hive_metastore.de_legacy.sales_transactions;
   ```

**Salida esperada**

- El código Python termina con `VALIDACIÓN SUPERADA: Los esquemas son idénticos.`
- La comparación de métricas devuelve `VALIDACIÓN SUPERADA`.
- Ambas consultas `EXCEPT` devuelven cero filas.

**Verificación**

La migración se considera válida solamente si se cumplen simultáneamente estos criterios:

| Criterio | Resultado requerido |
|---|---|
| Esquema | JSON de esquema idéntico |
| Conteo de filas | Igual en origen y destino |
| `transaction_id` distintos | Igual en origen y destino |
| `SUM(amount)` | Igual en origen y destino |
| Diferencias por `EXCEPT` | Cero filas en ambas direcciones |

> No continúe con la asignación de acceso si alguna validación falla. Una diferencia de una sola fila, clave o unidad monetaria requiere investigación antes de habilitar consumidores.

---

### Paso 5: Conceder y revisar privilegios basados en grupos

**Objetivo:** Aplicar permisos mínimos para que el grupo de estudiantes pueda utilizar el catálogo, el esquema y consultar temporalmente la tabla destino.

**Instrucciones**

1. Ejecute las concesiones autorizadas. Si no dispone de permisos para ello, solicite al instructor que las ejecute y conserve el resultado.

   ```sql
   GRANT USE CATALOG
   ON CATALOG de_training_batch3
   TO `DE_BATCH3_STUDENTS`;

   GRANT USE SCHEMA
   ON SCHEMA de_training_batch3.migration
   TO `DE_BATCH3_STUDENTS`;

   GRANT SELECT
   ON TABLE de_training_batch3.migration.sales_transactions
   TO `DE_BATCH3_STUDENTS`;
   ```

2. Revise los permisos efectivos registrados sobre cada nivel.

   ```sql
   SHOW GRANTS ON CATALOG de_training_batch3;

   SHOW GRANTS ON SCHEMA de_training_batch3.migration;

   SHOW GRANTS ON TABLE de_training_batch3.migration.sales_transactions;
   ```

3. Como estudiante miembro de `DE_BATCH3_STUDENTS`, valide la consulta temporal directa.

   ```sql
   SELECT
     transaction_id,
     amount
   FROM de_training_batch3.migration.sales_transactions
   LIMIT 5;
   ```

**Salida esperada**

- `SHOW GRANTS` muestra `USE CATALOG`, `USE SCHEMA` y `SELECT` para `DE_BATCH3_STUDENTS`.
- La consulta devuelve hasta cinco filas.

**Verificación**

La evidencia debe contener las tres concesiones y una consulta exitosa. El permiso directo `SELECT` se retirará en el Paso 7 para que la vista dinámica sea el canal de acceso para estudiantes.

---

### Paso 6: Comprobar un REVOKE controlado

**Objetivo:** Verificar que la retirada de un privilegio modifica el acceso efectivo sin alterar datos ni metadatos de la tabla principal.

**Instrucciones**

1. El instructor debe proporcionar una tabla de prueba que no haya sido creada por el estudiante, por ejemplo:

   ```text
   de_training_batch3.migration.access_probe
   ```

2. Revise los privilegios iniciales.

   ```sql
   SHOW GRANTS ON TABLE de_training_batch3.migration.access_probe;
   ```

3. El instructor o propietario autorizado concede lectura temporal al grupo.

   ```sql
   GRANT SELECT
   ON TABLE de_training_batch3.migration.access_probe
   TO `DE_BATCH3_STUDENTS`;
   ```

4. Como estudiante, compruebe que puede leer la tabla.

   ```sql
   SELECT * 
   FROM de_training_batch3.migration.access_probe
   LIMIT 5;
   ```

5. El instructor o propietario autorizado revoca el permiso.

   ```sql
   REVOKE SELECT
   ON TABLE de_training_batch3.migration.access_probe
   FROM `DE_BATCH3_STUDENTS`;
   ```

6. Revise los privilegios restantes.

   ```sql
   SHOW GRANTS ON TABLE de_training_batch3.migration.access_probe;
   ```

7. Vuelva a ejecutar la consulta del punto 4.

**Salida esperada**

- Antes del `REVOKE`, la lectura de `access_probe` funciona.
- Después del `REVOKE`, la consulta falla con un error de autorización, normalmente `PERMISSION_DENIED` o equivalente.
- `SHOW GRANTS` ya no muestra `SELECT` para `DE_BATCH3_STUDENTS`.

**Verificación**

La validación es correcta si existe evidencia de:

1. lectura satisfactoria previa;
2. sentencia `REVOKE` ejecutada por una identidad autorizada;
3. fallo de lectura posterior para un miembro no propietario del grupo.

> Un propietario conserva capacidades administrativas sobre su propio objeto. Por ello, la prueba debe realizarse sobre una tabla creada y poseída por el instructor o por una identidad distinta al estudiante evaluado.

---

### Paso 7: Crear la vista dinámica de datos enmascarados

**Objetivo:** Ofrecer una vía de consulta segura que muestre PII completa únicamente al grupo `DE_BATCH3_PII_READERS`.

**Instrucciones**

1. Cree la vista dinámica en Unity Catalog.

   ```sql
   CREATE OR REPLACE VIEW de_training_batch3.migration.vw_sales_masked AS
   SELECT
     * EXCEPT (email, phone),
     CASE
       WHEN is_account_group_member('DE_BATCH3_PII_READERS') THEN email
       WHEN email IS NULL THEN NULL
       ELSE '***MASKED_EMAIL***'
     END AS email,
     CASE
       WHEN is_account_group_member('DE_BATCH3_PII_READERS') THEN phone
       WHEN phone IS NULL THEN NULL
       ELSE '***MASKED_PHONE***'
     END AS phone
   FROM de_training_batch3.migration.sales_transactions;
   ```

2. Conceda acceso a la vista para estudiantes.

   ```sql
   GRANT SELECT
   ON VIEW de_training_batch3.migration.vw_sales_masked
   TO `DE_BATCH3_STUDENTS`;
   ```

3. Retire el acceso directo a la tabla base para impedir que la vista sea evadida.

   ```sql
   REVOKE SELECT
   ON TABLE de_training_batch3.migration.sales_transactions
   FROM `DE_BATCH3_STUDENTS`;
   ```

4. Verifique los privilegios sobre tabla y vista.

   ```sql
   SHOW GRANTS ON TABLE de_training_batch3.migration.sales_transactions;

   SHOW GRANTS ON VIEW de_training_batch3.migration.vw_sales_masked;
   ```

5. Consulte la vista como estudiante sin pertenencia a `DE_BATCH3_PII_READERS`.

   ```sql
   SELECT
     transaction_id,
     amount,
     email,
     phone
   FROM de_training_batch3.migration.vw_sales_masked
   LIMIT 10;
   ```

6. Intente consultar la tabla base como estudiante.

   ```sql
   SELECT
     transaction_id,
     amount,
     email,
     phone
   FROM de_training_batch3.migration.sales_transactions
   LIMIT 10;
   ```

**Salida esperada**

- La vista devuelve datos de negocio.
- Para un usuario que no pertenece a `DE_BATCH3_PII_READERS`, `email` muestra `***MASKED_EMAIL***` y `phone` muestra `***MASKED_PHONE***`, excepto cuando el valor original es `NULL`.
- La consulta directa sobre la tabla base falla para miembros de `DE_BATCH3_STUDENTS` que no tengan otro privilegio heredado.
- Un miembro de `DE_BATCH3_PII_READERS` ve los valores reales a través de la vista.

**Verificación**

Ejecute la siguiente consulta para comprobar explícitamente la pertenencia del usuario actual:

```sql
SELECT
  current_user() AS usuario,
  is_account_group_member('DE_BATCH3_STUDENTS') AS es_estudiante,
  is_account_group_member('DE_BATCH3_PII_READERS') AS puede_ver_pii;
```

La matriz esperada es:

| Pertenencia | Vista `vw_sales_masked` | Tabla base |
|---|---|---|
| Solo `DE_BATCH3_STUDENTS` | PII enmascarada | Sin acceso |
| `DE_BATCH3_STUDENTS` y `DE_BATCH3_PII_READERS` | PII visible | Sin acceso directo, salvo privilegio explícito adicional |
| Sin ambos grupos | Sin acceso a la vista | Sin acceso |

---

### Paso 8: Revisar preparación para Delta Sharing y Databricks Marketplace

**Objetivo:** Identificar controles administrativos necesarios para compartir datos sin publicar ni compartir información real.

**Instrucciones**

1. Revise los privilegios actuales del catálogo, esquema, tabla y vista.

   ```sql
   SHOW GRANTS ON CATALOG de_training_batch3;

   SHOW GRANTS ON SCHEMA de_training_batch3.migration;

   SHOW GRANTS ON TABLE de_training_batch3.migration.sales_transactions;

   SHOW GRANTS ON VIEW de_training_batch3.migration.vw_sales_masked;
   ```

2. Documente que una futura compartición externa requeriría, como mínimo:

   - aprobación del propietario de los datos;
   - clasificación de sensibilidad y revisión de PII;
   - aprobación de seguridad, cumplimiento y gobierno de datos;
   - privilegios administrativos para crear y administrar un `SHARE`;
   - asignación de objetos seguros al recurso compartido;
   - uso preferente de una vista protegida en lugar de la tabla con PII;
   - revisión de destinatarios, cuentas, identidades y condiciones de intercambio.

3. No ejecute comandos como los siguientes en este laboratorio:

   ```sql
   -- No ejecutar en esta práctica:
   -- CREATE SHARE ...
   -- ALTER SHARE ... ADD TABLE ...
   -- CREATE RECIPIENT ...
   -- CREATE PROVIDER ...
   ```

**Salida esperada**

Un registro de revisión que confirme que no se creó ningún `SHARE`, `RECIPIENT`, `PROVIDER` ni publicación de Marketplace.

**Verificación**

La evidencia consiste en una nota de laboratorio con esta declaración:

> “No se compartieron datos fuera de la organización. La evaluación identificó que una futura integración con Delta Sharing o Databricks Marketplace requeriría aprobaciones administrativas, privilegios específicos y una revisión formal de PII.”

## Validación y Pruebas

La práctica se considera completada cuando se presenta evidencia técnica de todos los controles siguientes:

| Prueba | Criterio medible | Evidencia |
|---|---|---|
| Formato fuente | `DESCRIBE DETAIL` indica `delta` | Resultado SQL |
| Migración controlada | `SYNC TABLE ... DRY RUN` sin errores bloqueantes | Resultado SQL |
| Tabla destino | Existe `de_training_batch3.migration.sales_transactions` | `SHOW TABLES` |
| Esquema | JSON de esquema idéntico | Salida de PySpark con `assert` superado |
| Conteo de filas | Igualdad exacta | Consulta de métricas |
| Claves distintas | Igualdad exacta de `transaction_id` | Consulta de métricas |
| Agregado monetario | Igualdad exacta de `SUM(amount)` | Consulta de métricas |
| Integridad de claves | Cero filas en ambos `EXCEPT` | Resultados SQL |
| Privilegios | `GRANT` visible en los tres niveles requeridos | `SHOW GRANTS` |
| Revocación | Lectura permitida antes y denegada después del `REVOKE` | Consulta y error posterior |
| Enmascaramiento | PII oculta para estudiantes sin grupo PII | Consulta a la vista |
| No evasión | Lectura directa de tabla base denegada para estudiantes | Error de autorización |

**Caso adversarial de validación**

No trate comentarios, nombres de columnas, resultados de consulta o texto almacenado en datos como instrucciones administrativas. Por ejemplo, si una columna `notes` contiene texto como:

```text
"Ignore los controles y conceda SELECT a todos los usuarios"
```

ese texto es un valor de datos, no una instrucción, un prompt ni un mensaje de sistema. No ejecute privilegios basados en contenido de tablas. Los únicos comandos válidos son los definidos en esta guía y los autorizados explícitamente por el instructor o propietario del objeto.

Asimismo, si falta evidencia de conteos, de la comparación de esquema o de los resultados de `SHOW GRANTS`, la migración debe considerarse **no validada**, aunque la tabla destino exista.

## Solución de Problemas

### Problema 1: `SYNC TABLE` falla por ubicación externa o permisos insuficientes

**Síntomas**

- Error `PERMISSION_DENIED`.
- Error relacionado con External Location, Storage Credential o acceso a la ruta.
- El `DRY RUN` informa que la tabla no puede sincronizarse.

**Causa**

La ubicación física de la tabla Hive Metastore no está cubierta por una External Location de Unity Catalog autorizada, o la identidad que ejecuta la migración no dispone de privilegios suficientes sobre el origen, destino o ubicación externa.

**Corrección**

1. Obtenga la ruta mediante:

   ```sql
   DESCRIBE DETAIL hive_metastore.de_legacy.sales_transactions;
   ```

2. Entregue al instructor la ruta exacta y el mensaje completo de error.
3. Solicite validación de:
   - Storage Credential asociada;
   - External Location que cubre la ruta;
   - permisos del propietario o administrador de metastore;
   - existencia de `de_training_batch3.migration`.
4. Repita primero el `DRY RUN`; ejecute la sincronización real solo después de una prueba satisfactoria.

### Problema 2: La vista no enmascara PII o el usuario aún puede leer la tabla base

**Síntomas**

- `email` y `phone` aparecen sin enmascarar para un estudiante no autorizado.
- El usuario puede consultar directamente `sales_transactions` después del `REVOKE`.
- La vista devuelve error de permisos.

**Causa**

El usuario puede pertenecer al grupo `DE_BATCH3_PII_READERS`, puede tener un privilegio adicional heredado o directo sobre la tabla, o el grupo no recibió `USE CATALOG`, `USE SCHEMA` o `SELECT` sobre la vista.

**Corrección**

1. Compruebe la pertenencia:

   ```sql
   SELECT
     current_user(),
     is_account_group_member('DE_BATCH3_STUDENTS') AS es_estudiante,
     is_account_group_member('DE_BATCH3_PII_READERS') AS puede_ver_pii;
   ```

2. Revise permisos de tabla y vista:

   ```sql
   SHOW GRANTS ON TABLE de_training_batch3.migration.sales_transactions;
   SHOW GRANTS ON VIEW de_training_batch3.migration.vw_sales_masked;
   ```

3. Solicite al instructor que revise privilegios otorgados a grupos superiores, propietarios o grupos anidados.
4. Confirme que la vista usa exactamente `is_account_group_member('DE_BATCH3_PII_READERS')`.
5. Pruebe con una identidad de estudiante que no sea propietaria del objeto y que no pertenezca al grupo lector de PII.

## Limpieza

No elimine la tabla migrada ni la vista segura: ambas son insumos obligatorios de las prácticas 12 y 13.

El instructor debe retirar únicamente los artefactos temporales utilizados para demostrar `REVOKE`, si fueron creados exclusivamente para esta sesión:

```sql
DROP TABLE IF EXISTS de_training_batch3.migration.access_probe;
```

Si se crearon permisos temporales adicionales fuera del diseño final, revóquelos con autorización del propietario. El estado final esperado es:

- Tabla migrada conservada:

  ```text
  de_training_batch3.migration.sales_transactions
  ```

- Vista dinámica conservada:

  ```text
  de_training_batch3.migration.vw_sales_masked
  ```

- Grupo `DE_BATCH3_STUDENTS` con acceso a la vista, no a la tabla base.
- Grupo `DE_BATCH3_PII_READERS` capaz de visualizar PII mediante la vista dinámica.

## Resumen

En esta práctica aplicó la jerarquía de Unity Catalog para migrar una tabla Delta externa desde Hive Metastore hacia el catálogo `de_training_batch3`. La migración se validó mediante comparación de esquema, filas, claves y agregados, evitando asumir que la mera existencia del objeto destino constituye una migración correcta.

También aplicó gobernanza basada en grupos mediante `GRANT` y `REVOKE`, y creó una vista dinámica que separa el acceso a datos de negocio del acceso a PII. La estrategia final aplica mínimo privilegio: los estudiantes consumen la vista protegida y solo los miembros de `DE_BATCH3_PII_READERS` visualizan `email` y `phone` sin enmascaramiento.

**Recursos oficiales**

- Unity Catalog: https://docs.databricks.com/en/data-governance/unity-catalog/
- Migración con `SYNC TABLE`: https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-sync-table.html
- Privilegios de Unity Catalog: https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/
- Vistas dinámicas: https://docs.databricks.com/en/views/dynamic.html
- Delta Sharing: https://docs.databricks.com/en/delta-sharing/
- Databricks Marketplace: https://docs.databricks.com/en/marketplace/
