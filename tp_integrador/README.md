# Trabajo Práctico Integrador - Bases de Datos para Inteligencia Artificial

Carrera de Especialización en Inteligencia Artificial - 2026

Docente: Martín Lacheski

## Integrantes

- Ronald Uthurralt
- Luis Diaz
- Juan Pablo Skobalski
- Lurdes

## Caso de uso

Sistema de detección de fraude y scoring de pagos en tiempo real. La solución recibe un intento de pago, recupera variables acumuladas, calcula un score con un Random Forest, devuelve una decisión síncrona y publica un evento para actualizar los acumulados y conservar trazabilidad.

## Arquitectura implementada

- **PostgreSQL:** entidades operacionales, reglas, decisiones, contracargos, auditoría, reportes y datasets de entrenamiento.
- **Redis:** hashes de features online, relaciones de cardinalidad y Redis Streams como bus de eventos.
- **Cassandra:** logs append-only y tablas desnormalizadas orientadas a consultas por fecha, pagador, comercio y decisión.
- **Python:** servicio de scoring, Random Forest, publicación del evento y procesador asíncrono.

La aplicación real implementa el camino `Scoring Service -> Redis Features -> Redis Streams -> Event Processor -> Cassandra`. PostgreSQL representa la capa relacional alimentada por el proceso batch de la arquitectura.

## Ejecución completa

Requisitos:

- Docker Desktop;
- Python 3.11 o posterior;
- `make`.

Desde esta carpeta:

```bash
make todo
```

El comando realiza las siguientes acciones:

1. levanta PostgreSQL, Redis y Cassandra y espera sus health checks;
2. crea esquemas, índices y vistas;
3. carga datos sintéticos deterministas;
4. ejecuta 8 consultas PostgreSQL, 5 Cassandra y 8 operaciones Redis;
5. instala las dependencias Python en `.venv`;
6. ejecuta tres pagos contra Redis y Cassandra reales.

Para reconstruir también los volúmenes desde cero:

```bash
make reiniciar
```

Para detener los servicios sin borrar datos:

```bash
make detener
```

## Ejecuciones parciales

```bash
make inicializar       # levanta los motores y carga Redis/Cassandra
make verificar         # ejecuta todas las consultas
make aplicacion        # usa Redis y Cassandra reales
make aplicacion-mock   # usa únicamente almacenamiento en memoria
```

## Guía para poner la aplicación a prueba

1. Iniciar Docker Desktop y esperar a que indique que el motor está activo.
2. Abrir una terminal en la carpeta `tp_integrador`.
3. Reconstruir y probar todo el entorno:

```bash
make reiniciar
```

La primera ejecución puede tardar algunos minutos mientras se descargan las imágenes y se inicia Cassandra. El proceso debe finalizar mostrando:

- las ocho consultas PostgreSQL sin errores;
- las cinco consultas Cassandra con resultados;
- las operaciones Redis sobre hashes, Sets y Streams;
- tres decisiones de la aplicación: `APROBADO`, `RECHAZADO`, `APROBADO`;
- `eventos_publicados: 3` y `eventos_pendientes: 0`.

4. Confirmar que los tres motores estén saludables:

```bash
docker compose ps
```

Los servicios `postgres`, `redis` y `cassandra` deben aparecer como `healthy`.

5. Volver a ejecutar solamente las consultas:

```bash
make verificar
```

6. Volver a probar solamente el camino online real:

```bash
make aplicacion
```

7. Como control adicional, verificar las cantidades principales:

```bash
docker compose exec -T postgres psql -At -U fraude -d fraude \
  -c "SELECT COUNT(*) FROM intento_pago; SELECT COUNT(*) FROM decision_scoring; SELECT COUNT(*) FROM decision_feature;"

docker compose exec -T redis redis-cli XLEN fraud:payment_scored

docker compose exec -T redis redis-cli XPENDING \
  fraud:payment_scored fraud-processors

docker compose exec -T cassandra cqlsh \
  -e "SELECT COUNT(*) FROM fraude.eventos_procesados;"
```

Después de una reconstrucción limpia se esperan `12`, `12` y `60` en PostgreSQL; `3` eventos y `0` pendientes en Redis; y al menos `5` eventos procesados en Cassandra, contando los dos eventos sintéticos iniciales y los tres generados por la aplicación.

8. Al terminar, detener los contenedores sin borrar los datos:

```bash
make detener
```

Puertos locales utilizados:

| Motor | Puerto |
|---|---:|
| PostgreSQL | 54321 |
| Redis | 63791 |
| Cassandra | 19042 |

Las credenciales incluidas en `compose.yaml` son exclusivas del entorno académico local y no deben reutilizarse en un despliegue real.

## Consultas representativas

Las consultas PostgreSQL responden, entre otras, estas preguntas:

1. ¿Cuál es el volumen y la tasa de rechazo diaria?
2. ¿Qué pagadores presentan mayor riesgo acumulado?
3. ¿Qué reglas se disparan y con qué decisiones se relacionan?
4. ¿Qué aprobaciones terminaron en contracargos confirmados?
5. ¿Qué dispositivos son compartidos por varios pagadores?
6. ¿Qué métricas corresponden a cada versión del modelo y dataset?
7. ¿Qué acciones realizaron los analistas y auditores?
8. ¿Cómo se compara la predicción con la etiqueta madura?

Cassandra agrega accesos por partición diaria, pagador, comercio, intento y snapshot de features. Redis permite inspeccionar hashes, Sets, longitud del stream y estado del grupo consumidor.

## Datos de ejemplo

La carga PostgreSQL contiene 12 intentos, 12 decisiones, 3 contracargos, 60 snapshots de features, reglas, métricas, reportes y eventos de auditoría. Redis y Cassandra incluyen ejemplos equivalentes. El evento semiestructurado de referencia se encuentra en `data/ejemplos/payment_scored_event.json`.

Todos los datos son ficticios y los identificadores personales se representan mediante hashes o tokens. El modelo se entrena con datos sintéticos y no es válido para tomar decisiones reales.

## Estructura principal

```text
tp_integrador/
├── app/                 # aplicación real y alternativa en memoria
├── data/ejemplos/       # eventos semiestructurados de ejemplo
├── db/
│   ├── estructura/      # DDL PostgreSQL
│   ├── datos/           # carga sintética
│   ├── consultas/       # consultas representativas
│   └── indices_vistas/  # optimizaciones y vistas
├── nosql/
│   ├── cassandra/       # CQL de esquema, datos y consultas
│   └── redis/           # datos y consultas redis-cli
├── vectorial/           # análisis de aplicabilidad vectorial
├── docs/                # informe y diagramas
├── scripts/             # inicialización y verificación
├── compose.yaml
└── Makefile
```

## Decisiones y limitaciones

- Cassandra se modela por consulta y acepta duplicación para evitar joins online.
- Redis prioriza baja latencia y utiliza estructuras específicas para acumulados y relaciones.
- PostgreSQL conserva integridad referencial, auditoría y consultas relacionales.
- `event_id` permite procesamiento idempotente.
- La demo no implementa ventanas móviles exactas, alta disponibilidad, cifrado ni gestión externa de secretos.
- Un despliegue productivo requeriría reintentos, mensajes fallidos, monitoreo, retención, particionado y pruebas de carga.
