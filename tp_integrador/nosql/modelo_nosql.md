# Modelo NoSQL

La capa NoSQL combina Redis y Cassandra porque resuelven patrones de acceso diferentes. Redis mantiene el estado mutable de baja latencia utilizado durante el scoring; Cassandra conserva el historial inmutable y desnormalizado de eventos.

## Redis Features

Los hashes siguen el patrón `fraud:features:<entidad>:<id>`, donde la entidad puede ser `payer`, `card`, `device` o `collector`. Contienen contadores de transacciones, montos acumulados, rechazos y cardinalidades por ventana temporal.

Las relaciones de cardinalidad se representan con Sets bajo `fraud:relations:*`. En producción, las ventanas móviles utilizarían Sorted Sets o estructuras probabilísticas, con TTL de 30 días para ventanas y 90 días renovables para acumulados. El prototipo conserva agregados simples para que el flujo pueda inspeccionarse fácilmente.

El stream `fraud:payment_scored` contiene eventos `PaymentScoredEvent` versionados. El grupo consumidor `fraud-processors` actualiza los acumulados, persiste el log y confirma el mensaje mediante `XACK`.

## Cassandra

Las tablas están diseñadas a partir de las consultas y duplican de manera intencional algunos datos:

- `payment_decision_log_by_day`: auditoría por fecha y bucket horario;
- `intentos_pago_por_pagador`: historial reciente de un pagador;
- `intentos_pago_por_comercio`: historial reciente de un cobrador;
- `decisiones_scoring`: decisión completa por intento;
- `evaluaciones_features`: snapshot de variables por decisión;
- `eventos_procesados`: control de idempotencia por `event_id`.

La clave de partición evita búsquedas globales y las claves de clustering mantienen los eventos ordenados por fecha. La duplicación se acepta para evitar joins en el camino online.

## Consistencia y fallas

El consumidor consulta `eventos_procesados` antes de aplicar un evento y registra el identificador al finalizar. En producción se agregarían reintentos con backoff, una cola de mensajes fallidos, métricas de lag y una estrategia de reconciliación ante fallas entre Redis y Cassandra.
