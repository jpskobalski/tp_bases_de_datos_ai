# Material complementario

## Alcance de la demostración

Los datos, reglas y etiquetas son sintéticos. El Random Forest se entrena durante la ejecución con datos generados de forma determinista y sirve únicamente para demostrar el contrato entre componentes.

Las ventanas temporales de Redis se representan mediante acumulados simples. Una versión productiva utilizaría eventos fechados, expiración, Sorted Sets o HyperLogLog según la precisión requerida.

## Evolución a producción

- Gestión de secretos mediante un servicio externo, sin credenciales en el repositorio.
- Cifrado en tránsito y en reposo para identificadores, tokens e IP.
- Autorización por roles para analistas, supervisores y auditores.
- Reintentos, mensajes fallidos y monitoreo del lag del stream.
- Reconciliación entre Redis, Cassandra y el proceso batch hacia PostgreSQL.
- Versionado de contratos de eventos, modelos, reglas y datasets.
- Particionado temporal, políticas de retención y pruebas de carga.
