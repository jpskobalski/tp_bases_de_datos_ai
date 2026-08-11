CREATE KEYSPACE IF NOT EXISTS fraude
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE fraude;

CREATE TABLE IF NOT EXISTS payment_decision_log_by_day (
    fecha_particion date,
    bucket int,
    fecha_hora timestamp,
    event_id uuid,
    intento_id uuid,
    decision_id uuid,
    pagador_id uuid,
    cobrador_id uuid,
    tarjeta_id uuid,
    dispositivo_id uuid,
    monto decimal,
    moneda text,
    direccion_ip text,
    pais_origen text,
    feature_snapshot map<text, text>,
    score_riesgo decimal,
    decision text,
    modelo_version_id text,
    reglas_disparadas list<text>,
    PRIMARY KEY ((fecha_particion, bucket), fecha_hora, event_id)
) WITH CLUSTERING ORDER BY (fecha_hora DESC, event_id ASC);

CREATE TABLE IF NOT EXISTS intentos_pago_por_pagador (
    pagador_id uuid,
    fecha_hora timestamp,
    intento_id uuid,
    event_id uuid,
    cobrador_id uuid,
    tarjeta_id uuid,
    dispositivo_id uuid,
    monto decimal,
    moneda text,
    pais_origen text,
    decision text,
    score_riesgo decimal,
    PRIMARY KEY (pagador_id, fecha_hora, intento_id)
) WITH CLUSTERING ORDER BY (fecha_hora DESC, intento_id ASC);

CREATE TABLE IF NOT EXISTS intentos_pago_por_comercio (
    cobrador_id uuid,
    fecha_hora timestamp,
    intento_id uuid,
    pagador_id uuid,
    monto decimal,
    moneda text,
    decision text,
    score_riesgo decimal,
    PRIMARY KEY (cobrador_id, fecha_hora, intento_id)
) WITH CLUSTERING ORDER BY (fecha_hora DESC, intento_id ASC);

CREATE TABLE IF NOT EXISTS decisiones_scoring (
    intento_id uuid,
    decision_id uuid,
    fecha_hora timestamp,
    modelo_version_id text,
    score_riesgo decimal,
    decision text,
    motivo_principal text,
    latencia_ms int,
    reglas_disparadas list<text>,
    PRIMARY KEY (intento_id, decision_id)
);

CREATE TABLE IF NOT EXISTS evaluaciones_features (
    decision_id uuid,
    feature_id text,
    nombre text,
    valor text,
    version text,
    fecha_calculo timestamp,
    ventana_temporal text,
    PRIMARY KEY (decision_id, feature_id)
);

CREATE TABLE IF NOT EXISTS eventos_procesados (
    event_id uuid PRIMARY KEY,
    processed_at timestamp
);
