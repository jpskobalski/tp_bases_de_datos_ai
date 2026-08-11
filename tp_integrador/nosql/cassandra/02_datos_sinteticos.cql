USE fraude;

INSERT INTO payment_decision_log_by_day (
    fecha_particion, bucket, fecha_hora, event_id, intento_id, decision_id,
    pagador_id, cobrador_id, tarjeta_id, dispositivo_id, monto, moneda,
    direccion_ip, pais_origen, feature_snapshot, score_riesgo, decision,
    modelo_version_id, reglas_disparadas
) VALUES (
    '2026-08-01', 9, '2026-08-01T09:05:00Z',
    10000000-0000-0000-0000-00000000e001,
    00000000-0000-0000-0000-00000000b001,
    00000000-0000-0000-0000-00000000c001,
    00000000-0000-0000-0000-000000007001,
    00000000-0000-0000-0000-000000009001,
    00000000-0000-0000-0000-00000000a001,
    00000000-0000-0000-0000-000000008001,
    50.00, 'ARS', '190.18.10.1', 'AR',
    {'payer.tx_count_5m': '1', 'payer.amount_sum_1h': '50.00', 'device.distinct_payers_24h': '1'},
    0.0800, 'APROBADO', 'rf-demo-v1', []
);

INSERT INTO payment_decision_log_by_day (
    fecha_particion, bucket, fecha_hora, event_id, intento_id, decision_id,
    pagador_id, cobrador_id, tarjeta_id, dispositivo_id, monto, moneda,
    direccion_ip, pais_origen, feature_snapshot, score_riesgo, decision,
    modelo_version_id, reglas_disparadas
) VALUES (
    '2026-08-02', 11, '2026-08-02T11:00:00Z',
    10000000-0000-0000-0000-00000000e003,
    00000000-0000-0000-0000-00000000b003,
    00000000-0000-0000-0000-00000000c003,
    00000000-0000-0000-0000-000000007002,
    00000000-0000-0000-0000-000000009002,
    00000000-0000-0000-0000-00000000a002,
    00000000-0000-0000-0000-000000008002,
    1850.00, 'ARS', '200.40.2.8', 'AR',
    {'payer.tx_count_5m': '8', 'payer.amount_sum_1h': '6200.00', 'device.distinct_payers_24h': '5'},
    0.9600, 'RECHAZADO', 'rf-demo-v1', ['RF001_HIGH_AMOUNT', 'RF002_PAYER_VELOCITY']
);

INSERT INTO payment_decision_log_by_day (
    fecha_particion, bucket, fecha_hora, event_id, intento_id, decision_id,
    pagador_id, cobrador_id, tarjeta_id, dispositivo_id, monto, moneda,
    direccion_ip, pais_origen, feature_snapshot, score_riesgo, decision,
    modelo_version_id, reglas_disparadas
) VALUES (
    '2026-08-03', 14, '2026-08-03T14:15:00Z',
    10000000-0000-0000-0000-00000000e008,
    00000000-0000-0000-0000-00000000b008,
    00000000-0000-0000-0000-00000000c008,
    00000000-0000-0000-0000-000000007003,
    00000000-0000-0000-0000-000000009003,
    00000000-0000-0000-0000-00000000a003,
    00000000-0000-0000-0000-000000008005,
    2200.00, 'USD', '181.22.4.5', 'UY',
    {'payer.tx_count_5m': '7', 'payer.amount_sum_1h': '2515.00', 'device.distinct_payers_24h': '3'},
    0.8900, 'RECHAZADO', 'rf-demo-v1', ['RF001_HIGH_AMOUNT', 'RF003_SHARED_DEVICE']
);

INSERT INTO intentos_pago_por_pagador (
    pagador_id, fecha_hora, intento_id, event_id, cobrador_id, tarjeta_id,
    dispositivo_id, monto, moneda, pais_origen, decision, score_riesgo
) VALUES (
    00000000-0000-0000-0000-000000007001, '2026-08-01T09:05:00Z',
    00000000-0000-0000-0000-00000000b001,
    10000000-0000-0000-0000-00000000e001,
    00000000-0000-0000-0000-000000009001,
    00000000-0000-0000-0000-00000000a001,
    00000000-0000-0000-0000-000000008001,
    50.00, 'ARS', 'AR', 'APROBADO', 0.0800
);

INSERT INTO intentos_pago_por_pagador (
    pagador_id, fecha_hora, intento_id, event_id, cobrador_id, tarjeta_id,
    dispositivo_id, monto, moneda, pais_origen, decision, score_riesgo
) VALUES (
    00000000-0000-0000-0000-000000007002, '2026-08-02T11:00:00Z',
    00000000-0000-0000-0000-00000000b003,
    10000000-0000-0000-0000-00000000e003,
    00000000-0000-0000-0000-000000009002,
    00000000-0000-0000-0000-00000000a002,
    00000000-0000-0000-0000-000000008002,
    1850.00, 'ARS', 'AR', 'RECHAZADO', 0.9600
);

INSERT INTO intentos_pago_por_comercio (
    cobrador_id, fecha_hora, intento_id, pagador_id, monto, moneda,
    decision, score_riesgo
) VALUES (
    00000000-0000-0000-0000-000000009001, '2026-08-01T09:05:00Z',
    00000000-0000-0000-0000-00000000b001,
    00000000-0000-0000-0000-000000007001,
    50.00, 'ARS', 'APROBADO', 0.0800
);

INSERT INTO intentos_pago_por_comercio (
    cobrador_id, fecha_hora, intento_id, pagador_id, monto, moneda,
    decision, score_riesgo
) VALUES (
    00000000-0000-0000-0000-000000009002, '2026-08-02T11:00:00Z',
    00000000-0000-0000-0000-00000000b003,
    00000000-0000-0000-0000-000000007002,
    1850.00, 'ARS', 'RECHAZADO', 0.9600
);

INSERT INTO decisiones_scoring (
    intento_id, decision_id, fecha_hora, modelo_version_id, score_riesgo,
    decision, motivo_principal, latencia_ms, reglas_disparadas
) VALUES (
    00000000-0000-0000-0000-00000000b001,
    00000000-0000-0000-0000-00000000c001,
    '2026-08-01T09:05:00Z', 'rf-demo-v1', 0.0800,
    'APROBADO', 'RIESGO_BAJO', 15, []
);

INSERT INTO decisiones_scoring (
    intento_id, decision_id, fecha_hora, modelo_version_id, score_riesgo,
    decision, motivo_principal, latencia_ms, reglas_disparadas
) VALUES (
    00000000-0000-0000-0000-00000000b003,
    00000000-0000-0000-0000-00000000c003,
    '2026-08-02T11:00:00Z', 'rf-demo-v1', 0.9600,
    'RECHAZADO', 'MONTO_Y_VELOCIDAD', 18,
    ['RF001_HIGH_AMOUNT', 'RF002_PAYER_VELOCITY']
);

INSERT INTO evaluaciones_features (
    decision_id, feature_id, nombre, valor, version, fecha_calculo,
    ventana_temporal
) VALUES (
    00000000-0000-0000-0000-00000000c003, 'tx_count_5m',
    'tx_count_5m', '8', '1', '2026-08-02T11:00:00Z', '5m'
);

INSERT INTO evaluaciones_features (
    decision_id, feature_id, nombre, valor, version, fecha_calculo,
    ventana_temporal
) VALUES (
    00000000-0000-0000-0000-00000000c003, 'amount_sum_1h',
    'amount_sum_1h', '6200.00', '1', '2026-08-02T11:00:00Z', '1h'
);

INSERT INTO eventos_procesados (event_id, processed_at) VALUES
(10000000-0000-0000-0000-00000000e001, '2026-08-01T09:05:00Z');

INSERT INTO eventos_procesados (event_id, processed_at) VALUES
(10000000-0000-0000-0000-00000000e003, '2026-08-02T11:00:00Z');
