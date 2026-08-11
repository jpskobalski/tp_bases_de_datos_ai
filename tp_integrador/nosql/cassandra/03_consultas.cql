USE fraude;

-- Consulta del log por partición diaria y bucket horario.
SELECT fecha_hora, intento_id, pagador_id, monto, moneda, score_riesgo, decision
FROM payment_decision_log_by_day
WHERE fecha_particion = '2026-08-02' AND bucket = 11;

-- Historial reciente de un pagador, alineado con la clave de partición.
SELECT fecha_hora, intento_id, cobrador_id, monto, moneda, decision, score_riesgo
FROM intentos_pago_por_pagador
WHERE pagador_id = 00000000-0000-0000-0000-000000007002
LIMIT 20;

-- Historial reciente de un comercio.
SELECT fecha_hora, intento_id, pagador_id, monto, decision, score_riesgo
FROM intentos_pago_por_comercio
WHERE cobrador_id = 00000000-0000-0000-0000-000000009002
LIMIT 20;

-- Decisión completa asociada a un intento.
SELECT * FROM decisiones_scoring
WHERE intento_id = 00000000-0000-0000-0000-00000000b003;

-- Snapshot de features utilizado por una decisión.
SELECT feature_id, nombre, valor, version, ventana_temporal
FROM evaluaciones_features
WHERE decision_id = 00000000-0000-0000-0000-00000000c003;
