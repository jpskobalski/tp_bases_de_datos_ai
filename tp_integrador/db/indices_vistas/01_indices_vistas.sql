-- Índices para los patrones de acceso críticos del sistema.
CREATE INDEX idx_intento_pago_fecha
    ON intento_pago (fecha_hora DESC);

CREATE INDEX idx_intento_pago_pagador_fecha
    ON intento_pago (pagador_id, fecha_hora DESC);

CREATE INDEX idx_intento_pago_cobrador_fecha
    ON intento_pago (cobrador_id, fecha_hora DESC);

CREATE INDEX idx_decision_scoring_decision_score
    ON decision_scoring (decision, score_riesgo DESC);

CREATE INDEX idx_contracargo_fecha
    ON contracargo (fecha_hora DESC);

CREATE INDEX idx_evento_auditoria_analista_fecha
    ON evento_auditoria (analista_id, fecha_hora DESC);

CREATE INDEX idx_decision_regla_regla
    ON decision_regla (regla_version_id, activada);

-- Vista de detalle para análisis operacional y reportes.
CREATE VIEW vw_detalle_scoring AS
SELECT
    ip.intento_id,
    ip.fecha_hora,
    ip.pagador_id,
    ip.cobrador_id,
    ip.tarjeta_id,
    ip.dispositivo_id,
    ip.monto,
    ip.moneda,
    ip.pais_origen,
    ip.estado_operacion,
    ds.decision_id,
    ds.score_riesgo,
    ds.decision,
    ds.motivo_principal,
    ds.latencia_ms,
    ms.nombre AS modelo_nombre,
    ms.version AS modelo_version,
    (c.contracargo_id IS NOT NULL) AS tiene_contracargo,
    c.estado AS estado_contracargo
FROM intento_pago ip
JOIN decision_scoring ds ON ds.intento_id = ip.intento_id
JOIN modelo_scoring ms ON ms.modelo_version_id = ds.modelo_version_id
LEFT JOIN contracargo c ON c.intento_id = ip.intento_id;

-- Vista materializada para el tablero diario. Se refresca en el proceso batch.
CREATE MATERIALIZED VIEW mv_resumen_diario_scoring AS
SELECT
    DATE(ip.fecha_hora) AS fecha,
    COUNT(*) AS total_intentos,
    COUNT(*) FILTER (WHERE ds.decision = 'APROBADO') AS aprobados,
    COUNT(*) FILTER (WHERE ds.decision = 'RECHAZADO') AS rechazados,
    ROUND(AVG(ds.score_riesgo), 4) AS score_promedio,
    SUM(ip.monto) AS monto_total
FROM intento_pago ip
JOIN decision_scoring ds ON ds.intento_id = ip.intento_id
GROUP BY DATE(ip.fecha_hora)
WITH NO DATA;

CREATE UNIQUE INDEX idx_mv_resumen_diario_fecha
    ON mv_resumen_diario_scoring (fecha);
