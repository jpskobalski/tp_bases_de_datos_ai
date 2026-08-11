-- Consulta 1: volumen, tasa de rechazo y monto por día.
SELECT
    fecha,
    total_intentos,
    aprobados,
    rechazados,
    ROUND(100.0 * rechazados / NULLIF(total_intentos, 0), 2) AS tasa_rechazo_pct,
    score_promedio,
    monto_total
FROM mv_resumen_diario_scoring
ORDER BY fecha;

-- Consulta 2: pagadores con mayor riesgo y actividad acumulada.
SELECT
    p.pagador_id,
    p.pais,
    COUNT(*) AS intentos,
    SUM(v.monto) AS monto_total,
    ROUND(AVG(v.score_riesgo), 4) AS score_promedio,
    COUNT(*) FILTER (WHERE v.decision = 'RECHAZADO') AS rechazos
FROM vw_detalle_scoring v
JOIN pagador p ON p.pagador_id = v.pagador_id
GROUP BY p.pagador_id, p.pais
ORDER BY score_promedio DESC, monto_total DESC
LIMIT 5;

-- Consulta 3: reglas disparadas y porcentaje de decisiones rechazadas.
SELECT
    rf.codigo_regla,
    rf.nombre,
    COUNT(*) FILTER (WHERE dr.activada) AS veces_disparada,
    COUNT(*) FILTER (
        WHERE dr.activada AND ds.decision = 'RECHAZADO'
    ) AS rechazos_asociados,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE dr.activada AND ds.decision = 'RECHAZADO'
        ) / NULLIF(COUNT(*) FILTER (WHERE dr.activada), 0),
        2
    ) AS rechazo_asociado_pct
FROM regla_fraude rf
JOIN decision_regla dr ON dr.regla_version_id = rf.regla_version_id
JOIN decision_scoring ds ON ds.decision_id = dr.decision_id
GROUP BY rf.codigo_regla, rf.nombre
ORDER BY veces_disparada DESC, rf.codigo_regla;

-- Consulta 4: falsos negativos confirmados mediante contracargos.
SELECT
    v.intento_id,
    v.fecha_hora,
    v.pagador_id,
    v.monto,
    v.moneda,
    v.score_riesgo,
    c.fecha_hora AS fecha_contracargo,
    c.motivo
FROM vw_detalle_scoring v
JOIN contracargo c ON c.intento_id = v.intento_id
WHERE v.decision = 'APROBADO'
  AND c.estado = 'CONFIRMADO'
ORDER BY c.fecha_hora DESC;

-- Consulta 5: dispositivos compartidos por varios pagadores.
SELECT
    d.dispositivo_id,
    d.tipo_dispositivo,
    d.sistema_operativo,
    COUNT(DISTINCT pd.pagador_id) AS pagadores_distintos,
    MIN(pd.primera_observacion) AS primera_observacion,
    MAX(pd.ultima_observacion) AS ultima_observacion
FROM dispositivo d
JOIN pagador_dispositivo pd ON pd.dispositivo_id = d.dispositivo_id
GROUP BY d.dispositivo_id, d.tipo_dispositivo, d.sistema_operativo
HAVING COUNT(DISTINCT pd.pagador_id) > 1
ORDER BY pagadores_distintos DESC;

-- Consulta 6: métricas registradas para cada versión del modelo.
SELECT
    ms.nombre AS modelo,
    ms.version,
    ms.estado,
    dv.version AS dataset,
    mm.nombre AS metrica,
    mm.valor,
    mm.segmento,
    mm.fecha_calculo
FROM modelo_scoring ms
JOIN dataset_version dv ON dv.dataset_version_id = ms.dataset_id
JOIN metrica_modelo mm ON mm.modelo_version_id = ms.modelo_version_id
ORDER BY ms.version, mm.nombre;

-- Consulta 7: trazabilidad de accesos y acciones de los analistas.
SELECT
    ar.nombre AS analista,
    r.nombre AS rol,
    ea.fecha_hora,
    ea.accion,
    ea.tipo_objeto,
    ea.objeto_id,
    ea.motivo
FROM evento_auditoria ea
JOIN analista_riesgo ar ON ar.analista_id = ea.analista_id
JOIN rol r ON r.rol_id = ar.rol_id
ORDER BY ea.fecha_hora DESC;

-- Consulta 8: comparación del score con la etiqueta madura del dataset.
SELECT
    CASE
        WHEN ds.score_riesgo >= 0.55 THEN 'PREDICE_FRAUDE'
        ELSE 'PREDICE_LEGITIMO'
    END AS prediccion,
    di.etiqueta_fraude,
    COUNT(*) AS casos
FROM dataset_item di
JOIN decision_scoring ds ON ds.decision_id = di.decision_id
GROUP BY prediccion, di.etiqueta_fraude
ORDER BY prediccion, di.etiqueta_fraude;
