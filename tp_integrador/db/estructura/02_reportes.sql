-- =====================================================================
-- ESTRUCTURA POSTGRES — DIAGRAMA "REPORTES"
-- Sistema de detección de fraude / scoring de pagos en tiempo real
-- =====================================================================

-- ---------------------------------------------------------------------
-- NIVEL 0: Tablas base (no dependen de otras)
-- ---------------------------------------------------------------------

CREATE TABLE rol (
    rol_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre VARCHAR(100),
    descripcion VARCHAR(250)
);

CREATE TABLE reporte (
    reporte_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre VARCHAR(100),
    tipo VARCHAR(50),
    periodo_desde TIMESTAMP,
    periodo_hasta TIMESTAMP,
    fecha_generacion TIMESTAMP
);

-- ---------------------------------------------------------------------
-- NIVEL 1: Depende de rol
-- ---------------------------------------------------------------------

CREATE TABLE analista_riesgo (
    analista_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre VARCHAR(100),
    email VARCHAR(100),
    estado VARCHAR(50),
    fecha_alta TIMESTAMP DEFAULT NOW(),
    rol_id UUID REFERENCES rol(rol_id)
);

-- ---------------------------------------------------------------------
-- NIVEL 2: Dependen de analista_riesgo (y de tablas de "operaciones")
-- ---------------------------------------------------------------------

-- Relación N:M entre reporte y analista_riesgo, con clave compuesta de 3 columnas
CREATE TABLE consulta_reporte (
    reporte_id UUID REFERENCES reporte(reporte_id),
    analista_id UUID REFERENCES analista_riesgo(analista_id),
    fecha_consulta TIMESTAMP DEFAULT NOW(),
    filtros_aplicados VARCHAR(500),
    PRIMARY KEY (reporte_id, analista_id, fecha_consulta)
);

CREATE TABLE evento_auditoria (
    evento_auditoria_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fecha_hora TIMESTAMP DEFAULT NOW(),
    accion VARCHAR(100),
    tipo_objeto VARCHAR(50),
    objeto_id VARCHAR(100),
    estado_anterior VARCHAR(100),
    estado_nuevo VARCHAR(100),
    motivo VARCHAR(250),
    analista_id UUID REFERENCES analista_riesgo(analista_id)
);

-- Relación N:M entre reporte y decision_scoring (tabla de "operaciones")
CREATE TABLE reporte_decision (
    reporte_id UUID REFERENCES reporte(reporte_id),
    decision_id UUID REFERENCES decision_scoring(decision_id),
    PRIMARY KEY (reporte_id, decision_id)
);

-- Relación N:M entre reporte y contracargo (tabla de "operaciones")
CREATE TABLE reporte_contracargo (
    reporte_id UUID REFERENCES reporte(reporte_id),
    contracargo_id UUID REFERENCES contracargo(contracargo_id),
    PRIMARY KEY (reporte_id, contracargo_id)
);
