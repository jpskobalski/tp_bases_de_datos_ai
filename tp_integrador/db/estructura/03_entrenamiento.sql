-- =====================================================================
-- ESTRUCTURA POSTGRES — DIAGRAMA "ENTRENAMIENTO"
-- Sistema de detección de fraude / scoring de pagos en tiempo real
-- =====================================================================

-- ---------------------------------------------------------------------
-- NIVEL 0: Tablas base
-- ---------------------------------------------------------------------

CREATE TABLE dataset_version (
    dataset_version_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    version VARCHAR(50),
    fecha_generacion TIMESTAMP, 
    ventana_desde TIMESTAMP,
    ventana_hasta TIMESTAMP,
    fecha_corte_labels TIMESTAMP,
    estado VARCHAR(50)
);

-- Depende de modelo_scoring (tabla de "operaciones")
CREATE TABLE metrica_modelo (
    metrica_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre VARCHAR(100),
    valor DECIMAL(5, 2),
    segmento VARCHAR(50),
    fecha_calculo TIMESTAMP DEFAULT NOW(),
    modelo_version_id UUID REFERENCES modelo_scoring(modelo_version_id)
);


-- ---------------------------------------------------------------------
-- NIVEL 1: Dependen de dataset_version, decision_scoring y contracargo
-- (estas dos últimas son tablas de "operaciones")
-- ---------------------------------------------------------------------

CREATE TABLE dataset_item (
    dataset_version_id UUID REFERENCES dataset_version(dataset_version_id),
    decision_id UUID REFERENCES decision_scoring(decision_id),
    contracargo_id UUID REFERENCES contracargo(contracargo_id),
    etiqueta_fraude BOOLEAN,
    fuente_etiqueta VARCHAR(100),
    fecha_maduracion TIMESTAMP,
    PRIMARY KEY (dataset_version_id, decision_id)
);

-- Relación N:M entre dataset_item y feature_riesgo (tabla de "operaciones")
CREATE TABLE dataset_item_feature (
    dataset_version_id UUID REFERENCES dataset_version(dataset_version_id),
    decision_id UUID REFERENCES decision_scoring(decision_id),
    feature_id UUID REFERENCES feature_riesgo(feature_id),
    valor VARCHAR(250),
    fecha_calculo TIMESTAMP,
    calculada_hasta TIMESTAMP,
    PRIMARY KEY (dataset_version_id, decision_id, feature_id)
);
