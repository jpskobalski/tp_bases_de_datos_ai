-- =====================================================================
-- ESTRUCTURA POSTGRES — DIAGRAMA "OPERACIONES"
-- Sistema de detección de fraude / scoring de pagos en tiempo real
-- =====================================================================

-- ---------------------------------------------------------------------
-- NIVEL 0: Tablas base (no dependen de otras)
-- ---------------------------------------------------------------------

CREATE TABLE pagador (
    pagador_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tipo_identificacion VARCHAR(50),
    identificacion_hash VARCHAR(250),
    pais VARCHAR(100),
    estado VARCHAR(50),
    fecha_alta TIMESTAMP DEFAULT NOW()
);

CREATE TABLE dispositivo (
    dispositivo_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fingerprint VARCHAR(250),
    tipo_dispositivo VARCHAR(50),
    sistema_operativo VARCHAR(50),
    primera_observacion TIMESTAMP DEFAULT NOW()
);

CREATE TABLE cobrador (
    cobrador_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tipo_cobrador VARCHAR(100),
    categoria VARCHAR(100),
    pais VARCHAR(100),
    estado VARCHAR(50),
    fecha_alta TIMESTAMP DEFAULT NOW()
);

CREATE TABLE feature_riesgo (
    feature_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre VARCHAR(100),
    version VARCHAR(100),
    descripcion VARCHAR(250),
    tipo_dato VARCHAR(100),
    ventana_temporal VARCHAR(100),
    activa BOOLEAN DEFAULT TRUE
);

CREATE TABLE regla_fraude (
    regla_version_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    analista_creador_id UUID,  -- FK pendiente: apunta a analista_riesgo (diagrama "reportes")
    codigo_regla VARCHAR(50),
    nombre VARCHAR(100),
    version VARCHAR(50),
    expresion_condicion VARCHAR(500),
    accion VARCHAR(50),
    estado VARCHAR(50),
    vigencia_desde TIMESTAMP,
    vigencia_hasta TIMESTAMP,
    fecha_creacion TIMESTAMP DEFAULT NOW()
);

CREATE TABLE modelo_scoring (
    modelo_version_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID, -- FK pendiente: apunta a dataset (diagrama "entrenamientos")
    nombre VARCHAR(100),
    version VARCHAR(100),
    fecha_entrenamiento TIMESTAMP,
    estado VARCHAR(50),
    fecha_despliegue TIMESTAMP
);

-- ---------------------------------------------------------------------
-- NIVEL 1: Dependen de una tabla de nivel 0
-- ---------------------------------------------------------------------

CREATE TABLE tarjeta (
    tarjeta_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pan_token VARCHAR(250),
    marca VARCHAR(100),
    pais_emisor VARCHAR(100),
    estado VARCHAR(50),
    pagador_id UUID REFERENCES pagador(pagador_id)
);

-- Relación N:M entre pagador y dispositivo
CREATE TABLE pagador_dispositivo (
    pagador_id UUID REFERENCES pagador(pagador_id),
    dispositivo_id UUID REFERENCES dispositivo(dispositivo_id),
    primera_observacion TIMESTAMP DEFAULT NOW(),
    ultima_observacion TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (pagador_id, dispositivo_id)
);


-- ---------------------------------------------------------------------
-- NIVEL 2: Tabla central (depende de 4 tablas de niveles anteriores)
-- ---------------------------------------------------------------------

CREATE TABLE intento_pago (
    intento_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    evento_id UUID, 
    pagador_id UUID REFERENCES pagador(pagador_id),
    cobrador_id UUID REFERENCES cobrador(cobrador_id),
    tarjeta_id UUID REFERENCES tarjeta(tarjeta_id),
    dispositivo_id UUID REFERENCES dispositivo(dispositivo_id),
    fecha_hora TIMESTAMP,
    monto DECIMAL(12, 2),
    moneda VARCHAR(30),
    direccion_ip VARCHAR(100),
    pais_origen VARCHAR(100),
    estado_operacion VARCHAR(50)
);


-- ---------------------------------------------------------------------
-- NIVEL 3: Dependen de intento_pago
-- ---------------------------------------------------------------------

-- Relación 1:1 con intento_pago (UNIQUE en intento_id)
CREATE TABLE decision_scoring (
    decision_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    intento_id UUID UNIQUE REFERENCES intento_pago(intento_id),
    modelo_version_id UUID REFERENCES modelo_scoring(modelo_version_id),
    fecha_hora TIMESTAMP,
    score_riesgo DECIMAL(5, 4),
    decision VARCHAR(50),
    motivo_principal VARCHAR(100),
    latencia_ms INT
);

-- Relación 1:1 con intento_pago (UNIQUE en intento_id)
CREATE TABLE contracargo (
    contracargo_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    intento_id UUID UNIQUE REFERENCES intento_pago(intento_id),
    fecha_hora TIMESTAMP,
    monto DECIMAL(12, 2),
    motivo VARCHAR(250),
    estado VARCHAR(50),
    fuente VARCHAR(50)
);

-- ---------------------------------------------------------------------
-- NIVEL 4: Dependen de decision_scoring
-- ---------------------------------------------------------------------

-- Relación N:M entre decision_scoring y feature_riesgo
CREATE TABLE decision_feature (
    decision_id UUID REFERENCES decision_scoring(decision_id), 
    feature_id UUID REFERENCES feature_riesgo(feature_id),
    valor VARCHAR(250),
    calculada_hasta TIMESTAMP,
    PRIMARY KEY (decision_id, feature_id)
);

-- Relación N:M entre decision_scoring y regla_fraude
CREATE TABLE decision_regla (
    decision_id UUID REFERENCES decision_scoring(decision_id), 
    regla_version_id UUID REFERENCES regla_fraude(regla_version_id),
    activada BOOLEAN,
    valor_observado VARCHAR(250),
    resultado VARCHAR(100),
    PRIMARY KEY (decision_id, regla_version_id)
);

