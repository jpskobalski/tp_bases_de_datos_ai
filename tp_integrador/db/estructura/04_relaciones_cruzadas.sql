ALTER TABLE regla_fraude
ADD CONSTRAINT fk_regla_analista
FOREIGN KEY (analista_creador_id) REFERENCES analista_riesgo(analista_id);

ALTER TABLE modelo_scoring
ADD CONSTRAINT fk_modelo_dataset
FOREIGN KEY (dataset_id) REFERENCES dataset_version(dataset_version_id);