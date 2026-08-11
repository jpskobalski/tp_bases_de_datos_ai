.PHONY: entorno iniciar inicializar verificar aplicacion aplicacion-mock todo detener reiniciar

entorno:
	python3 -m venv .venv
	.venv/bin/python -m pip install -r requirements.txt

iniciar:
	docker compose up -d --wait --wait-timeout 240

inicializar: iniciar
	./scripts/inicializar_nosql.sh

verificar:
	./scripts/verificar_bases.sh

aplicacion: entorno
	.venv/bin/python app/real_fraud_pipeline.py

aplicacion-mock: entorno
	.venv/bin/python app/mock_fraud_pipeline.py

todo: inicializar verificar aplicacion

detener:
	docker compose down

reiniciar:
	docker compose down --volumes
	$(MAKE) todo
