.PHONY: entorno iniciar inicializar verificar aplicacion aplicacion-mock todo detener reiniciar

# 1. Detección del Sistema Operativo
ifeq ($(OS),Windows_NT)
    # Configuración para Windows
    DETECTED_OS := Windows
    PYTHON := python
    VENV_ACTIVATE := .venv\Scripts\activate
    VENV_PYTHON := .venv\Scripts\python.exe
    # En Windows ejecutamos los scripts Bash invocando bash explícitamente
    RUN_SH := bash
else
    # Configuración para Linux / macOS
    DETECTED_OS := $(shell uname -s)
    PYTHON := python3
    VENV_ACTIVATE := .venv/bin/activate
    VENV_PYTHON := .venv/bin/python
    RUN_SH := 
endif

entorno:
	$(PYTHON) -m venv .venv
	$(VENV_PYTHON) -m pip install --upgrade pip
	$(VENV_PYTHON) -m pip install -r requirements.txt

iniciar:
	docker compose up -d --wait --wait-timeout 240

inicializar: iniciar
	$(RUN_SH) ./scripts/inicializar_nosql.sh

verificar:
	$(RUN_SH) ./scripts/verificar_bases.sh

aplicacion: entorno
	$(VENV_PYTHON) app/real_fraud_pipeline.py

aplicacion-mock: entorno
	$(VENV_PYTHON) app/mock_fraud_pipeline.py

todo: inicializar verificar aplicacion

detener:
	docker compose down

reiniciar:
	docker compose down --volumes
	$(MAKE) todo