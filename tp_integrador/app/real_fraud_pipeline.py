"""Ejecución del scoring contra Redis y Cassandra reales.

PostgreSQL se inicializa con los scripts SQL del proyecto y representa la capa
relacional/batch. El camino online de este módulo sigue el diagrama de secuencia:
consulta Redis Features, publica en Redis Streams y persiste el evento en
Cassandra mediante un consumidor asíncrono.
"""

from __future__ import annotations

import copy
import json
import os
import threading
import time
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import redis
from cassandra.cluster import Cluster
from cassandra.query import PreparedStatement

from mock_fraud_pipeline import (
    FeatureSnapshot,
    FraudRiskModel,
    InMemoryRedisFeatureStore,
    PaymentScoredEvent,
    ScoringDecision,
    ScoringRequest,
    ScoringService,
    make_request,
    print_json,
    utc_now,
)


STREAM_NAME = "fraud:payment_scored"
CONSUMER_GROUP = "fraud-processors"


def _json_default(value: Any) -> str:
    return str(value)


def serialize_event(event: PaymentScoredEvent) -> str:
    return json.dumps(asdict(event), default=_json_default, ensure_ascii=False)


def deserialize_event(payload: str) -> PaymentScoredEvent:
    raw = json.loads(payload)
    intento = raw["intento"]
    decision = raw["decision"]
    snapshot = raw["feature_snapshot"]
    return PaymentScoredEvent(
        event_id=UUID(raw["event_id"]),
        event_type=raw["event_type"],
        schema_version=int(raw["schema_version"]),
        occurred_at=datetime.fromisoformat(raw["occurred_at"]),
        intento=ScoringRequest(
            intento_id=UUID(intento["intento_id"]),
            fecha_hora=datetime.fromisoformat(intento["fecha_hora"]),
            pagador_id=UUID(intento["pagador_id"]),
            cobrador_id=UUID(intento["cobrador_id"]),
            tarjeta_id=UUID(intento["tarjeta_id"]),
            dispositivo_id=UUID(intento["dispositivo_id"]),
            monto=Decimal(intento["monto"]),
            moneda=intento["moneda"],
            direccion_ip=intento["direccion_ip"],
            pais_origen=intento["pais_origen"],
        ),
        feature_snapshot=FeatureSnapshot(
            payer_features=snapshot["payer_features"],
            card_features=snapshot["card_features"],
            device_features=snapshot["device_features"],
            collector_features=snapshot["collector_features"],
            snapshot_at=datetime.fromisoformat(snapshot["snapshot_at"]),
        ),
        decision=ScoringDecision(
            decision_id=UUID(decision["decision_id"]),
            intento_id=UUID(decision["intento_id"]),
            score_riesgo=float(decision["score_riesgo"]),
            decision=decision["decision"],
            modelo_version_id=decision["modelo_version_id"],
            reglas_disparadas=tuple(decision["reglas_disparadas"]),
            fecha_hora=datetime.fromisoformat(decision["fecha_hora"]),
            latencia_ms=int(decision["latencia_ms"]),
        ),
    )


class RedisFeatureStore:
    """Adaptador de Redis para los hashes y relaciones de features online."""

    DEFAULTS = InMemoryRedisFeatureStore.DEFAULTS
    KEY_PREFIXES = InMemoryRedisFeatureStore.KEY_PREFIXES

    def __init__(self, client: redis.Redis) -> None:
        self.client = client

    def _key(self, entity_type: str, entity_id: UUID) -> str:
        return f"{self.KEY_PREFIXES[entity_type]}:{entity_id}"

    def seed(self, entity_type: str, entity_id: UUID, **features: Any) -> None:
        values = copy.deepcopy(self.DEFAULTS[entity_type])
        values.update(features)
        values["updated_at"] = values["updated_at"] or utc_now().isoformat()
        key = self._key(entity_type, entity_id)
        self.client.delete(key)
        self.client.hset(
            key,
            mapping={name: "" if value is None else value for name, value in values.items()},
        )

    def _get(self, entity_type: str, entity_id: UUID) -> dict[str, Any]:
        key = self._key(entity_type, entity_id)
        raw = self.client.hgetall(key)
        if not raw:
            self.seed(entity_type, entity_id)
            raw = self.client.hgetall(key)

        parsed: dict[str, Any] = {}
        for name, default in self.DEFAULTS[entity_type].items():
            value = raw.get(name, "")
            if name == "updated_at":
                parsed[name] = value or None
            elif isinstance(default, int):
                parsed[name] = int(value or 0)
            elif isinstance(default, float):
                parsed[name] = float(value or 0.0)
            else:
                parsed[name] = value
        return parsed

    def get_features(self, entity_type: str, entity_id: UUID) -> dict[str, Any]:
        return self._get(entity_type, entity_id)

    def get_snapshot(self, request: ScoringRequest) -> FeatureSnapshot:
        return FeatureSnapshot(
            payer_features=self._get("payer", request.pagador_id),
            card_features=self._get("card", request.tarjeta_id),
            device_features=self._get("device", request.dispositivo_id),
            collector_features=self._get("collector", request.cobrador_id),
            snapshot_at=utc_now(),
        )

    def _relation_key(self, owner_type: str, owner_id: UUID, related: str) -> str:
        return f"fraud:relations:{owner_type}:{owner_id}:{related}"

    def apply_event(self, event: PaymentScoredEvent) -> None:
        request = event.intento
        rejected = event.decision.decision == "RECHAZADO"
        timestamp = event.occurred_at.isoformat()
        entities = (
            ("payer", request.pagador_id),
            ("card", request.tarjeta_id),
            ("device", request.dispositivo_id),
            ("collector", request.cobrador_id),
        )

        pipeline = self.client.pipeline(transaction=True)
        for entity_type, entity_id in entities:
            key = self._key(entity_type, entity_id)
            pipeline.hincrby(key, "tx_count_5m", 1)
            if entity_type == "payer":
                pipeline.hincrby(key, "tx_count_1h", 1)
            pipeline.hincrbyfloat(key, "amount_sum_1h", float(request.monto))
            if rejected:
                pipeline.hincrby(key, "reject_count_24h", 1)
            pipeline.hset(key, "updated_at", timestamp)

        relations = (
            ("payer", request.pagador_id, "cards", request.tarjeta_id),
            ("payer", request.pagador_id, "devices", request.dispositivo_id),
            ("card", request.tarjeta_id, "payers", request.pagador_id),
            ("card", request.tarjeta_id, "devices", request.dispositivo_id),
            ("device", request.dispositivo_id, "payers", request.pagador_id),
            ("device", request.dispositivo_id, "cards", request.tarjeta_id),
            ("collector", request.cobrador_id, "payers", request.pagador_id),
        )
        for owner_type, owner_id, related, related_id in relations:
            pipeline.sadd(
                self._relation_key(owner_type, owner_id, related), str(related_id)
            )
        pipeline.execute()

        count_fields = (
            ("payer", request.pagador_id, "cards", "distinct_cards_24h"),
            ("payer", request.pagador_id, "devices", "distinct_devices_24h"),
            ("card", request.tarjeta_id, "payers", "distinct_payers_24h"),
            ("card", request.tarjeta_id, "devices", "distinct_devices_24h"),
            ("device", request.dispositivo_id, "payers", "distinct_payers_24h"),
            ("device", request.dispositivo_id, "cards", "distinct_cards_24h"),
            ("collector", request.cobrador_id, "payers", "distinct_payers_1h"),
        )
        for owner_type, owner_id, related, field in count_fields:
            relation_count = self.client.scard(
                self._relation_key(owner_type, owner_id, related)
            )
            key = self._key(owner_type, owner_id)
            current = int(float(self.client.hget(key, field) or 0))
            self.client.hset(key, field, max(current, relation_count))

        collector_key = self._key("collector", request.cobrador_id)
        tx_count = int(self.client.hget(collector_key, "tx_count_5m") or 0)
        reject_count = int(
            self.client.hget(collector_key, "reject_count_24h") or 0
        )
        self.client.hset(
            collector_key,
            "reject_rate_24h",
            round(reject_count / max(tx_count, 1), 4),
        )


class RedisStreamEventBus:
    """Publicador de eventos para el stream real de Redis."""

    def __init__(self, client: redis.Redis) -> None:
        self.client = client

    def publish(self, event: PaymentScoredEvent) -> None:
        self.client.xadd(
            STREAM_NAME,
            {
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "schema_version": event.schema_version,
                "payload": serialize_event(event),
            },
        )


class CassandraDecisionLogStore:
    """Adaptador de persistencia para las tablas query-driven de Cassandra."""

    def __init__(self, host: str, port: int) -> None:
        self.cluster = Cluster([host], port=port)
        self.session = self.cluster.connect("fraude")
        self._prepare_statements()

    def _prepare(self, query: str) -> PreparedStatement:
        return self.session.prepare(query)

    def _prepare_statements(self) -> None:
        self.select_event = self._prepare(
            "SELECT event_id FROM eventos_procesados WHERE event_id = ?"
        )
        self.insert_event = self._prepare(
            "INSERT INTO eventos_procesados (event_id, processed_at) VALUES (?, ?)"
        )
        self.insert_log = self._prepare(
            """
            INSERT INTO payment_decision_log_by_day (
                fecha_particion, bucket, fecha_hora, event_id, intento_id,
                decision_id, pagador_id, cobrador_id, tarjeta_id, dispositivo_id,
                monto, moneda, direccion_ip, pais_origen, feature_snapshot,
                score_riesgo, decision, modelo_version_id, reglas_disparadas
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        self.insert_payer = self._prepare(
            """
            INSERT INTO intentos_pago_por_pagador (
                pagador_id, fecha_hora, intento_id, event_id, cobrador_id,
                tarjeta_id, dispositivo_id, monto, moneda, pais_origen,
                decision, score_riesgo
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        self.insert_collector = self._prepare(
            """
            INSERT INTO intentos_pago_por_comercio (
                cobrador_id, fecha_hora, intento_id, pagador_id, monto,
                moneda, decision, score_riesgo
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        self.insert_decision = self._prepare(
            """
            INSERT INTO decisiones_scoring (
                intento_id, decision_id, fecha_hora, modelo_version_id,
                score_riesgo, decision, motivo_principal, latencia_ms,
                reglas_disparadas
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        self.insert_feature = self._prepare(
            """
            INSERT INTO evaluaciones_features (
                decision_id, feature_id, nombre, valor, version,
                fecha_calculo, ventana_temporal
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """
        )

    def has_event(self, event_id: UUID) -> bool:
        return self.session.execute(self.select_event, (event_id,)).one() is not None

    @staticmethod
    def _flatten_snapshot(snapshot: FeatureSnapshot) -> dict[str, str]:
        result: dict[str, str] = {}
        groups = {
            "payer": snapshot.payer_features,
            "card": snapshot.card_features,
            "device": snapshot.device_features,
            "collector": snapshot.collector_features,
        }
        for group, values in groups.items():
            for name, value in values.items():
                result[f"{group}.{name}"] = str(value)
        return result

    def persist_event(self, event: PaymentScoredEvent) -> None:
        request = event.intento
        decision = event.decision
        score = Decimal(str(decision.score_riesgo))
        amount = request.monto
        rules = list(decision.reglas_disparadas)
        snapshot = self._flatten_snapshot(event.feature_snapshot)

        self.session.execute(
            self.insert_log,
            (
                event.occurred_at.date(),
                event.occurred_at.hour,
                event.occurred_at,
                event.event_id,
                request.intento_id,
                decision.decision_id,
                request.pagador_id,
                request.cobrador_id,
                request.tarjeta_id,
                request.dispositivo_id,
                amount,
                request.moneda,
                request.direccion_ip,
                request.pais_origen,
                snapshot,
                score,
                decision.decision,
                decision.modelo_version_id,
                rules,
            ),
        )
        self.session.execute(
            self.insert_payer,
            (
                request.pagador_id,
                event.occurred_at,
                request.intento_id,
                event.event_id,
                request.cobrador_id,
                request.tarjeta_id,
                request.dispositivo_id,
                amount,
                request.moneda,
                request.pais_origen,
                decision.decision,
                score,
            ),
        )
        self.session.execute(
            self.insert_collector,
            (
                request.cobrador_id,
                event.occurred_at,
                request.intento_id,
                request.pagador_id,
                amount,
                request.moneda,
                decision.decision,
                score,
            ),
        )
        self.session.execute(
            self.insert_decision,
            (
                request.intento_id,
                decision.decision_id,
                decision.fecha_hora,
                decision.modelo_version_id,
                score,
                decision.decision,
                decision.reglas_disparadas[0] if rules else "MODELO_RANDOM_FOREST",
                decision.latencia_ms,
                rules,
            ),
        )

        windows = {
            "tx_count_5m": "5m",
            "tx_count_1h": "1h",
            "amount_sum_1h": "1h",
            "reject_count_24h": "24h",
            "distinct_cards_24h": "24h",
            "distinct_devices_24h": "24h",
            "distinct_payers_24h": "24h",
            "distinct_payers_1h": "1h",
            "reject_rate_24h": "24h",
        }
        for feature_id, value in snapshot.items():
            name = feature_id.split(".", 1)[1]
            if name == "updated_at":
                continue
            self.session.execute(
                self.insert_feature,
                (
                    decision.decision_id,
                    feature_id,
                    name,
                    value,
                    "1",
                    event.feature_snapshot.snapshot_at,
                    windows.get(name, "sin_ventana"),
                ),
            )
        self.session.execute(self.insert_event, (event.event_id, utc_now()))

    def close(self) -> None:
        self.cluster.shutdown()


class RedisStreamEventProcessor:
    """Consumidor que simula XREADGROUP, procesamiento idempotente y XACK."""

    def __init__(
        self,
        client: redis.Redis,
        feature_store: RedisFeatureStore,
        log_store: CassandraDecisionLogStore,
    ) -> None:
        self.client = client
        self.feature_store = feature_store
        self.log_store = log_store
        self.error: BaseException | None = None

    def run(self, expected_events: int) -> None:
        processed = 0
        deadline = time.monotonic() + 45
        try:
            while processed < expected_events and time.monotonic() < deadline:
                streams = self.client.xreadgroup(
                    CONSUMER_GROUP,
                    "processor-1",
                    {STREAM_NAME: ">"},
                    count=1,
                    block=1_000,
                )
                if not streams:
                    continue
                for _, entries in streams:
                    for message_id, fields in entries:
                        event = deserialize_event(fields["payload"])
                        if not self.log_store.has_event(event.event_id):
                            self.feature_store.apply_event(event)
                            self.log_store.persist_event(event)
                        self.client.xack(STREAM_NAME, CONSUMER_GROUP, message_id)
                        processed += 1
            if processed != expected_events:
                raise TimeoutError(
                    f"Se esperaban {expected_events} eventos y se procesaron {processed}"
                )
        except BaseException as exc:
            self.error = exc


def reset_runtime_keys(client: redis.Redis) -> None:
    keys = list(
        client.scan_iter(
            "fraud:features:*:00000000-0000-0000-0000-000000000*"
        )
    )
    keys.extend(
        client.scan_iter(
            "fraud:relations:*:00000000-0000-0000-0000-000000000*:*"
        )
    )
    keys.append(STREAM_NAME)
    if keys:
        client.delete(*keys)


def main() -> None:
    redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
    redis_port = int(os.getenv("REDIS_PORT", "63791"))
    cassandra_host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
    cassandra_port = int(os.getenv("CASSANDRA_PORT", "19042"))

    client = redis.Redis(
        host=redis_host, port=redis_port, decode_responses=True
    )
    client.ping()
    reset_runtime_keys(client)

    feature_store = RedisFeatureStore(client)
    event_bus = RedisStreamEventBus(client)
    log_store = CassandraDecisionLogStore(cassandra_host, cassandra_port)
    model = FraudRiskModel()
    scoring = ScoringService(feature_store, event_bus, model)

    payer_safe = UUID("00000000-0000-0000-0000-000000000101")
    payer_risky = UUID("00000000-0000-0000-0000-000000000102")
    card_safe = UUID("00000000-0000-0000-0000-000000000201")
    card_risky = UUID("00000000-0000-0000-0000-000000000202")
    device_safe = UUID("00000000-0000-0000-0000-000000000301")
    device_risky = UUID("00000000-0000-0000-0000-000000000302")
    collector = UUID("00000000-0000-0000-0000-000000000401")

    feature_store.seed(
        "payer", payer_safe, tx_count_5m=1, tx_count_1h=2, amount_sum_1h=120.0
    )
    feature_store.seed(
        "payer",
        payer_risky,
        tx_count_5m=9,
        tx_count_1h=16,
        amount_sum_1h=6_200.0,
        reject_count_24h=5,
        distinct_cards_24h=4,
        distinct_devices_24h=5,
    )
    feature_store.seed("card", card_safe, tx_count_5m=1, amount_sum_1h=120.0)
    feature_store.seed(
        "card", card_risky, tx_count_5m=8, amount_sum_1h=5_800.0,
        distinct_payers_24h=4, distinct_devices_24h=5, reject_count_24h=4,
    )
    feature_store.seed("device", device_safe, tx_count_5m=1, amount_sum_1h=120.0)
    feature_store.seed(
        "device", device_risky, tx_count_5m=11, amount_sum_1h=7_500.0,
        distinct_payers_24h=6, distinct_cards_24h=7, reject_count_24h=5,
    )
    feature_store.seed(
        "collector", collector, tx_count_5m=3, amount_sum_1h=800.0,
        distinct_payers_1h=3, reject_count_24h=0, reject_rate_24h=0.0,
    )

    client.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
    processor = RedisStreamEventProcessor(client, feature_store, log_store)
    worker = threading.Thread(target=processor.run, args=(3,), daemon=True)
    worker.start()

    requests = [
        make_request(
            pagador_id=payer_safe, cobrador_id=collector,
            tarjeta_id=card_safe, dispositivo_id=device_safe, monto="85.50",
        ),
        make_request(
            pagador_id=payer_risky, cobrador_id=collector,
            tarjeta_id=card_risky, dispositivo_id=device_risky, monto="1850.00",
        ),
        make_request(
            pagador_id=payer_safe, cobrador_id=collector,
            tarjeta_id=card_safe, dispositivo_id=device_safe, monto="240.00",
        ),
    ]

    decisions = [scoring.evaluate(request) for request in requests]
    worker.join(timeout=50)
    if worker.is_alive():
        raise TimeoutError("El procesador de eventos no terminó dentro del plazo")
    if processor.error:
        raise processor.error

    print_json(
        "Ejecución real completada",
        {
            "redis_stream": STREAM_NAME,
            "eventos_publicados": client.xlen(STREAM_NAME),
            "eventos_pendientes": client.xpending(
                STREAM_NAME, CONSUMER_GROUP
            )["pending"],
            "decisiones": [
                {
                    "intento_id": decision.intento_id,
                    "score_riesgo": decision.score_riesgo,
                    "decision": decision.decision,
                    "reglas_disparadas": decision.reglas_disparadas,
                }
                for decision in decisions
            ],
            "features_finales_pagador_seguro": feature_store.get_features(
                "payer", payer_safe
            ),
            "persistencia": "Cassandra",
        },
    )
    log_store.close()


if __name__ == "__main__":
    main()
