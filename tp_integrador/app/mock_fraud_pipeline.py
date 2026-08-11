"""Prototipo en memoria de la arquitectura de puntuación de fraude en línea.

El módulo reproduce los contratos mostrados en los diagramas del proyecto:

    API de pagos -> servicio de puntuación -> Redis Stream -> procesador de eventos
                                                |                   |
                                      variables en Redis   registros en Cassandra

Solo las clases adaptadoras dependen de la infraestructura. En un despliegue
real, ``InMemoryRedisFeatureStore``, ``QueueEventBus`` e
``InMemoryCassandraStore`` pueden reemplazarse por implementaciones basadas en
redis-py y cassandra-driver sin modificar los objetos ni los servicios del
dominio.
"""

from __future__ import annotations

import copy
import json
import random
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from queue import Queue
from typing import Any, Mapping, Protocol
from uuid import UUID, uuid4

from sklearn.ensemble import RandomForestClassifier


UTC = timezone.utc
ENTITY_TYPES = ("payer", "card", "device", "collector")


def utc_now() -> datetime:
    """Devuelve una fecha y hora UTC con información de zona horaria."""

    return datetime.now(UTC)


@dataclass(frozen=True)
class ScoringRequest:
    """Solicitud síncrona recibida desde la aplicación de pagos."""

    intento_id: UUID
    fecha_hora: datetime
    pagador_id: UUID
    cobrador_id: UUID
    tarjeta_id: UUID
    dispositivo_id: UUID
    monto: Decimal
    moneda: str
    direccion_ip: str
    pais_origen: str


@dataclass(frozen=True)
class FeatureSnapshot:
    """Copia inmutable de las variables en línea utilizadas por el modelo."""

    payer_features: dict[str, Any]
    card_features: dict[str, Any]
    device_features: dict[str, Any]
    collector_features: dict[str, Any]
    snapshot_at: datetime


@dataclass(frozen=True)
class ScoringDecision:
    """Respuesta devuelta de forma síncrona a la aplicación de pagos."""

    decision_id: UUID
    intento_id: UUID
    score_riesgo: float
    decision: str
    modelo_version_id: str
    reglas_disparadas: tuple[str, ...]
    fecha_hora: datetime
    latencia_ms: int


@dataclass(frozen=True)
class PaymentScoredEvent:
    """Evento versionado escrito en ``fraud:payment_scored``."""

    event_id: UUID
    event_type: str
    schema_version: int
    occurred_at: datetime
    intento: ScoringRequest
    feature_snapshot: FeatureSnapshot
    decision: ScoringDecision


class FeatureStore(Protocol):
    """Puerto implementado por el adaptador del almacén de variables en Redis."""

    def get_snapshot(self, request: ScoringRequest) -> FeatureSnapshot: ...

    def apply_event(self, event: PaymentScoredEvent) -> None: ...


class EventBus(Protocol):
    """Puerto implementado por Redis Streams (una cola en este prototipo)."""

    def publish(self, event: PaymentScoredEvent) -> None: ...


class DecisionLogStore(Protocol):
    """Puerto implementado por el adaptador de persistencia en Cassandra."""

    def has_event(self, event_id: UUID) -> bool: ...

    def persist_event(self, event: PaymentScoredEvent) -> None: ...


class InMemoryRedisFeatureStore:
    """Simulación con diccionarios de los hashes de variables de fraude en Redis.

    Las claves siguen ``Diagrama Redis_Features.drawio.pdf``. Los conjuntos
    privados modelan contadores de elementos distintos; en producción, Redis
    utilizaría Sets/HyperLogLog y Sorted Sets con TTL para implementar ventanas
    temporales móviles reales.
    """

    DEFAULTS: dict[str, dict[str, Any]] = {
        "payer": {
            "tx_count_5m": 0,
            "tx_count_1h": 0,
            "amount_sum_1h": 0.0,
            "reject_count_24h": 0,
            "distinct_cards_24h": 0,
            "distinct_devices_24h": 0,
            "updated_at": None,
        },
        "card": {
            "tx_count_5m": 0,
            "amount_sum_1h": 0.0,
            "distinct_payers_24h": 0,
            "distinct_devices_24h": 0,
            "reject_count_24h": 0,
            "updated_at": None,
        },
        "device": {
            "tx_count_5m": 0,
            "amount_sum_1h": 0.0,
            "distinct_payers_24h": 0,
            "distinct_cards_24h": 0,
            "reject_count_24h": 0,
            "updated_at": None,
        },
        "collector": {
            "tx_count_5m": 0,
            "amount_sum_1h": 0.0,
            "distinct_payers_1h": 0,
            "reject_count_24h": 0,
            "reject_rate_24h": 0.0,
            "updated_at": None,
        },
    }

    KEY_PREFIXES = {
        "payer": "fraud:features:payer",
        "card": "fraud:features:card",
        "device": "fraud:features:device",
        "collector": "fraud:features:collector",
    }

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, Any]] = {}
        self._distinct: dict[str, set[UUID]] = {}
        self._lock = threading.Lock()

    def _key(self, entity_type: str, entity_id: UUID) -> str:
        if entity_type not in self.KEY_PREFIXES:
            raise ValueError(f"Unsupported entity type: {entity_type}")
        return f"{self.KEY_PREFIXES[entity_type]}:{entity_id}"

    def seed(self, entity_type: str, entity_id: UUID, **features: Any) -> None:
        """Crea o reemplaza un hash con valores ficticios compatibles con el diagrama."""

        unknown = set(features) - set(self.DEFAULTS[entity_type])
        if unknown:
            raise ValueError(f"Unknown {entity_type} features: {sorted(unknown)}")
        values = copy.deepcopy(self.DEFAULTS[entity_type])
        values.update(features)
        values["updated_at"] = values["updated_at"] or utc_now()
        with self._lock:
            self.hashes[self._key(entity_type, entity_id)] = values

    def _get_unlocked(self, entity_type: str, entity_id: UUID) -> dict[str, Any]:
        key = self._key(entity_type, entity_id)
        if key not in self.hashes:
            self.hashes[key] = copy.deepcopy(self.DEFAULTS[entity_type])
        return self.hashes[key]

    def get_features(self, entity_type: str, entity_id: UUID) -> dict[str, Any]:
        """Equivale a Redis HGETALL y devuelve una copia defensiva."""

        with self._lock:
            return copy.deepcopy(self._get_unlocked(entity_type, entity_id))

    def get_snapshot(self, request: ScoringRequest) -> FeatureSnapshot:
        with self._lock:
            return FeatureSnapshot(
                payer_features=copy.deepcopy(
                    self._get_unlocked("payer", request.pagador_id)
                ),
                card_features=copy.deepcopy(
                    self._get_unlocked("card", request.tarjeta_id)
                ),
                device_features=copy.deepcopy(
                    self._get_unlocked("device", request.dispositivo_id)
                ),
                collector_features=copy.deepcopy(
                    self._get_unlocked("collector", request.cobrador_id)
                ),
                snapshot_at=utc_now(),
            )

    def _remember_distinct(
        self,
        owner_type: str,
        owner_id: UUID,
        related_type: str,
        related_id: UUID,
        current_count: int,
    ) -> int:
        distinct_key = f"{owner_type}:{owner_id}:{related_type}"
        members = self._distinct.setdefault(distinct_key, set())
        members.add(related_id)
        # Los datos iniciales solo contienen conteos agregados, no los ID
        # originales. Se conserva esa base histórica y se agregan los ID
        # observados durante esta ejecución.
        return max(current_count, len(members))

    @staticmethod
    def _increment_common(
        values: dict[str, Any], amount: float, rejected: bool, timestamp: datetime
    ) -> None:
        values["tx_count_5m"] += 1
        if "tx_count_1h" in values:
            values["tx_count_1h"] += 1
        values["amount_sum_1h"] = round(values["amount_sum_1h"] + amount, 2)
        values["reject_count_24h"] += int(rejected)
        values["updated_at"] = timestamp

    def apply_event(self, event: PaymentScoredEvent) -> None:
        """Actualiza el hash de cada entidad después de consumir el evento."""

        request = event.intento
        amount = float(request.monto)
        rejected = event.decision.decision == "RECHAZADO"
        timestamp = event.occurred_at

        with self._lock:
            payer = self._get_unlocked("payer", request.pagador_id)
            card = self._get_unlocked("card", request.tarjeta_id)
            device = self._get_unlocked("device", request.dispositivo_id)
            collector = self._get_unlocked("collector", request.cobrador_id)

            for values in (payer, card, device, collector):
                self._increment_common(values, amount, rejected, timestamp)

            payer["distinct_cards_24h"] = self._remember_distinct(
                "payer",
                request.pagador_id,
                "card",
                request.tarjeta_id,
                payer["distinct_cards_24h"],
            )
            payer["distinct_devices_24h"] = self._remember_distinct(
                "payer",
                request.pagador_id,
                "device",
                request.dispositivo_id,
                payer["distinct_devices_24h"],
            )
            card["distinct_payers_24h"] = self._remember_distinct(
                "card",
                request.tarjeta_id,
                "payer",
                request.pagador_id,
                card["distinct_payers_24h"],
            )
            card["distinct_devices_24h"] = self._remember_distinct(
                "card",
                request.tarjeta_id,
                "device",
                request.dispositivo_id,
                card["distinct_devices_24h"],
            )
            device["distinct_payers_24h"] = self._remember_distinct(
                "device",
                request.dispositivo_id,
                "payer",
                request.pagador_id,
                device["distinct_payers_24h"],
            )
            device["distinct_cards_24h"] = self._remember_distinct(
                "device",
                request.dispositivo_id,
                "card",
                request.tarjeta_id,
                device["distinct_cards_24h"],
            )
            collector["distinct_payers_1h"] = self._remember_distinct(
                "collector",
                request.cobrador_id,
                "payer",
                request.pagador_id,
                collector["distinct_payers_1h"],
            )
            collector["reject_rate_24h"] = round(
                collector["reject_count_24h"] / max(collector["tx_count_5m"], 1),
                4,
            )


class QueueEventBus:
    """Reemplazo con una cola del Redis Stream ``fraud:payment_scored``."""

    stream_name = "fraud:payment_scored"
    consumer_group = "fraud-processors"

    def __init__(self) -> None:
        self.queue: Queue[PaymentScoredEvent | None] = Queue()

    def publish(self, event: PaymentScoredEvent) -> None:
        self.queue.put(event)


class InMemoryCassandraStore:
    """Simulación con listas de tablas Cassandra de solo anexado orientadas a consultas."""

    def __init__(self) -> None:
        self.intentos_pago: list[dict[str, Any]] = []
        self.decisiones_scoring: list[dict[str, Any]] = []
        self.evaluaciones_features: list[dict[str, Any]] = []
        self.payment_decision_log_by_day: list[dict[str, Any]] = []
        self._processed_event_ids: set[UUID] = set()
        self._lock = threading.Lock()

    def has_event(self, event_id: UUID) -> bool:
        with self._lock:
            return event_id in self._processed_event_ids

    def persist_event(self, event: PaymentScoredEvent) -> None:
        """Persiste las vistas desnormalizadas de los diagramas de Cassandra."""

        request = asdict(event.intento)
        decision = asdict(event.decision)
        snapshot = asdict(event.feature_snapshot)
        with self._lock:
            if event.event_id in self._processed_event_ids:
                return

            self.intentos_pago.append(copy.deepcopy(request))
            self.decisiones_scoring.append(copy.deepcopy(decision))
            self.evaluaciones_features.append(
                {
                    "decision_id": event.decision.decision_id,
                    "feature_snapshot": copy.deepcopy(snapshot),
                }
            )
            self.payment_decision_log_by_day.append(
                {
                    "fecha_particion": event.occurred_at.date().isoformat(),
                    "bucket": event.occurred_at.hour,
                    "fecha_hora": event.occurred_at,
                    "event_id": event.event_id,
                    **copy.deepcopy(request),
                    **copy.deepcopy(decision),
                    "feature_snapshot": copy.deepcopy(snapshot),
                }
            )
            self._processed_event_ids.add(event.event_id)


class FraudRiskModel:
    """Random Forest determinista entrenado con datos sintéticos de demostración."""

    FEATURE_NAMES = (
        "amount",
        "payer_tx_5m",
        "payer_amount_1h",
        "payer_rejects_24h",
        "card_tx_5m",
        "card_rejects_24h",
        "device_tx_5m",
        "device_distinct_payers_24h",
        "device_rejects_24h",
        "collector_tx_5m",
        "collector_reject_rate_24h",
    )

    def __init__(self, model_version: str = "rf-demo-v1") -> None:
        self.model_version = model_version
        self.classifier = RandomForestClassifier(
            n_estimators=160,
            max_depth=7,
            min_samples_leaf=3,
            class_weight="balanced",
            random_state=42,
        )
        features, labels = self._synthetic_training_data()
        self.classifier.fit(features, labels)

    @staticmethod
    def _synthetic_training_data() -> tuple[list[list[float]], list[int]]:
        """Genera datos reproducibles; no es un flujo de entrenamiento productivo."""

        rng = random.Random(42)
        rows: list[list[float]] = []
        labels: list[int] = []
        for _ in range(1_500):
            amount = rng.uniform(5, 2_500)
            payer_tx = rng.randint(0, 15)
            payer_amount = rng.uniform(0, 8_000)
            payer_rejects = rng.randint(0, 8)
            card_tx = rng.randint(0, 12)
            card_rejects = rng.randint(0, 7)
            device_tx = rng.randint(0, 16)
            device_payers = rng.randint(0, 10)
            device_rejects = rng.randint(0, 8)
            collector_tx = rng.randint(0, 40)
            collector_reject_rate = rng.random() * 0.65

            risk_signal = (
                amount / 2_000
                + payer_tx / 12
                + payer_amount / 7_000
                + payer_rejects / 5
                + card_rejects / 5
                + device_tx / 14
                + device_payers / 7
                + device_rejects / 5
                + collector_reject_rate * 2.2
                + rng.uniform(-0.65, 0.65)
            )
            label = int(risk_signal >= 3.7)
            rows.append(
                [
                    amount,
                    payer_tx,
                    payer_amount,
                    payer_rejects,
                    card_tx,
                    card_rejects,
                    device_tx,
                    device_payers,
                    device_rejects,
                    collector_tx,
                    collector_reject_rate,
                ]
            )
            labels.append(label)
        return rows, labels

    @staticmethod
    def _vector(request: ScoringRequest, snapshot: FeatureSnapshot) -> list[float]:
        payer = snapshot.payer_features
        card = snapshot.card_features
        device = snapshot.device_features
        collector = snapshot.collector_features
        return [
            float(request.monto),
            float(payer["tx_count_5m"]),
            float(payer["amount_sum_1h"]),
            float(payer["reject_count_24h"]),
            float(card["tx_count_5m"]),
            float(card["reject_count_24h"]),
            float(device["tx_count_5m"]),
            float(device["distinct_payers_24h"]),
            float(device["reject_count_24h"]),
            float(collector["tx_count_5m"]),
            float(collector["reject_rate_24h"]),
        ]

    def predict_risk(
        self, request: ScoringRequest, snapshot: FeatureSnapshot
    ) -> float:
        probability = self.classifier.predict_proba([self._vector(request, snapshot)])[0]
        fraud_index = list(self.classifier.classes_).index(1)
        return round(float(probability[fraud_index]), 4)


class ScoringService:
    """Obtiene variables en línea, calcula la puntuación y publica un evento."""

    def __init__(
        self,
        feature_store: FeatureStore,
        event_bus: EventBus,
        model: FraudRiskModel,
        rejection_threshold: float = 0.55,
    ) -> None:
        self.feature_store = feature_store
        self.event_bus = event_bus
        self.model = model
        self.rejection_threshold = rejection_threshold

    @staticmethod
    def _triggered_rules(
        request: ScoringRequest, snapshot: FeatureSnapshot
    ) -> tuple[str, ...]:
        rules: list[str] = []
        if request.monto >= Decimal("1000"):
            rules.append("RF001_HIGH_AMOUNT")
        if snapshot.payer_features["tx_count_5m"] >= 5:
            rules.append("RF002_PAYER_VELOCITY")
        if snapshot.device_features["distinct_payers_24h"] >= 3:
            rules.append("RF003_SHARED_DEVICE")
        if snapshot.collector_features["reject_rate_24h"] >= 0.25:
            rules.append("RF004_COLLECTOR_REJECT_RATE")
        return tuple(rules)

    def evaluate(self, request: ScoringRequest) -> ScoringDecision:
        started = time.perf_counter()
        snapshot = self.feature_store.get_snapshot(request)
        risk_score = self.model.predict_risk(request, snapshot)
        decision = ScoringDecision(
            decision_id=uuid4(),
            intento_id=request.intento_id,
            score_riesgo=risk_score,
            decision=(
                "RECHAZADO" if risk_score >= self.rejection_threshold else "APROBADO"
            ),
            modelo_version_id=self.model.model_version,
            reglas_disparadas=self._triggered_rules(request, snapshot),
            fecha_hora=utc_now(),
            latencia_ms=max(1, round((time.perf_counter() - started) * 1_000)),
        )
        self.event_bus.publish(
            PaymentScoredEvent(
                event_id=uuid4(),
                event_type="PaymentScoredEvent",
                schema_version=1,
                occurred_at=utc_now(),
                intento=request,
                feature_snapshot=snapshot,
                decision=decision,
            )
        )
        return decision


class EventProcessor:
    """Proceso consumidor que actualiza Redis y persiste vistas en Cassandra."""

    def __init__(
        self,
        event_bus: QueueEventBus,
        feature_store: FeatureStore,
        log_store: DecisionLogStore,
    ) -> None:
        self.event_bus = event_bus
        self.feature_store = feature_store
        self.log_store = log_store

    def run(self) -> None:
        while True:
            event = self.event_bus.queue.get()
            try:
                if event is None:
                    return
                if self.log_store.has_event(event.event_id):
                    continue
                self.feature_store.apply_event(event)
                self.log_store.persist_event(event)
                # El queue.task_done() de abajo representa el XACK de Redis Streams.
            finally:
                self.event_bus.queue.task_done()


def make_request(
    *,
    pagador_id: UUID,
    cobrador_id: UUID,
    tarjeta_id: UUID,
    dispositivo_id: UUID,
    monto: str,
) -> ScoringRequest:
    return ScoringRequest(
        intento_id=uuid4(),
        fecha_hora=utc_now(),
        pagador_id=pagador_id,
        cobrador_id=cobrador_id,
        tarjeta_id=tarjeta_id,
        dispositivo_id=dispositivo_id,
        monto=Decimal(monto),
        moneda="ARS",
        direccion_ip="190.18.10.25",
        pais_origen="AR",
    )


def print_json(title: str, value: Mapping[str, Any]) -> None:
    print(f"\n{title}")
    print(json.dumps(value, indent=2, ensure_ascii=False, default=str))


def main() -> None:
    redis_features = InMemoryRedisFeatureStore()
    redis_stream = QueueEventBus()
    cassandra = InMemoryCassandraStore()
    model = FraudRiskModel()
    scoring = ScoringService(redis_features, redis_stream, model)
    processor = EventProcessor(redis_stream, redis_features, cassandra)

    # Los ID estables facilitan el seguimiento de la salida de demostración.
    payer_safe = UUID("00000000-0000-0000-0000-000000000101")
    payer_risky = UUID("00000000-0000-0000-0000-000000000102")
    card_safe = UUID("00000000-0000-0000-0000-000000000201")
    card_risky = UUID("00000000-0000-0000-0000-000000000202")
    device_safe = UUID("00000000-0000-0000-0000-000000000301")
    device_risky = UUID("00000000-0000-0000-0000-000000000302")
    collector = UUID("00000000-0000-0000-0000-000000000401")

    redis_features.seed(
        "payer", payer_safe, tx_count_5m=1, tx_count_1h=2, amount_sum_1h=120.0
    )
    redis_features.seed(
        "payer",
        payer_risky,
        tx_count_5m=9,
        tx_count_1h=16,
        amount_sum_1h=6_200.0,
        reject_count_24h=5,
        distinct_cards_24h=4,
        distinct_devices_24h=5,
    )
    redis_features.seed("card", card_safe, tx_count_5m=1, amount_sum_1h=120.0)
    redis_features.seed(
        "card",
        card_risky,
        tx_count_5m=8,
        amount_sum_1h=5_800.0,
        distinct_payers_24h=4,
        distinct_devices_24h=5,
        reject_count_24h=4,
    )
    redis_features.seed("device", device_safe, tx_count_5m=1, amount_sum_1h=120.0)
    redis_features.seed(
        "device",
        device_risky,
        tx_count_5m=11,
        amount_sum_1h=7_500.0,
        distinct_payers_24h=6,
        distinct_cards_24h=7,
        reject_count_24h=5,
    )
    redis_features.seed(
        "collector",
        collector,
        tx_count_5m=3,
        amount_sum_1h=800.0,
        distinct_payers_1h=3,
        reject_count_24h=0,
        reject_rate_24h=0.0,
    )

    worker = threading.Thread(target=processor.run, name="fraud-event-processor")
    worker.start()

    requests = [
        make_request(
            pagador_id=payer_safe,
            cobrador_id=collector,
            tarjeta_id=card_safe,
            dispositivo_id=device_safe,
            monto="85.50",
        ),
        make_request(
            pagador_id=payer_risky,
            cobrador_id=collector,
            tarjeta_id=card_risky,
            dispositivo_id=device_risky,
            monto="1850.00",
        ),
        make_request(
            pagador_id=payer_safe,
            cobrador_id=collector,
            tarjeta_id=card_safe,
            dispositivo_id=device_safe,
            monto="240.00",
        ),
    ]

    print("=== Simulación de scoring de pagos ===")
    for number, request in enumerate(requests, start=1):
        decision = scoring.evaluate(request)
        print_json(
            f"Intento {number} - respuesta síncrona",
            {
                "intento_id": request.intento_id,
                "monto": request.monto,
                **asdict(decision),
            },
        )
        # El procesador continúa ejecutándose en otro hilo; join solo hace que
        # esta demostración por CLI sea determinista y permite que la siguiente
        # puntuación observe los datos actualizados.
        redis_stream.queue.join()

    redis_stream.queue.put(None)
    redis_stream.queue.join()
    worker.join()

    print_json(
        "Redis final - features del pagador seguro",
        redis_features.get_features("payer", payer_safe),
    )
    print_json(
        "Persistencia final - resumen Cassandra",
        {
            "intentos_pago": len(cassandra.intentos_pago),
            "decisiones_scoring": len(cassandra.decisiones_scoring),
            "evaluaciones_features": len(cassandra.evaluaciones_features),
            "payment_decision_log_by_day": len(
                cassandra.payment_decision_log_by_day
            ),
            "decisiones": [
                {
                    "intento_id": row["intento_id"],
                    "score_riesgo": row["score_riesgo"],
                    "decision": row["decision"],
                    "reglas_disparadas": row["reglas_disparadas"],
                }
                for row in cassandra.payment_decision_log_by_day
            ],
        },
    )


if __name__ == "__main__":
    main()
