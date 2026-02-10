"""Derivation implementation for dani-demo/python-derivations/shipment-delay-training."""

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Dict, Literal, Optional, TypedDict

from pydantic import BaseModel, Field

from datetime import datetime, time, timezone

from dani_demo.python_derivations.shipment_delay_training import (
    IDerivation,
    Document,
    Request,
    Response,
    SourceShipments,
)

# ---------------------------------------------------------------------------
# Typed helpers
# ---------------------------------------------------------------------------

Label = Literal[0, 1]


class FeatureSnapshot(TypedDict):
    total_shipments: int
    on_time_count: int
    late_count: int
    active_shipments: int
    avg_delivery_days: Optional[float]


# ---------------------------------------------------------------------------
# Stateful models
# ---------------------------------------------------------------------------

class CustomerState(BaseModel):
    total_shipments: int = 0
    on_time_count: int = 0
    late_count: int = 0
    active_shipments: int = 0
    total_delivery_days: int = 0
    delivered_count: int = 0

    # Track per-shipment status to handle CDC transitions correctly
    known_shipments: Dict[int, str] = Field(
        default_factory=lambda: {}  # type: Dict[int, str]
    )


class State(BaseModel):
    customers: Dict[int, CustomerState] = Field(
        default_factory=lambda: {}  # type: Dict[int, CustomerState]
    )


# ---------------------------------------------------------------------------
# Derivation
# ---------------------------------------------------------------------------

class Derivation(IDerivation):
    """Emit append-only training rows for shipment delay prediction."""

    def __init__(self, open: Request.Open):
        super().__init__(open)
        self.state: State = State(**open.state)
        self.touched_customers: set[int] = set()

    async def shipments(
        self,
        read: Request.ReadShipments,
    ) -> AsyncIterator[Document]:
        doc: SourceShipments = read.doc

        # Skip malformed records
        if doc.customer_id is None or doc.shipment_status is None:
            return

        customer_id: int = doc.customer_id
        shipment_id: int = doc.id
        status: str = doc.shipment_status

        customer: CustomerState = self.state.customers.setdefault(
            customer_id,
            CustomerState(),
        )

        previous_status: Optional[str] = customer.known_shipments.get(shipment_id)

        # -------------------------------------------------------------------
        # Snapshot features BEFORE applying this shipment update
        # -------------------------------------------------------------------
        snapshot: FeatureSnapshot = self._snapshot_features(customer)

        # -------------------------------------------------------------------
        # Apply CDC update to state
        # -------------------------------------------------------------------
        if previous_status is None:
            customer.total_shipments += 1

        is_active: bool = status in (
            "In Transit",
            "At Checkpoint",
            "Out for Delivery",
        )
        was_active: bool = (
            previous_status in (
                "In Transit",
                "At Checkpoint",
                "Out for Delivery",
            )
            if previous_status is not None
            else False
        )

        if is_active and not was_active:
            customer.active_shipments += 1
        elif was_active and not is_active:
            customer.active_shipments = max(0, customer.active_shipments - 1)

        delivered_now: bool = False
        if status == "Delivered" and previous_status != "Delivered":
            delivered_now = True
            customer.delivered_count += 1
            self._record_delivery(customer, doc)

        customer.known_shipments[shipment_id] = status
        self.touched_customers.add(customer_id)

        # -------------------------------------------------------------------
        # Emit exactly one training row per delivered shipment
        # -------------------------------------------------------------------
        if delivered_now:
            label: Label = self._late_label(doc)

            yield Document(
                shipment_id=shipment_id,
                customer_id=customer_id,
                total_shipments=snapshot["total_shipments"],
                on_time_count=snapshot["on_time_count"],
                late_count=snapshot["late_count"],
                active_shipments=snapshot["active_shipments"],
                avg_delivery_days=snapshot["avg_delivery_days"],
                label=label,
            )

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _snapshot_features(self, customer: CustomerState) -> FeatureSnapshot:
        avg_delivery_days: Optional[float] = None
        if customer.delivered_count > 0:
            avg_delivery_days = round(
                customer.total_delivery_days / customer.delivered_count,
                1,
            )

        return {
            "total_shipments": customer.total_shipments,
            "on_time_count": customer.on_time_count,
            "late_count": customer.late_count,
            "active_shipments": customer.active_shipments,
            "avg_delivery_days": avg_delivery_days,
        }

    def _record_delivery(
        self,
        customer: CustomerState,
        doc: SourceShipments,
    ) -> None:
        if doc.created_at and doc.updated_at:
            try:
                created = datetime.fromisoformat(
                    doc.created_at.replace("Z", "+00:00")
                )
                delivered = datetime.fromisoformat(
                    doc.updated_at.replace("Z", "+00:00")
                )
                days = (delivered.date() - created.date()).days
                customer.total_delivery_days += max(0, days)
            except Exception:
                pass

        if doc.expected_delivery_date and doc.updated_at:
            try:
                expected_date = datetime.fromisoformat(
                    doc.expected_delivery_date
                ).date()
                delivered_date = datetime.fromisoformat(
                    doc.updated_at.replace("Z", "+00:00")
                ).date()

                if delivered_date <= expected_date:
                    customer.on_time_count += 1
                else:
                    customer.late_count += 1
            except Exception:
                pass

    def _late_label(self, doc: SourceShipments) -> Label:
        if doc.expected_delivery_date is None or doc.updated_at is None:
            return 0

        try:
            # Expected delivery = end of expected date (23:59:59 UTC)
            expected_date = datetime.fromisoformat(doc.expected_delivery_date)
            expected = datetime.combine(
                expected_date.date(),
                time(23, 59, 59),
                tzinfo=timezone.utc,
            )

            # Delivery time approximated by CDC update time
            delivered = datetime.fromisoformat(
                doc.updated_at.replace("Z", "+00:00")
            )

            return 1 if delivered > expected else 0

        except Exception:
            return 0

    # -----------------------------------------------------------------------
    # Commit lifecycle
    # -----------------------------------------------------------------------

    def start_commit(
        self,
        start_commit: Request.StartCommit,
    ) -> Response.StartedCommit:
        updated: Dict[str, Dict[str, object]] = {
            "customers": {
                str(cid): dict(self.state.customers[cid].model_dump())
                for cid in self.touched_customers
            }
        }

        self.touched_customers.clear()

        return Response.StartedCommit(
            state=Response.StartedCommit.State(
                updated=updated,
                merge_patch=True,
            )
        )

    async def reset(self) -> None:
        self.state = State()
        self.touched_customers.clear()