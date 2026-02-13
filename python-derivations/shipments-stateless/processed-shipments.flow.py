"""Derivation implementation for dani-demo/python-derivations/processed-shipments."""
from collections.abc import AsyncIterator
from dani_demo.python_derivations.processed_shipments import IDerivation, Document, Request

from datetime import datetime, date

# Implementation for derivation dani-demo/python-derivations/processed-shipments.
class Derivation(IDerivation):
    async def shipments(self, read: Request.ReadShipments) -> AsyncIterator[Document]:
        # Simple transformation: combine address fields and add calculated fields
        doc = read.doc

        # Combine street address and city into full address
        full_address = f"{doc.street_address or 'Unknown'}, {doc.city or 'Unknown'}"

        # Determine if urgent (priority shipments or certain statuses)
        is_urgent = doc.is_priority or doc.shipment_status in ["delayed", "critical"]

        # Create status summary
        status_summary = f"{doc.shipment_status or 'unknown'} - {'Priority' if doc.is_priority else 'Standard'}"

        # Calculate days until delivery
        days_until_delivery = 0  # Default to 0 if no delivery date
        if doc.expected_delivery_date:
            expected = datetime.fromisoformat(doc.expected_delivery_date.replace('Z', '+00:00'))
            days_until_delivery = (expected.date() - date.today()).days

        yield Document(
            id=doc.id,
            full_address=full_address,
            is_urgent=is_urgent,
            status_summary=status_summary,
            days_until_delivery=days_until_delivery
        )
