from collections.abc import AsyncIterator
from dani_demo.python_derivations.enriched_shipments import (
    IDerivation,
    Document,
    Request,
    Response
)
from pydantic import BaseModel, Field


class CustomerTier(BaseModel):
    """Reference data for a customer's tier information."""
    tier: str | None = None
    region: str | None = None
    account_manager: str | None = None


class ShipmentData(BaseModel):
    """Core shipment fields we need for the join output."""
    shipment_id: int
    customer_id: int
    shipment_status: str | None = None
    is_priority: bool = False
    city: str | None = None
    expected_delivery_date: str | None = None


class JoinState(BaseModel):
    """State for a single join key (customer_id).

    We store both sides of the join here. For a one-to-many join
    (one customer tier to many shipments), we store the tier once
    and shipments in a dictionary keyed by shipment_id.
    """
    # Right side: customer tier (one per customer)
    tier: CustomerTier | None = None

    # Left side: shipments (many per customer)
    # Keyed by shipment_id for efficient updates
    shipments: dict[int, ShipmentData] = Field(default_factory=lambda: {})


class State(BaseModel):
    """Root state container mapping customer_id to join state."""
    customers: dict[int, JoinState] = Field(default_factory=lambda: {})


class Derivation(IDerivation):
    """Derivation that left-joins shipments with customer tier reference data.

    This implements a LEFT JOIN where:
    - Shipments are always emitted (left side)
    - Customer tier data is enriched when available (right side)
    - Shipments without matching tier data still appear in output
    """

    def __init__(self, open: Request.Open):
        """Initialize and restore persisted state."""
        super().__init__(open)
        self.state = State(**open.state)
        self.touched_customers: set[int] = set()
    
    async def shipments(
        self,
        read: Request.ReadShipments
    ) -> AsyncIterator[Document]:
        """Process shipment events (left side of the join).

        For a left join, we always emit shipments. If we have tier data
        for this customer, we include it. Otherwise, tier fields are null.
        """
        doc = read.doc
        customer_id = doc.customer_id
        shipment_id = doc.id

        # Skip if customer_id is None
        if customer_id is None:
            return

        # Get or create join state for this customer
        join_state = self.state.customers.setdefault(
            customer_id,
            JoinState()
        )

        # Handle deletion: remove the shipment from state
        op = doc.m_meta.op
        if op == 'd':
            join_state.shipments.pop(shipment_id, None)
            self.touched_customers.add(customer_id)
            return  # Don't emit for deletions

        # Store/update the shipment in state
        shipment_data = ShipmentData(
            shipment_id=shipment_id,
            customer_id=customer_id,
            shipment_status=doc.shipment_status,
            is_priority=doc.is_priority or False,
            city=doc.city,
            expected_delivery_date=doc.expected_delivery_date
        )
        join_state.shipments[shipment_id] = shipment_data
        self.touched_customers.add(customer_id)

        # Left join: always emit the shipment with whatever tier data we have
        yield self._build_joined_document(shipment_data, join_state.tier)
    
    async def customer_tiers(
        self,
        read: Request.ReadCustomerTiers
    ) -> AsyncIterator[Document]:
        """Process customer tier updates (right side of the join).

        When tier data arrives or changes, we re-emit all shipments for
        this customer with the updated enrichment data.
        """
        doc = read.doc
        # Convert customer_id from string to int (source schema has it as string)
        customer_id = int(doc.customer_id)

        # Get or create join state for this customer
        join_state = self.state.customers.setdefault(
            customer_id,
            JoinState()
        )
        
        # Handle deletion: clear the tier data
        # (For Google Sheets, this happens when a row is removed)
        op = doc.m_meta.op
        if op == 'd':
            join_state.tier = None
            self.touched_customers.add(customer_id)
            # Re-emit all shipments for this customer without tier data
            for shipment in join_state.shipments.values():
                yield self._build_joined_document(shipment, None)
            return

        # Store/update the tier data
        join_state.tier = CustomerTier(
            tier=doc.tier,
            region=doc.region,
            account_manager=doc.account_manager
        )
        self.touched_customers.add(customer_id)

        # Re-emit all shipments for this customer with the updated tier data
        for shipment in join_state.shipments.values():
            yield self._build_joined_document(shipment, join_state.tier)

    def _build_joined_document(
        self, 
        shipment: ShipmentData, 
        tier: CustomerTier | None
    ) -> Document:
        """Construct the joined output document."""
        return Document(
            shipment_id=shipment.shipment_id,
            customer_id=shipment.customer_id,
            shipment_status=shipment.shipment_status,
            is_priority=shipment.is_priority,
            city=shipment.city,
            expected_delivery_date=shipment.expected_delivery_date,
            # Enrichment fields from tier (null if not available)
            customer_tier=tier.tier if tier else None,
            customer_region=tier.region if tier else None,
            account_manager=tier.account_manager if tier else None
        )
    
    def start_commit(
        self, 
        start_commit: Request.StartCommit
    ) -> Response.StartedCommit:
        """Persist state changes."""
        updated_state = {
            "customers": {
                str(cid): self.state.customers[cid].model_dump()
                for cid in self.touched_customers
                if cid in self.state.customers
            }
        }
        self.touched_customers = set()
        
        return Response.StartedCommit(
            state=Response.StartedCommit.State(
                updated=updated_state,
                merge_patch=True,
            )
        )
    
    async def reset(self):
        """Reset state for testing."""
        self.state = State()
        self.touched_customers = set()