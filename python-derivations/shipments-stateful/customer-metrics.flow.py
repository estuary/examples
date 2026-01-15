"""Stateful derivation that maintains per-customer shipping metrics.

This derivation demonstrates how to:
1. Load persisted state when the derivation initializes
2. Maintain in-memory state as documents are processed  
3. Persist state updates efficiently via start_commit()
4. Handle state recovery across restarts and failures

The key insight is that state is partitioned by customer_id (via shuffle.key),
so each worker only manages state for its assigned subset of customers.
"""
from collections.abc import AsyncIterator
from datetime import datetime, date
from dani_demo.python_derivations.customer_metrics import (
    IDerivation,
    Document,
    Request,
    Response,
    SourceShipments
)
from pydantic import BaseModel, Field


class CustomerState(BaseModel):
    """Metrics we track for each customer.
    
    This model defines the shape of our per-customer state. Using Pydantic
    gives us automatic serialization to/from JSON for persistence, plus
    validation and clear documentation of our state structure.
    """
    total_shipments: int = 0
    on_time_count: int = 0
    late_count: int = 0
    active_shipments: int = 0
    total_delivery_days: int = 0  # Running sum for computing average
    delivered_count: int = 0       # Denominator for average calculation
    last_shipment_date: str | None = None
    
    # Track which shipments we've seen and their current status.
    # This is essential for CDC: when a shipment transitions from
    # "In Transit" to "Delivered", we need to know it was previously
    # active so we can decrement active_shipments correctly.
    known_shipments: dict[int, str] = Field(default_factory=lambda: {})


class State(BaseModel):
    """Root state container mapping customer IDs to their metrics.

    The runtime persists this entire structure. On restart, it's passed
    back to us via Request.Open so we can resume where we left off.
    """
    customers: dict[int, CustomerState] = Field(default_factory=lambda: {})


class Derivation(IDerivation):
    """Derivation that aggregates shipment events into customer profiles."""
    
    # Customers with this many shipments or more are considered VIPs.
    VIP_THRESHOLD = 10
    
    def __init__(self, open: Request.Open):
        """Initialize the derivation and restore persisted state.
        
        The runtime calls this when the derivation starts (or restarts).
        The open.state dictionary contains whatever we returned from
        start_commit() in our previous run. On the very first run,
        open.state is an empty dictionary.
        
        Args:
            open: Contains runtime configuration and persisted state
        """
        super().__init__(open)
        
        # Restore state from the previous transaction. If this is our
        # first run, State() creates empty default structures.
        self.state = State(**open.state)
        
        # Track which customers we've modified in the current transaction.
        # This optimization lets us persist only changed state rather than
        # the entire state dictionary, which matters when you have millions
        # of customers but only thousands change per transaction.
        self.touched_customers: dict[int, CustomerState] = {}
    
    async def shipments(
        self, 
        read: Request.ReadShipments
    ) -> AsyncIterator[Document]:
        """Process a shipment event and update customer metrics.
        
        This method is called for each document from our source collection.
        We update the relevant customer's state in memory, then yield
        their current metrics as an output document.
        
        Args:
            read: Contains the shipment document and metadata
            
        Yields:
            Updated customer metrics document
        """
        doc = read.doc

        # Ensure required fields are not None
        assert doc.customer_id is not None, "customer_id cannot be None"
        assert doc.shipment_status is not None, "shipment_status cannot be None"

        customer_id = doc.customer_id
        shipment_id = doc.id
        current_status = doc.shipment_status

        # Get or create state for this customer. The setdefault pattern
        # handles both existing and new customers elegantly.
        customer = self.state.customers.setdefault(
            customer_id,
            CustomerState()
        )

        # Check if we've seen this shipment before and what its status was.
        # This is crucial for CDC processing—we need to handle transitions
        # like "In Transit" -> "Delivered" without double-counting.
        previous_status = customer.known_shipments.get(shipment_id)

        # Handle the CDC operation type. The _meta.op field tells us whether
        # this is a create, update, or delete operation.
        op = doc.m_meta.op
        
        if op == 'd':
            # Deletion: remove the shipment from our tracking.
            # In practice, you might handle this differently depending
            # on your business logic—perhaps archive rather than forget.
            self._handle_deletion(customer, shipment_id, previous_status)
        else:
            # Create or update: process the shipment event
            self._process_shipment(
                customer,
                shipment_id,
                current_status,
                previous_status,
                doc
            )
        
        # Update the last shipment date if this one is more recent
        shipment_date = doc.created_at[:10] if doc.created_at else None
        if shipment_date:
            if not customer.last_shipment_date or shipment_date > customer.last_shipment_date:
                customer.last_shipment_date = shipment_date
        
        # Mark this customer as touched so start_commit knows to persist them
        self.touched_customers[customer_id] = customer
        
        # Emit the current state of this customer's metrics.
        # Because our collection is keyed by customer_id, this document
        # will replace any previous version for this customer.
        yield self._build_output_document(customer_id, customer)
    
    def _handle_deletion(
        self, 
        customer: CustomerState, 
        shipment_id: int, 
        previous_status: str | None
    ) -> None:
        """Handle a deleted shipment by reversing its contribution to metrics."""
        if previous_status is None:
            # We never saw this shipment, nothing to undo
            return
            
        # Decrement the appropriate counters based on what we knew
        customer.total_shipments = max(0, customer.total_shipments - 1)
        
        if previous_status in ('In Transit', 'At Checkpoint', 'Out for Delivery'):
            customer.active_shipments = max(0, customer.active_shipments - 1)
        elif previous_status == 'Delivered':
            customer.delivered_count = max(0, customer.delivered_count - 1)
            # Note: We can't perfectly undo the delivery time contribution
            # without storing more state. This is a trade-off.
        
        # Remove from our tracking
        del customer.known_shipments[shipment_id]
    
    def _process_shipment(
        self,
        customer: CustomerState,
        shipment_id: int,
        current_status: str,
        previous_status: str | None,
        doc: SourceShipments
    ) -> None:
        """Process a create or update event for a shipment."""
        
        # If this is a new shipment we haven't seen before, increment total
        if previous_status is None:
            customer.total_shipments += 1
        
        # Handle status transitions. The key insight is that we need to
        # properly account for a shipment moving between states.
        is_active_status = current_status in (
            'In Transit', 'At Checkpoint', 'Out for Delivery'
        )
        was_active_status = previous_status in (
            'In Transit', 'At Checkpoint', 'Out for Delivery'
        ) if previous_status else False
        
        # Update active shipment count based on status transition
        if is_active_status and not was_active_status:
            # Became active (new shipment or status changed to active)
            customer.active_shipments += 1
        elif was_active_status and not is_active_status:
            # Was active, now isn't (delivered, cancelled, etc.)
            customer.active_shipments = max(0, customer.active_shipments - 1)
        
        # Handle delivery completion
        if current_status == 'Delivered' and previous_status != 'Delivered':
            customer.delivered_count += 1
            
            # Calculate delivery time and update on-time/late counts
            self._record_delivery(customer, doc)
        
        # Update our tracking of this shipment's status
        customer.known_shipments[shipment_id] = current_status
    
    def _record_delivery(self, customer: CustomerState, doc: SourceShipments) -> None:
        """Record delivery metrics when a shipment is delivered."""
        # Calculate days from creation to now (delivery time)
        if doc.created_at:
            try:
                created = datetime.fromisoformat(
                    doc.created_at.replace('Z', '+00:00')
                )
                delivery_days = (date.today() - created.date()).days
                customer.total_delivery_days += max(0, delivery_days)
            except (ValueError, AttributeError):
                pass  # Skip if date parsing fails
        
        # Determine if delivery was on time
        if doc.expected_delivery_date:
            try:
                expected = datetime.fromisoformat(
                    doc.expected_delivery_date.replace('Z', '+00:00')
                ).date()
                if date.today() <= expected:
                    customer.on_time_count += 1
                else:
                    customer.late_count += 1
            except (ValueError, AttributeError):
                pass  # Skip if date parsing fails
    
    def _build_output_document(
        self, 
        customer_id: int, 
        customer: CustomerState
    ) -> Document:
        """Construct the output document from current customer state."""
        # Calculate average delivery days, handling division by zero
        avg_delivery_days = None
        if customer.delivered_count > 0:
            avg_delivery_days = round(
                customer.total_delivery_days / customer.delivered_count, 
                1
            )
        
        return Document(
            customer_id=customer_id,
            total_shipments=customer.total_shipments,
            on_time_count=customer.on_time_count,
            late_count=customer.late_count,
            active_shipments=customer.active_shipments,
            avg_delivery_days=avg_delivery_days,
            is_vip=customer.total_shipments >= self.VIP_THRESHOLD,
            last_shipment_date=customer.last_shipment_date
        )
    
    def start_commit(
        self, 
        start_commit: Request.StartCommit
    ) -> Response.StartedCommit:
        """Persist state changes at the end of the transaction.
        
        The runtime calls this method when it's ready to commit the current
        transaction. We return the state that should be durably persisted.
        
        The key optimization here is merge_patch=True, which tells the runtime
        to merge our updates with the existing state rather than replacing it
        entirely. This means we only need to return the customers that changed,
        not the entire state dictionary.
        
        Args:
            start_commit: Contains metadata about the commit
            
        Returns:
            State updates to persist, with merge semantics
        """
        # Build the partial state update containing only touched customers.
        # This is a JSON merge patch: keys present in our update replace
        # the corresponding keys in the persisted state; keys we don't
        # mention are left unchanged.
        updated_state = {
            "customers": {
                str(cid): customer.model_dump() 
                for cid, customer in self.touched_customers.items()
            }
        }
        
        # Clear the touched set for the next transaction
        self.touched_customers = {}
        
        return Response.StartedCommit(
            state=Response.StartedCommit.State(
                updated=updated_state,
                merge_patch=True,  # Merge with existing state, don't replace
            )
        )
    
    async def reset(self):
        """Reset all state for testing.
        
        The runtime calls this between catalog tests to ensure each test
        starts with a clean slate. Without this, state from one test would
        leak into the next, causing flaky and confusing test failures.
        """
        self.state = State()
        self.touched_customers = {}