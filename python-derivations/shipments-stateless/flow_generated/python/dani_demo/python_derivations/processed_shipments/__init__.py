from abc import ABC, abstractmethod
import typing
import collections.abc
import pydantic


# Generated for published documents of derived collection dani-demo/python-derivations/processed-shipments
class Document(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra='allow')

    days_until_delivery: typing.Optional[int] = None
    full_address: str
    id: int
    is_urgent: typing.Optional[bool] = None
    status_summary: str



# Generated for read documents of sourced collection Artificial-Industries/postgres-shipments/public/shipments
class SourceShipmentsPublicShipments(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra='allow')

    city: typing.Optional[typing.Union[str, None]] = None
    """(source type: varchar)"""
    created_at: typing.Optional[typing.Union[str, None]] = None
    """(source type: timestamp)"""
    current_location: typing.Optional[typing.Any] = None
    """(source type: composite)"""
    customer_id: typing.Optional[typing.Union[int, None]] = None
    """(source type: int4)"""
    delivery_coordinates: typing.Optional[typing.Any] = None
    """(source type: composite)"""
    delivery_name: typing.Optional[typing.Union[str, None]] = None
    """(source type: varchar)"""
    expected_delivery_date: typing.Optional[typing.Union[str, None]] = None
    """(source type: date)"""
    id: int
    """(source type: non-nullable int4)"""
    is_priority: typing.Optional[typing.Union[bool, None]] = None
    """(source type: bool)"""
    order_id: typing.Optional[typing.Union[str, None]] = None
    """(source type: uuid)"""
    shipment_status: typing.Optional[typing.Union[str, None]] = None
    """(source type: enum)"""
    street_address: typing.Optional[typing.Union[str, None]] = None
    """(source type: varchar)"""
    updated_at: typing.Optional[typing.Union[str, None]] = None
    """(source type: timestamp)"""

class SourceShipments(pydantic.BaseModel):
    class MMeta(pydantic.BaseModel):
        class Before(pydantic.BaseModel):
            """Record state immediately before this change was applied."""
            model_config = pydantic.ConfigDict(extra='allow')

            city: typing.Optional[typing.Union[str, None]] = None
            """(source type: varchar)"""
            created_at: typing.Optional[typing.Union[str, None]] = None
            """(source type: timestamp)"""
            current_location: typing.Optional[typing.Any] = None
            """(source type: composite)"""
            customer_id: typing.Optional[typing.Union[int, None]] = None
            """(source type: int4)"""
            delivery_coordinates: typing.Optional[typing.Any] = None
            """(source type: composite)"""
            delivery_name: typing.Optional[typing.Union[str, None]] = None
            """(source type: varchar)"""
            expected_delivery_date: typing.Optional[typing.Union[str, None]] = None
            """(source type: date)"""
            id: int
            """(source type: non-nullable int4)"""
            is_priority: typing.Optional[typing.Union[bool, None]] = None
            """(source type: bool)"""
            order_id: typing.Optional[typing.Union[str, None]] = None
            """(source type: uuid)"""
            shipment_status: typing.Optional[typing.Union[str, None]] = None
            """(source type: enum)"""
            street_address: typing.Optional[typing.Union[str, None]] = None
            """(source type: varchar)"""
            updated_at: typing.Optional[typing.Union[str, None]] = None
            """(source type: timestamp)"""

        class Source(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(extra='allow')

            loc: tuple[int, int, int]
            """Location of this WAL event as [last Commit.EndLSN; event LSN; current Begin.FinalLSN]. See https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html"""
            schema: str
            """Database schema (namespace) of the event."""
            snapshot: typing.Optional[bool] = None
            """Snapshot is true if the record was produced from an initial table backfill and unset if produced from the replication log."""
            table: str
            """Database table of the event."""
            ts_ms: typing.Optional[int] = None
            """Unix timestamp (in millis) at which this event was recorded by the database."""
            txid: typing.Optional[int] = None
            """The 32-bit transaction ID assigned by Postgres to the commit which produced this change."""

        model_config = pydantic.ConfigDict(extra='allow')

        before: typing.Optional["Before"] = None
        """Record state immediately before this change was applied."""
        op: typing.Literal["c", "d", "u"]
        """Change operation type: 'c' Create/Insert, 'u' Update, 'd' Delete."""
        source: "Source"

    model_config = pydantic.ConfigDict(extra='allow')

    m_meta: "MMeta" = pydantic.Field(alias="""_meta""")
    city: typing.Optional[typing.Union[str, None]] = None
    """(source type: varchar)"""
    created_at: typing.Optional[typing.Union[str, None]] = None
    """(source type: timestamp)"""
    current_location: typing.Optional[typing.Any] = None
    """(source type: composite)"""
    customer_id: typing.Optional[typing.Union[int, None]] = None
    """(source type: int4)"""
    delivery_coordinates: typing.Optional[typing.Any] = None
    """(source type: composite)"""
    delivery_name: typing.Optional[typing.Union[str, None]] = None
    """(source type: varchar)"""
    expected_delivery_date: typing.Optional[typing.Union[str, None]] = None
    """(source type: date)"""
    id: int
    """(source type: non-nullable int4)"""
    is_priority: typing.Optional[typing.Union[bool, None]] = None
    """(source type: bool)"""
    order_id: typing.Optional[typing.Union[str, None]] = None
    """(source type: uuid)"""
    shipment_status: typing.Optional[typing.Union[str, None]] = None
    """(source type: enum)"""
    street_address: typing.Optional[typing.Union[str, None]] = None
    """(source type: varchar)"""
    updated_at: typing.Optional[typing.Union[str, None]] = None
    """(source type: timestamp)"""



class Request(pydantic.BaseModel):

    class Open(pydantic.BaseModel):
        state: dict[str, typing.Any]

    class Flush(pydantic.BaseModel):
        pass

    class Reset(pydantic.BaseModel):
        pass

    class StartCommit(pydantic.BaseModel):
        runtime_checkpoint: typing.Any = pydantic.Field(default=None, alias='runtimeCheckpoint')

    open: typing.Optional[Open] = None
    flush: typing.Optional[Flush] = None
    reset: typing.Optional[Reset] = None
    start_commit: typing.Optional[StartCommit] = pydantic.Field(default=None, alias='startCommit')


    class ReadShipments(pydantic.BaseModel):
        doc: SourceShipments
        transform: typing.Literal[0]

    read : typing.Annotated[ReadShipments, pydantic.Field(discriminator='transform')] | None = None

    @pydantic.model_validator(mode='before')
    @classmethod
    def inject_default_transform(cls, data: dict[str, typing.Any]) -> dict[str, typing.Any]:
        if 'read' in data and 'transform' not in data['read']:
            data['read']['transform'] = 0 # Make implicit default explicit
        return data


class Response(pydantic.BaseModel):
    class Opened(pydantic.BaseModel):
        pass

    class Published(pydantic.BaseModel):
        doc: Document

    class Flushed(pydantic.BaseModel):
        pass

    class StartedCommit(pydantic.BaseModel):

        class State(pydantic.BaseModel):
            updated: dict[str, typing.Any]
            merge_patch: bool = False

        state: typing.Optional[State] = None

    opened: typing.Optional[Opened] = None
    published: typing.Optional[Published] = None
    flushed: typing.Optional[Flushed] = None
    started_commit: typing.Optional[StartedCommit] = pydantic.Field(default=None, alias='startedCommit')

class IDerivation(ABC):
    """Abstract base class for derivation implementations."""

    def __init__(self, open: Request.Open):
        """Initialize the derivation with an Open message."""
        pass

    @abstractmethod
    async def shipments(self, read: Request.ReadShipments) -> collections.abc.AsyncIterator[Document]:
        """Transform method for 'shipments' source."""
        if False:
            yield  # Mark as a generator.

    async def flush(self) -> collections.abc.AsyncIterator[Document]:
        """Flush any buffered documents. Override to implement pipelining."""
        if False:
            yield  # Mark as a generator.

    def start_commit(self, start_commit: Request.StartCommit) -> Response.StartedCommit:
        """Return state updates to persist. Override to implement stateful derivations."""
        return Response.StartedCommit()

    async def reset(self):
        """Reset internal state for testing. Override if needed."""
        pass
