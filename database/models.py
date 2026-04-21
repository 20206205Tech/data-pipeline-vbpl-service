import datetime
import uuid
from typing import Optional

from sqlalchemy import JSON, UUID, DateTime, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class RequestLog(Base):
    __tablename__ = "request_logs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True
    )

    request_id: Mapped[Optional[str]] = mapped_column(
        String(255), index=True, nullable=True
    )
    method: Mapped[str] = mapped_column(String(10))
    url: Mapped[str] = mapped_column(String(255))
    client_ip: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    status_code: Mapped[int] = mapped_column(Integer)

    request_payload: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    response_payload: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    process_time: Mapped[float] = mapped_column(Float)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
