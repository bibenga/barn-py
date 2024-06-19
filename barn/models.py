from datetime import datetime, UTC
from typing import Any, Optional

from sqlalchemy import JSON, TIMESTAMP, func
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import expression


class Base(DeclarativeBase):
    pass


# CREATE TABLE barn_lock (
#         id VARCHAR NOT NULL,
#         locked_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
#         locked_by VARCHAR NOT NULL,
#         PRIMARY KEY (id)
# )
class Lock(Base):
    @declared_attr
    def __tablename__(cls):
        return "barn_lock"

    # __tablename__ = "barn_lock"
    __table_args__ = {}

    name: Mapped[str] = mapped_column(primary_key=True)
    locked_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC),
                                                server_default=func.current_timestamp())
    locked_by: Mapped[str] = mapped_column()


class Entry(Base):
    @declared_attr
    def __tablename__(cls):
        return "barn_entry"

    # __tablename__ = "barn_entry"
    __table_args__ = {}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)
    cron: Mapped[Optional[str]]
    is_active: Mapped[bool] = mapped_column(default=True, server_default=expression.false())
    next_ts: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    last_ts: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    message: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON(none_as_null=True))