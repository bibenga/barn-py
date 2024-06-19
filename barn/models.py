from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, TIMESTAMP, Boolean, Column, Integer, MetaData, String, Table
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
    locked_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    locked_by: Mapped[Optional[str]] = mapped_column(nullable=True)


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


metadata = MetaData()

lock_table = Table(
    "barn_lock",
    metadata,
    Column("name", String(), key="name", primary_key=True, comment="lock name"),
    Column("locked_at", TIMESTAMP(timezone=True), key="locked_at", nullable=True, comment="when it was captured"),
    Column("locked_by", String(), key="locked_by", nullable=True, comment="who captured it"),
)

barn_entry = Table(
    "barn_entry",
    metadata,
    Column("id", Integer(), primary_key=True),
    Column("name", String(), unique=True),
    Column("cron", String(), nullable=True),
    Column("is_active", Boolean(), default=False, server_default=expression.false()),
    Column("next_ts", TIMESTAMP(timezone=True), nullable=True),
    Column("last_ts", TIMESTAMP(timezone=True), nullable=True),
    Column("message", JSON(none_as_null=True), nullable=True),
    Column("object_type", String(), nullable=True),
    Column("object_id", String(), nullable=True),
)
