import logging
import time
from datetime import UTC, datetime, timedelta

from croniter import croniter
from sqlalchemy import delete, insert, select, update
from sqlalchemy.orm import Session, sessionmaker

from barn_old.models import Entry

log = logging.getLogger(__name__)


class Scheduler:
    def __init__(
        self,
        session_ctx: sessionmaker[Session],
    ) -> None:
        self._session_ctx = session_ctx
        self._entries = dict[int, Entry]()
        self._reload_entry = Entry(name="<reload>", cron="* * * * * */10")
        self._calculate_next_ts(self._reload_entry)

    def add(self, name: str, cron: str | None = None, next_ts: datetime | None = None) -> None:
        with self._session_ctx() as session:
            stmt = insert(Entry).values(
                name=name,
                cron=cron,
                next_ts=next_ts,
                message={}
            ).returning(Entry)
            res = session.execute(stmt)
            e: Entry = res.one().Entry
            log.info("added: id=%s", e.id)
            session.commit()

    def delete_all(self) -> None:
        with self._session_ctx() as session:
            stmt = delete(Entry)
            res = session.execute(stmt)
            log.info("deleted: rowcount=%s", res.rowcount)
            session.commit()

    def _update(self, entry: Entry) -> None:
        with self._session_ctx() as session:
            stmt = update(Entry).where(
                Entry.id == entry.id
            ).values(
                is_active=entry.is_active,
                next_ts=entry.next_ts,
                last_ts=entry.last_ts,
            )
            res = session.execute(stmt)
            log.info("added: id=%r, rowcount=%s", entry.id, res.rowcount)
            session.commit()

    def _deactivate(self, entry: Entry) -> None:
        entry.is_active = False
        with self._session_ctx() as session:
            stmt = update(Entry).where(
                Entry.id == entry.id
            ).values(
                is_active=False,
            )
            res = session.execute(stmt)
            log.info("added: id=%r, rowcount=%s", entry.id, res.rowcount)
            session.commit()

    def run(self) -> None:
        self._reload()

        while True:
            entry = self._get_next()
            if entry.next_ts is None:
                return

            now = datetime.now(UTC)
            if entry.next_ts > now:
                sleep_seconds: timedelta = entry.next_ts - now
                log.info("entry: id=%r, sleep_seconds=%s", entry.id, sleep_seconds)
                time.sleep(sleep_seconds.total_seconds())

            if entry is self._reload_entry:
                log.info("process reload")
                # self._reload()
            else:
                message = entry.message or {}
                message["_meta"] = {
                    "id": entry.id,
                    "name": entry.name,
                    "time": entry.next_ts.isoformat()
                }
                log.info("process entry: id=%r, name=%r, message=%r", entry.id, entry.name, message)

            if entry.cron:
                # now = datetime.now(UTC)
                iter = croniter(entry.cron, entry.next_ts)
                entry.last_ts = entry.next_ts
                entry.next_ts = iter.get_next(datetime)
                if entry is not self._reload_entry:
                    self._update(entry)
                log.info("set next_ts entry: id=%r, next_ts=%s", entry.id, entry.next_ts)
            else:
                if entry is self._reload_entry:
                    raise RuntimeError("wtf1")
                entry.last_ts = entry.next_ts
                entry.is_active = False
                self._update(entry)
                del self._entries[entry.id]

    def _reload(self) -> None:
        log.info("reload")
        newEntries = self._get_entries()

        newKeys = set(newEntries.keys())
        keys = set(self._entries.keys())

        added = newKeys - keys
        changedOrNot = newKeys & keys
        changed = set[int]()
        deleted = keys - newKeys

        for entry_id in added:
            self._entries[entry_id] = newEntries[entry_id]

        for entry_id in changedOrNot:
            entry = self._entries[entry_id]
            newEntry = newEntries[entry_id]
            if self._is_changed(entry, newEntry):
                self._entries[entry_id] = newEntries[entry_id]
                changed.add(entry_id)
            else:
                entry.name = newEntry.name
                entry.message = newEntry.message

        for entry_id in deleted:
            del self._entries[entry_id]

        log.info("all: %r, added: %r; changed: %r; deleted=%r",
                 len(self._entries), added, changed, deleted)

    def _get_entries(self) -> dict[int, Entry]:
        entries = dict[int, Entry]()
        with self._session_ctx() as session:
            stmt = select(Entry)
            res = session.scalars(stmt)
            for entry in res:
                session.expunge(entry)
                log.debug("found entry: id=%r, is_active=%r, cron=%r, next_ts=%r",
                          entry.id, entry.is_active, entry.cron, entry.next_ts)
                if entry.is_active and (entry.cron or entry.next_ts):
                    if entry.cron and not entry.next_ts:
                        now = datetime.now(UTC)
                        iter = croniter(entry.cron, now)
                        entry.next_ts = iter.get_next(datetime)
                        log.info("set next_ts entry: id=%r, next_ts=%s", entry.id, entry.next_ts)
                        stmt = update(Entry).where(
                            Entry.id == entry.id
                        ).values(
                            next_ts=entry.next_ts,
                        )
                    entries[entry.id] = entry
            session.commit()
        log.info("found %d entries", len(entries))
        return entries

    def _is_changed(self, e1: Entry, e2: Entry) -> bool:
        if e1.cron != e2.cron:
            return True
        if e1.next_ts != e2.next_ts:
            return True
        return False

    def _get_next(self) -> Entry:
        # entry: Entry | None = None
        entry = self._reload_entry
        for e in self._entries.values():
            if e.next_ts and entry.next_ts and e.next_ts < entry.next_ts:
                entry = e
        return entry

    def _calculate_next_ts(self, entry: Entry) -> None:
        if entry.cron:
            base = entry.next_ts or datetime.now(UTC)
            iter = croniter(entry.cron, base)
            entry.next_ts = iter.get_next(datetime)
            log.info("set next_ts entry: id=%r, next_ts=%s", entry.id, entry.next_ts)
