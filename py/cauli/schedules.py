"""Schedule types for cauli's periodic scheduler (PROTOCOL.md section 10).

Two kinds:

- :func:`interval` -- fire every N seconds, on the phase established by the
  first slot (so a beat restart does not shift the cadence).
- :func:`crontab` -- POSIX cron-style wall-clock schedule in an explicit IANA
  timezone.

Every schedule is a **pure function from one absolute instant to the next**:
``next_after(slot_ms) -> slot_ms'`` with ``slot_ms' > slot_ms``, in unix epoch
MILLISECONDS. Nothing in this module reads the clock, and no schedule carries
mutable state.

That purity is load-bearing, not stylistic. It is what makes exactly-once
firing across beat replicas possible: two replicas whose clocks disagree still
compute the *same* next slot from the same previous slot, so the
compare-and-set in :mod:`cauli.beat` (advance the stored slot from S to S'
only if it is still S) can only ever let one of them win. If the next slot
depended on ``now()``, two replicas would compute two different S' values and
the CAS would stop being a mutual exclusion.

Timezones are explicit. A crontab has an IANA timezone name (default
``"UTC"``); wall-clock arithmetic happens in that zone via :mod:`zoneinfo`,
and only the resulting absolute instant is ever stored or compared.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date as _date
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

__all__ = [
    "Schedule",
    "IntervalSchedule",
    "CrontabSchedule",
    "ScheduleEntry",
    "interval",
    "crontab",
    "schedule_from_spec",
]

#: Missed-slot policies (PROTOCOL.md section 10.4).
ON_MISSED = ("fire_once", "skip")

_MONTH_NAMES = {
    n: i
    for i, n in enumerate(
        [
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
        ],
        start=1,
    )
}
_DOW_NAMES = {
    n: i for i, n in enumerate(["sun", "mon", "tue", "wed", "thu", "fri", "sat"])
}

# Bound on CrontabSchedule's forward day scan. Eight years covers the worst
# legitimate expression (Feb 29 lands at most 8 years out across a century
# boundary, e.g. 2096 -> 2104); anything past it is an expression that can
# never match at all, such as "0 0 30 2 *" (February 30th).
_MAX_SCAN_DAYS = 366 * 8

# Bound on the missed-slot fast-forward loop in `advance_past`. A crontab slot
# is at least one minute wide, so this covers ~7 days of beat downtime before
# the loop gives up and jumps straight to `next_after(now)`.
_MAX_ADVANCE_STEPS = 10_000


def _canonical(spec: dict[str, Any]) -> str:
    """Deterministic text form of a schedule spec, for :meth:`Schedule.rev`.

    Uses ``sort_keys`` (which ``cauli._codec`` deliberately does not offer:
    it encodes wire payloads, where key order is not semantic). This string
    never reaches the wire -- it is hashed and thrown away.
    """
    return json.dumps(spec, sort_keys=True, separators=(",", ":"))


class Schedule:
    """Base class. Subclasses implement :meth:`next_after` and :meth:`to_spec`."""

    def next_after(self, slot_ms: int) -> int:
        """The first slot strictly after ``slot_ms`` (both epoch ms)."""
        raise NotImplementedError

    def to_spec(self) -> dict[str, Any]:
        """JSON-serializable description; the inverse of :func:`schedule_from_spec`."""
        raise NotImplementedError

    def advance_past(self, slot_ms: int, now_ms: int) -> int:
        """First slot strictly after BOTH ``slot_ms`` and ``now_ms``.

        This is the "missed ticks" primitive (PROTOCOL.md section 10.4): after
        downtime the scheduler fires the due slot **once** and then fast
        forwards past the present, rather than replaying every slot it slept
        through. Subclasses may override with closed-form arithmetic; the
        default steps and is bounded by :data:`_MAX_ADVANCE_STEPS`, after which
        it jumps directly (a jump can only ever *skip* slots, never duplicate
        one, so the bound is safe).
        """
        nxt = self.next_after(slot_ms)
        steps = 0
        while nxt <= now_ms:
            if steps >= _MAX_ADVANCE_STEPS:
                return self.next_after(now_ms)
            nxt = self.next_after(nxt)
            steps += 1
        return nxt

    def rev(self) -> str:
        """Stable 16-hex-char fingerprint of this schedule's definition.

        The beat loop reseeds an entry's next-fire slot when its rev changes,
        which is how an edited schedule (in code or via a future admin view)
        takes effect without a restart.
        """
        return hashlib.sha1(_canonical(self.to_spec()).encode("utf-8")).hexdigest()[:16]

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Schedule) and self.to_spec() == other.to_spec()

    def __hash__(self) -> int:
        return hash(_canonical(self.to_spec()))


class IntervalSchedule(Schedule):
    """Fire every ``every`` seconds.

    Slots keep the phase of the first one: slot N is ``first + N * every``, so
    restarting beat does not shift the cadence and does not re-fire.
    """

    def __init__(self, every: float) -> None:
        every_ms = int(round(float(every) * 1000))
        if every_ms <= 0:
            raise ValueError(f"interval must be > 0 seconds, got {every!r}")
        self.every_ms = every_ms

    @property
    def every(self) -> float:
        return self.every_ms / 1000.0

    def next_after(self, slot_ms: int) -> int:
        return int(slot_ms) + self.every_ms

    def advance_past(self, slot_ms: int, now_ms: int) -> int:
        # Closed form: no loop, so an interval schedule recovers from arbitrary
        # downtime in O(1) (a 1-second interval down for a day would otherwise
        # step 86_400 times).
        slot_ms = int(slot_ms)
        if now_ms < slot_ms:
            return slot_ms + self.every_ms
        steps = (now_ms - slot_ms) // self.every_ms + 1
        return slot_ms + steps * self.every_ms

    def to_spec(self) -> dict[str, Any]:
        return {"type": "interval", "every_ms": self.every_ms}

    def __repr__(self) -> str:
        return f"interval(every={self.every})"


def _num(token: str, lo: int, hi: int, names: dict[str, int] | None) -> int:
    token = token.strip().lower()
    if names is not None and token in names:
        value = names[token]
    else:
        try:
            value = int(token)
        except ValueError:
            raise ValueError(f"not a valid cron value: {token!r}") from None
    if not (lo <= value <= hi):
        raise ValueError(f"cron value {token!r} out of range [{lo}, {hi}]")
    return value


def _parse_field(
    expr: Any, lo: int, hi: int, names: dict[str, int] | None = None
) -> frozenset[int]:
    """Parse one cron field into the set of values it matches.

    Accepted: ``*``, ``*/step``, ``a``, ``a-b``, ``a-b/step``, ``a/step``
    (vixie extension: ``a`` through ``hi`` by ``step``), comma-separated lists
    of any of those, plus three-letter names for month and day-of-week. An
    ``int``, or any iterable of the above, is also accepted so schedules can be
    written as ``crontab(minute=[0, 30])``.
    """
    if expr is None:
        expr = "*"
    if isinstance(expr, bool):  # bool is an int subclass; never a cron value
        raise ValueError("cron field cannot be a bool")
    if isinstance(expr, int):
        expr = str(expr)
    elif not isinstance(expr, str):
        try:
            expr = ",".join(str(x) for x in expr)
        except TypeError:
            raise ValueError(f"unsupported cron field {expr!r}") from None
    expr = expr.strip().lower()
    if not expr:
        raise ValueError("empty cron field")

    values: set[int] = set()
    for part in expr.split(","):
        part = part.strip()
        if not part:
            raise ValueError(f"empty element in cron field {expr!r}")
        step = 1
        if "/" in part:
            part, _, raw_step = part.partition("/")
            try:
                step = int(raw_step)
            except ValueError:
                raise ValueError(f"bad cron step {raw_step!r}") from None
            if step < 1:
                raise ValueError(f"cron step must be >= 1, got {step}")
            part = part.strip()
        if part == "*":
            start, end = lo, hi
        elif "-" in part:
            a, _, b = part.partition("-")
            start, end = _num(a, lo, hi, names), _num(b, lo, hi, names)
            if start > end:
                raise ValueError(f"inverted cron range {part!r}")
        else:
            start = _num(part, lo, hi, names)
            # "5/15" means "from 5 to the top of the range, every 15".
            end = hi if step != 1 else start
        values.update(range(start, end + 1, step))
    if not values:
        raise ValueError(f"cron field {expr!r} matches nothing")
    return frozenset(values)


class CrontabSchedule(Schedule):
    """POSIX cron-style wall-clock schedule in one explicit IANA timezone.

    Semantics deliberately follow ``cron(8)``, not Celery:

    - **day_of_month / day_of_week are OR'd** when BOTH are restricted (a field
      counts as restricted unless it is exactly ``*``). ``0 3 1 * mon`` fires
      on the 1st of the month *and* on every Monday. Celery ANDs them; POSIX
      cron ORs them, and an expression written in crontab syntax should mean
      what crontab means.
    - ``day_of_week`` is ``0``-``6`` with ``0`` = Sunday; ``7`` is accepted as
      Sunday too.

    DST is handled by construction rather than by special cases, because every
    slot is stored as an absolute instant and :meth:`next_after` is required to
    return an instant strictly greater than its argument:

    - **Fall back** (a wall time happens twice): the first occurrence
      (``fold=0``) is the slot. The second cannot fire, because it is not
      strictly after the first.
    - **Spring forward** (a wall time never happens): ``zoneinfo`` resolves the
      nonexistent time using the pre-transition offset, so e.g. a 02:30 job on
      the skipped day fires at the same instant that 02:30 standard time would
      have been -- 03:30 by the new wall clock. It fires once; it is not
      silently dropped.
    """

    def __init__(
        self,
        minute: Any = "*",
        hour: Any = "*",
        day_of_month: Any = "*",
        month: Any = "*",
        day_of_week: Any = "*",
        timezone: str = "UTC",
    ) -> None:
        self.minute_expr = _expr_text(minute)
        self.hour_expr = _expr_text(hour)
        self.dom_expr = _expr_text(day_of_month)
        self.month_expr = _expr_text(month)
        self.dow_expr = _expr_text(day_of_week)
        self.timezone = str(timezone)
        try:
            self.tz = ZoneInfo(self.timezone)
        except Exception as exc:
            raise ValueError(
                f"unknown timezone {self.timezone!r} "
                "(needs an IANA name such as 'UTC' or 'Europe/Berlin'; on "
                "Windows this also needs the 'tzdata' package)"
            ) from exc

        self.minutes = _parse_field(minute, 0, 59)
        self.hours = _parse_field(hour, 0, 23)
        self.doms = _parse_field(day_of_month, 1, 31)
        self.months = _parse_field(month, 1, 12, _MONTH_NAMES)
        dows = _parse_field(day_of_week, 0, 7, _DOW_NAMES)
        self.dows = frozenset(0 if d == 7 else d for d in dows)

        # POSIX restriction flags: a field is unrestricted only when it is
        # literally "*". "*/2" is a restriction.
        self._dom_restricted = self.dom_expr != "*"
        self._dow_restricted = self.dow_expr != "*"

        # Sorted (hour, minute) slots for one day; the scan below bisects it.
        self._hm: list[tuple[int, int]] = sorted(
            (h, m) for h in self.hours for m in self.minutes
        )

    def _date_matches(self, d: _date) -> bool:
        if d.month not in self.months:
            return False
        dom_ok = d.day in self.doms
        # date.weekday(): Monday == 0 .. Sunday == 6. cron: Sunday == 0.
        dow_ok = ((d.weekday() + 1) % 7) in self.dows
        if self._dom_restricted and self._dow_restricted:
            return dom_ok or dow_ok
        if self._dom_restricted:
            return dom_ok
        if self._dow_restricted:
            return dow_ok
        return True

    def _instant_ms(self, d: _date, hour: int, minute: int) -> int:
        # fold=0 resolves an ambiguous (repeated) wall time to its FIRST
        # occurrence and a nonexistent one via the pre-transition offset.
        naive = datetime(d.year, d.month, d.day, hour, minute, tzinfo=self.tz)
        return int(naive.timestamp() * 1000)

    def next_after(self, slot_ms: int) -> int:
        slot_ms = int(slot_ms)
        local = datetime.fromtimestamp(slot_ms / 1000.0, tz=timezone.utc).astimezone(
            self.tz
        )
        day = local.date()
        # Start scanning from the minute after the argument's wall-clock minute;
        # the strict `> slot_ms` test below is what actually enforces progress,
        # so an over-inclusive starting point is harmless.
        from_hm = (local.hour, local.minute)
        for offset in range(_MAX_SCAN_DAYS):
            if self._date_matches(day):
                for hm in self._hm:
                    if offset == 0 and hm < from_hm:
                        continue
                    ms = self._instant_ms(day, hm[0], hm[1])
                    if ms > slot_ms:
                        return ms
            day = day + timedelta(days=1)
        raise ValueError(
            f"crontab {self!r} has no occurrence within {_MAX_SCAN_DAYS} days "
            "of the given instant (an impossible expression, e.g. February 30th)"
        )

    def to_spec(self) -> dict[str, Any]:
        return {
            "type": "crontab",
            "minute": self.minute_expr,
            "hour": self.hour_expr,
            "day_of_month": self.dom_expr,
            "month": self.month_expr,
            "day_of_week": self.dow_expr,
            "timezone": self.timezone,
        }

    def __repr__(self) -> str:
        return (
            f"crontab({self.minute_expr!r} {self.hour_expr!r} {self.dom_expr!r} "
            f"{self.month_expr!r} {self.dow_expr!r} tz={self.timezone!r})"
        )


def _expr_text(expr: Any) -> str:
    """Normalize a field to its canonical text form (what gets stored/hashed)."""
    if expr is None:
        return "*"
    if isinstance(expr, bool):
        raise ValueError("cron field cannot be a bool")
    if isinstance(expr, int):
        return str(expr)
    if isinstance(expr, str):
        return expr.strip().lower() or "*"
    return ",".join(str(x).strip().lower() for x in expr)


def interval(every: float) -> IntervalSchedule:
    """Fire every ``every`` seconds. See :class:`IntervalSchedule`."""
    return IntervalSchedule(every)


def crontab(
    minute: Any = "*",
    hour: Any = "*",
    day_of_month: Any = "*",
    month: Any = "*",
    day_of_week: Any = "*",
    timezone: str = "UTC",
) -> CrontabSchedule:
    """POSIX cron-style schedule. See :class:`CrontabSchedule`."""
    return CrontabSchedule(
        minute=minute,
        hour=hour,
        day_of_month=day_of_month,
        month=month,
        day_of_week=day_of_week,
        timezone=timezone,
    )


class ScheduleEntry:
    """One periodic schedule entry (PROTOCOL.md section 10.2).

    This is the *definition* only -- what to run and when. Runtime state (the
    next slot, the last firing, the run count) lives in Redis, owned by the
    beat leader, and never in this object. That split is what lets a future
    Django-admin view be an addition rather than a rewrite: the admin edits
    definitions, beat owns state.

    ``on_missed`` / ``max_lateness`` are the documented answer to "beat was
    down, what happens on recovery":

    - ``max_lateness=None`` (default): a due slot always fires, however late.
    - ``max_lateness=S`` with ``on_missed="skip"``: a slot more than ``S``
      seconds late does not fire at all.
    - ``on_missed="fire_once"`` (default): it fires.

    Either way beat fast forwards past the present afterwards -- a slot backlog
    is coalesced into a single firing, never replayed one by one.
    """

    __slots__ = (
        "name",
        "task",
        "args",
        "kwargs",
        "schedule",
        "queue",
        "expires",
        "idempotent",
        "enabled",
        "on_missed",
        "max_lateness",
        "source",
    )

    def __init__(
        self,
        name: str,
        task: str,
        schedule: Any,
        args: Any = (),
        kwargs: dict[str, Any] | None = None,
        queue: str | None = None,
        expires: float | None = None,
        idempotent: bool = False,
        enabled: bool = True,
        on_missed: str = "fire_once",
        max_lateness: float | None = None,
        source: str = "code",
    ) -> None:
        if not name or not isinstance(name, str):
            raise ValueError(
                f"schedule entry name must be a non-empty str, got {name!r}"
            )
        if not task or not isinstance(task, str):
            raise ValueError(f"schedule entry {name!r}: task must be a non-empty str")
        if on_missed not in ON_MISSED:
            raise ValueError(
                f"schedule entry {name!r}: on_missed must be one of {ON_MISSED}"
            )
        self.name = name
        self.task = task
        self.schedule: Schedule = schedule_from_spec(schedule)
        self.args = list(args or ())
        self.kwargs = dict(kwargs or {})
        self.queue = queue
        self.expires = None if expires is None else float(expires)
        self.idempotent = bool(idempotent)
        self.enabled = bool(enabled)
        self.on_missed = on_missed
        self.max_lateness = None if max_lateness is None else float(max_lateness)
        self.source = source

    @property
    def max_lateness_ms(self) -> int | None:
        return None if self.max_lateness is None else int(self.max_lateness * 1000)

    def rev(self) -> str:
        """Fingerprint of everything that affects WHEN this entry fires.

        Only the schedule and enabled-ness are in scope: changing the task's
        kwargs should not reset its next slot, but changing ``crontab(hour=3)``
        to ``crontab(hour=4)`` must.
        """
        return hashlib.sha1(
            _canonical(
                {"schedule": self.schedule.to_spec(), "enabled": self.enabled}
            ).encode("utf-8")
        ).hexdigest()[:16]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "task": self.task,
            "args": list(self.args),
            "kwargs": dict(self.kwargs),
            "schedule": self.schedule.to_spec(),
            "queue": self.queue,
            "expires": self.expires,
            "idempotent": self.idempotent,
            "enabled": self.enabled,
            "on_missed": self.on_missed,
            "max_lateness": self.max_lateness,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, doc: dict[str, Any]) -> "ScheduleEntry":
        return cls(
            name=doc["name"],
            task=doc["task"],
            schedule=doc["schedule"],
            args=doc.get("args") or (),
            kwargs=doc.get("kwargs") or {},
            queue=doc.get("queue"),
            expires=doc.get("expires"),
            idempotent=bool(doc.get("idempotent", False)),
            enabled=bool(doc.get("enabled", True)),
            on_missed=doc.get("on_missed", "fire_once"),
            max_lateness=doc.get("max_lateness"),
            source=doc.get("source", "redis"),
        )

    def __repr__(self) -> str:
        state = "" if self.enabled else " disabled"
        return f"<ScheduleEntry {self.name!r} {self.task} {self.schedule!r}{state}>"


def schedule_from_spec(spec: Any) -> Schedule:
    """Rebuild a :class:`Schedule` from its :meth:`Schedule.to_spec` form.

    Also accepts a bare number (seconds -> interval) and a
    :class:`datetime.timedelta`, so a schedule entry stored in Redis by hand
    can say ``"schedule": 300`` instead of the full object.
    """
    if isinstance(spec, Schedule):
        return spec
    if isinstance(spec, timedelta):
        return IntervalSchedule(spec.total_seconds())
    if isinstance(spec, bool):
        raise ValueError("schedule cannot be a bool")
    if isinstance(spec, (int, float)):
        return IntervalSchedule(float(spec))
    if not isinstance(spec, dict):
        raise ValueError(f"unsupported schedule spec {spec!r}")
    kind = spec.get("type")
    if kind == "interval":
        if "every_ms" in spec:
            return IntervalSchedule(float(spec["every_ms"]) / 1000.0)
        return IntervalSchedule(float(spec["every"]))
    if kind == "crontab":
        return CrontabSchedule(
            minute=spec.get("minute", "*"),
            hour=spec.get("hour", "*"),
            day_of_month=spec.get("day_of_month", "*"),
            month=spec.get("month", "*"),
            day_of_week=spec.get("day_of_week", "*"),
            timezone=spec.get("timezone", "UTC"),
        )
    raise ValueError(f"unknown schedule type {kind!r}")
