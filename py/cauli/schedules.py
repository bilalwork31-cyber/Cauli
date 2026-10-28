"""Schedule types for cauli's periodic scheduler (PROTOCOL.md section 10).

:func:`interval` fires every N seconds; :func:`crontab` is a POSIX cron
schedule in an explicit IANA timezone (default ``"UTC"``).

Every schedule is a pure function ``next_after(slot_ms) -> slot_ms'`` over
epoch milliseconds and never reads the clock. Beat replicas whose clocks
disagree must compute the same next slot, or the compare and set in
:mod:`cauli.beat` stops being a mutual exclusion and a slot fires twice.
"""

from __future__ import annotations

import bisect
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

# Feb 29 can be 8 years out (2096 -> 2104); past that the expression can
# never match, such as "0 0 30 2 *".
_MAX_SCAN_DAYS = 366 * 8

# About 7 days of one minute slots; past it advance_past jumps to next_after(now).
_MAX_ADVANCE_STEPS = 10_000


def _canonical(spec: dict[str, Any]) -> str:
    """Deterministic text form of a spec, hashed by :meth:`Schedule.rev`."""
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

        After downtime beat fires the due slot once and then fast forwards
        past the present (PROTOCOL.md section 10.4). Past the step bound it
        jumps, which can only skip slots, never duplicate one.
        """
        return self.advance_past_with_missed(slot_ms, now_ms)[0]

    def advance_past_with_missed(
        self, slot_ms: int, now_ms: int
    ) -> tuple[int, int | None]:
        """:meth:`advance_past`, plus how many slots the fast forward swallowed.

        The count covers ``(slot_ms, now_ms]``, so beat can report work that
        will never run. ``None`` when the step bound was hit.
        """
        nxt = self.next_after(slot_ms)
        missed = 0
        while nxt <= now_ms:
            if missed >= _MAX_ADVANCE_STEPS:
                return self.next_after(now_ms), None
            nxt = self.next_after(nxt)
            missed += 1
        return nxt, missed

    def rev(self) -> str:
        """Stable 16 hex char fingerprint; beat reseeds the next slot when it changes."""
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

    def advance_past_with_missed(
        self, slot_ms: int, now_ms: int
    ) -> tuple[int, int | None]:
        # Closed form: a 1 second interval down for a day would step 86_400 times.
        slot_ms = int(slot_ms)
        if now_ms < slot_ms:
            return slot_ms + self.every_ms, 0
        steps = (now_ms - slot_ms) // self.every_ms + 1
        return slot_ms + steps * self.every_ms, steps - 1

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

    Accepts ``*``, ``*/step``, ``a``, ``a-b``, ``a-b/step``, ``a/step`` (``a``
    to ``hi``), comma lists, month and weekday names, an ``int``, or an
    iterable such as ``[0, 30]``.
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

    Follows ``cron(8)``, not Celery:

    - ``day_of_month`` and ``day_of_week`` are ORed when both are restricted
      (not exactly ``*``): ``0 3 1 * mon`` fires on the 1st and every Monday.
    - ``day_of_week`` is ``0`` to ``6`` with ``0`` Sunday; ``7`` is Sunday too.
    - Fall back: a fixed wall time fires once, at its first occurrence. A job
      whose minute or hour field starts with ``*`` also fires in the repeated hour.
    - Spring forward: a skipped wall time resolves through the old offset, so
      a 02:30 job fires once, at 03:30 new wall time.
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

        # "*/2" counts as restricted; only a literal "*" does not.
        self._dom_restricted = self.dom_expr != "*"
        self._dow_restricted = self.dow_expr != "*"
        self._repeats_in_fold = self.minute_expr.startswith("*") or (
            self.hour_expr.startswith("*")
        )

        self._shifting_days: dict[_date, list[int]] = {}
        self._hm: list[tuple[int, int]] = sorted(
            (h, m) for h in self.hours for m in self.minutes
        )

    def _date_matches(self, d: _date) -> bool:
        if d.month not in self.months:
            return False
        dom_ok = d.day in self.doms
        # weekday() has Monday == 0; cron has Sunday == 0.
        dow_ok = ((d.weekday() + 1) % 7) in self.dows
        if self._dom_restricted and self._dow_restricted:
            return dom_ok or dow_ok
        if self._dom_restricted:
            return dom_ok
        if self._dow_restricted:
            return dow_ok
        return True

    def _instant_ms(self, d: _date, hour: int, minute: int, fold: int = 0) -> int:
        # fold=0 resolves an ambiguous (repeated) wall time to its FIRST
        # occurrence and a nonexistent one via the pre-transition offset.
        naive = datetime(
            d.year, d.month, d.day, hour, minute, tzinfo=self.tz, fold=fold
        )
        return int(naive.timestamp() * 1000)

    def _offset_changes_during(self, d: _date) -> bool:
        """True when this zone changes UTC offset somewhere inside day d."""
        after = d + timedelta(days=1)
        start = datetime(d.year, d.month, d.day, tzinfo=self.tz)
        end = datetime(after.year, after.month, after.day, tzinfo=self.tz)
        return start.utcoffset() != end.utcoffset()

    def _shifting_day_instants(self, d: _date) -> list[int]:
        """Every instant this schedule fires on a day whose offset changes.

        Wall order is not instant order on such a day: with a two hour spring
        forward (Antarctica/Troll) wall 02:30 lands at 02:30Z, after wall 03:30
        at 01:30Z. So the day is resolved, deduplicated and sorted. Memoized
        because a recovery fast forward asks for the same day once per slot.
        """
        cached = self._shifting_days.get(d)
        if cached is not None:
            return cached
        found: set[int] = set()
        for hour, minute in self._hm:
            first = self._instant_ms(d, hour, minute)
            found.add(first)
            if self._repeats_in_fold:
                # fold=1 is LATER only for a repeated wall time; for a
                # nonexistent one it is earlier and must not fire.
                again = self._instant_ms(d, hour, minute, fold=1)
                if again > first:
                    found.add(again)
        cached = self._shifting_days[d] = sorted(found)
        return cached

    def next_after(self, slot_ms: int) -> int:
        slot_ms = int(slot_ms)
        local = datetime.fromtimestamp(slot_ms / 1000.0, tz=timezone.utc).astimezone(
            self.tz
        )
        day = local.date()
        # The strict `> slot_ms` test enforces progress; from_hm only prunes.
        from_hm = (local.hour, local.minute)
        for offset in range(_MAX_SCAN_DAYS):
            if self._date_matches(day) and self._offset_changes_during(day):
                instants = self._shifting_day_instants(day)
                i = bisect.bisect_right(instants, slot_ms)
                if i < len(instants):
                    return instants[i]
            elif self._date_matches(day):
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
    """Canonical text of a field, as stored and hashed."""
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
    *,
    day_of_month: Any = "*",
    month: Any = "*",
    day_of_week: Any = "*",
    timezone: str = "UTC",
    month_of_year: Any = None,
) -> CrontabSchedule:
    """POSIX cron-style schedule. See :class:`CrontabSchedule`.

    Only ``minute`` and ``hour`` are positional: Celery orders the fields
    ``(minute, hour, day_of_week, day_of_month, month_of_year)``, cauli uses
    ``cron(8)`` order, so a copied ``crontab(0, 4, 1)`` would silently change
    meaning. ``month_of_year=`` exists only to be rejected by name.
    """
    if month_of_year is not None:
        raise TypeError(
            "crontab() has no 'month_of_year' argument; cauli calls that field "
            "'month'. Note also that cauli's field order is cron(8)'s "
            "(minute, hour, day_of_month, month, day_of_week), while Celery's "
            "is (minute, hour, day_of_week, day_of_month, month_of_year): the "
            "third and fourth are swapped, so pass them by name."
        )
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

    The definition only; runtime state (next slot, last firing, run count)
    lives in Redis, owned by the beat leader.

    After beat downtime a due slot fires once, however late, unless
    ``max_lateness=S`` and ``on_missed="skip"`` and it is more than ``S``
    seconds late. A backlog of slots is coalesced, never replayed.
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
        if max_lateness is not None and max_lateness < 0:
            # A negative bound would silently stop the entry from ever firing.
            raise ValueError(
                f"schedule entry {name!r}: max_lateness must be >= 0 seconds, "
                f"got {max_lateness!r}"
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

        Schedule and enabled only: editing kwargs must not reset the next slot.
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

    Also accepts seconds as a number or a :class:`datetime.timedelta`, so a
    hand written Redis entry can say ``"schedule": 300``.
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
