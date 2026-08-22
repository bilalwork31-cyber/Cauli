"""Schedule math: interval, crontab parsing, POSIX dom/dow OR, DST, purity.

Every assertion here is on `next_after` / `advance_past`, which are pure
functions of an instant. That purity is what the beat compare-and-set relies
on (see cauli/schedules.py and cauli/beat.py docstrings), so it gets tested
directly rather than only through the scheduler.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from cauli.schedules import (
    CrontabSchedule,
    ScheduleEntry,
    crontab,
    interval,
    schedule_from_spec,
)

UTC = timezone.utc


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def at(*args, tz=UTC) -> int:
    return ms(datetime(*args, tzinfo=tz))


def show(epoch_ms: int, tz=UTC) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=tz).isoformat()


# ---------------------------------------------------------------- interval


def test_interval_next_after_is_exact_and_phase_stable():
    s = interval(2.5)
    assert s.every_ms == 2500
    base = at(2024, 1, 1, 0, 0, 0)
    assert s.next_after(base) == base + 2500
    # Slot N keeps the phase of slot 0: no drift, no accumulation of error.
    slot = base
    for n in range(1, 11):
        slot = s.next_after(slot)
        assert slot == base + n * 2500


def test_interval_advance_past_coalesces_missed_slots_in_one_step():
    s = interval(1.0)
    base = at(2024, 1, 1, 0, 0, 0)
    # Beat was down for an hour. The next slot must be the first one strictly
    # in the future -- NOT 3600 replayed firings, and not a slot in the past.
    now = base + 3_600_000
    nxt = s.advance_past(base, now)
    assert nxt == now + 1000
    assert nxt > now
    # Phase preserved across the jump.
    assert (nxt - base) % 1000 == 0


def test_interval_advance_past_when_not_actually_late():
    s = interval(60.0)
    base = at(2024, 1, 1, 0, 0, 0)
    assert s.advance_past(base, base + 1) == base + 60_000


def test_interval_rejects_non_positive():
    for bad in (0, -1, 0.0):
        with pytest.raises(ValueError):
            interval(bad)


# ---------------------------------------------------------------- parsing


@pytest.mark.parametrize(
    "expr,expected",
    [
        ("*", set(range(60))),
        ("0", {0}),
        ("0,30", {0, 30}),
        ("*/15", {0, 15, 30, 45}),
        ("10-13", {10, 11, 12, 13}),
        ("0-20/5", {0, 5, 10, 15, 20}),
        ("45/10", {45, 55}),  # vixie "from 45 to the top, every 10"
        ("5,10-12,*/30", {0, 5, 10, 11, 12, 30}),
    ],
)
def test_minute_field_parsing(expr, expected):
    assert set(CrontabSchedule(minute=expr).minutes) == expected


def test_named_months_and_days():
    s = crontab(minute=0, hour=0, month="jan,mar", day_of_week="mon-fri")
    assert set(s.months) == {1, 3}
    assert set(s.dows) == {1, 2, 3, 4, 5}


def test_day_of_week_7_is_sunday():
    assert set(crontab(day_of_week="7").dows) == {0}
    assert set(crontab(day_of_week="5-7").dows) == {0, 5, 6}


def test_int_and_iterable_fields():
    assert set(crontab(minute=0).minutes) == {0}
    assert set(crontab(minute=[0, 30]).minutes) == {0, 30}


@pytest.mark.parametrize(
    "kwargs",
    [
        {"minute": "60"},
        {"hour": "24"},
        {"day_of_month": "0"},
        {"month": "13"},
        {"minute": "10-5"},
        {"minute": "*/0"},
        {"minute": ""},
        {"minute": "nope"},
        {"minute": True},
    ],
)
def test_invalid_fields_rejected(kwargs):
    with pytest.raises(ValueError):
        crontab(**kwargs)


def test_unknown_timezone_rejected_with_a_useful_message():
    with pytest.raises(ValueError, match="unknown timezone"):
        crontab(timezone="Mars/Olympus_Mons")


# --------------------------------------------------- Celery signature guards


def test_crontab_is_keyword_only_past_hour():
    # Celery's third positional is day_of_week, cauli's is day_of_month.
    # A copied `crontab(0, 4, 1)` must fail loudly instead of meaning
    # something different from what it meant under Celery.
    with pytest.raises(TypeError):
        crontab(0, 4, 1)
    # The two shared positions stay positional and keep working.
    assert crontab(0, 4).to_spec() == crontab(minute=0, hour=4).to_spec()


def test_crontab_rejects_celery_month_of_year_by_name():
    with pytest.raises(TypeError, match="month_of_year"):
        crontab(minute=0, hour=4, month_of_year=3)


# ---------------------------------------------------------------- crontab


def test_daily_at_three_am_in_a_named_zone():
    berlin = ZoneInfo("Europe/Berlin")
    s = crontab(minute=0, hour=3, timezone="Europe/Berlin")
    # 2024-06-10 01:00 UTC == 03:00 Berlin (CEST). Asking from just before it.
    start = at(2024, 6, 10, 0, 59)
    nxt = s.next_after(start)
    local = datetime.fromtimestamp(nxt / 1000, tz=berlin)
    assert (local.hour, local.minute) == (3, 0)
    assert local.date() == datetime(2024, 6, 10).date()
    # And the following one is exactly one (wall-clock) day later.
    local2 = datetime.fromtimestamp(s.next_after(nxt) / 1000, tz=berlin)
    assert (local2.hour, local2.minute) == (3, 0)
    assert (local2.date() - local.date()).days == 1


def test_next_after_is_always_strictly_greater():
    s = crontab(minute="*")
    exact = at(2024, 1, 1, 12, 0, 0)
    # Called with an instant that IS a slot, the answer must be the next one.
    assert s.next_after(exact) == exact + 60_000


def test_next_after_is_pure_and_deterministic():
    # The property the beat CAS depends on: two callers (two replicas, two
    # clocks) computing from the same previous slot must get the same answer.
    s = crontab(minute="*/7", hour="1,13", day_of_week="mon")
    slot = at(2024, 3, 1, 0, 0)
    first = [slot := s.next_after(slot) for _ in range(20)]
    slot = at(2024, 3, 1, 0, 0)
    second = [slot := s.next_after(slot) for _ in range(20)]
    assert first == second


def test_dom_and_dow_are_ORed_like_posix_cron():
    # "0 0 1 * mon": the 1st of the month OR any Monday. POSIX cron ORs the two
    # day fields when both are restricted; Celery ANDs them. cauli follows
    # POSIX, because the expression is written in crontab syntax.
    s = crontab(minute=0, hour=0, day_of_month=1, day_of_week="mon")
    fired = []
    slot = at(2024, 4, 1, 0, 0) - 1
    for _ in range(6):
        slot = s.next_after(slot)
        fired.append(datetime.fromtimestamp(slot / 1000, tz=UTC).date())
    # April 2024: 1st is a Monday; then 8th, 15th, 22nd, 29th are Mondays;
    # then May 1st (a Wednesday) because day_of_month matches.
    assert [d.isoformat() for d in fired] == [
        "2024-04-01",
        "2024-04-08",
        "2024-04-15",
        "2024-04-22",
        "2024-04-29",
        "2024-05-01",
    ]


def test_dow_only_is_not_ORed_with_an_unrestricted_dom():
    # With day_of_month="*" only the dow restricts; every Monday, no more.
    s = crontab(minute=0, hour=0, day_of_week="mon")
    slot = at(2024, 4, 2, 0, 0)
    days = []
    for _ in range(3):
        slot = s.next_after(slot)
        days.append(datetime.fromtimestamp(slot / 1000, tz=UTC).date().isoformat())
    assert days == ["2024-04-08", "2024-04-15", "2024-04-22"]


def test_impossible_expression_raises_rather_than_hanging():
    s = crontab(minute=0, hour=0, day_of_month=30, month=2)  # February 30th
    with pytest.raises(ValueError, match="no occurrence"):
        s.next_after(at(2024, 1, 1))


def test_leap_day_is_reachable():
    s = crontab(minute=0, hour=0, day_of_month=29, month=2)
    nxt = s.next_after(at(2024, 3, 1))
    assert datetime.fromtimestamp(nxt / 1000, tz=UTC).date().isoformat() == "2028-02-29"


# ---------------------------------------------------------------- DST


def test_dst_fall_back_fires_a_repeated_wall_time_exactly_once():
    """US fall-back 2024-11-03: 01:00-01:59 EDT happens, then again as EST.

    A 01:30 daily job must fire ONCE that day. It does, structurally rather
    than by special case: next_after must return an instant strictly greater
    than the previous slot, and the second 01:30 is not greater than the first
    once the schedule has moved on to the next day.
    """
    ny = ZoneInfo("America/New_York")
    s = crontab(minute=30, hour=1, timezone="America/New_York")
    slot = at(2024, 11, 1, 0, 0)  # before the transition
    fired = []
    for _ in range(4):
        slot = s.next_after(slot)
        fired.append(slot)

    locals_ = [datetime.fromtimestamp(x / 1000, tz=ny) for x in fired]
    assert [f"{d.date()} {d.hour:02d}:{d.minute:02d}" for d in locals_] == [
        "2024-11-01 01:30",
        "2024-11-02 01:30",
        "2024-11-03 01:30",
        "2024-11-04 01:30",
    ]
    # Exactly one firing on the ambiguous day, and it is the FIRST (EDT) one.
    assert locals_[2].utcoffset() == timedelta(hours=-4)
    assert len({x for x in fired}) == 4
    assert fired == sorted(fired), "slots must be strictly increasing instants"


def test_dst_fall_back_hourly_job_does_not_double_fire():
    """An hourly job across the fall-back fires once per WALL-CLOCK hour.

    The hour 01:00-01:59 local happens twice. The 01:30 slot fires on the
    first (EDT) pass; the second (EST) 01:30 is not a later instant than a slot
    already consumed, so it is skipped rather than duplicated. Net effect: the
    repeated hour contributes one firing, not two, and the run is strictly
    increasing throughout.
    """
    ny = ZoneInfo("America/New_York")
    s = crontab(minute=30, timezone="America/New_York")
    slot = at(2024, 11, 3, 4, 0)  # 2024-11-03 00:00 EDT
    fired = []
    for _ in range(4):
        slot = s.next_after(slot)
        fired.append(slot)

    assert fired == sorted(set(fired)), "no repeated or out-of-order instants"
    stamps = [datetime.fromtimestamp(x / 1000, tz=ny) for x in fired]
    assert [f"{d.hour:02d}:{d.minute:02d}{d.tzname()}" for d in stamps] == [
        "00:30EDT",
        "01:30EDT",
        # 01:30 EST (the repeat) is skipped; the run continues at 02:30 EST.
        "02:30EST",
        "03:30EST",
    ]
    gaps = [b - a for a, b in zip(fired, fired[1:])]
    assert gaps == [3_600_000, 7_200_000, 3_600_000]


def test_dst_spring_forward_nonexistent_wall_time_still_fires_once():
    """US spring-forward 2024-03-10: 02:00-02:59 local never happens.

    A 02:30 daily job must not be silently dropped for that day, and must not
    fire twice. zoneinfo resolves the nonexistent time via the pre-transition
    offset, so it lands at the same instant 02:30 EST would have been -- 03:30
    by the new wall clock.
    """
    ny = ZoneInfo("America/New_York")
    s = crontab(minute=30, hour=2, timezone="America/New_York")
    slot = at(2024, 3, 8, 0, 0)
    fired = []
    for _ in range(4):
        slot = s.next_after(slot)
        fired.append(slot)
    dates = [datetime.fromtimestamp(x / 1000, tz=ny).date().isoformat() for x in fired]
    assert dates == ["2024-03-08", "2024-03-09", "2024-03-10", "2024-03-11"]
    assert fired == sorted(set(fired))
    skipped_day = datetime.fromtimestamp(fired[2] / 1000, tz=ny)
    assert (skipped_day.hour, skipped_day.minute) == (3, 30)


def test_utc_schedule_is_unaffected_by_dst_anywhere():
    s = crontab(minute=30, hour=2)  # default UTC
    slot = at(2024, 3, 8, 0, 0)
    gaps = []
    for _ in range(5):
        nxt = s.next_after(slot)
        gaps.append(nxt - slot if slot != at(2024, 3, 8, 0, 0) else None)
        slot = nxt
    assert all(g == 86_400_000 for g in gaps if g is not None)


# ---------------------------------------------------------------- specs


def test_spec_roundtrip_for_both_kinds():
    for original in [
        interval(90),
        crontab(
            minute="*/5", hour="9-17", day_of_week="mon-fri", timezone="Asia/Tokyo"
        ),
    ]:
        rebuilt = schedule_from_spec(original.to_spec())
        assert rebuilt.to_spec() == original.to_spec()
        assert rebuilt.rev() == original.rev()
        assert rebuilt == original


def test_shorthand_specs():
    assert schedule_from_spec(30).to_spec() == {"type": "interval", "every_ms": 30000}
    assert schedule_from_spec(timedelta(minutes=2)).to_spec() == {
        "type": "interval",
        "every_ms": 120000,
    }
    with pytest.raises(ValueError):
        schedule_from_spec({"type": "sundial"})


def test_rev_changes_only_when_the_schedule_changes():
    a = crontab(minute=0, hour=3)
    b = crontab(minute=0, hour=3)
    c = crontab(minute=0, hour=4)
    assert a.rev() == b.rev()
    assert a.rev() != c.rev()
    assert len(a.rev()) == 16


def test_entry_rev_ignores_payload_but_not_schedule_or_enabled():
    base = dict(name="e", task="t", schedule=interval(10))
    assert (
        ScheduleEntry(**base, args=[1]).rev() == ScheduleEntry(**base, args=[2]).rev()
    )
    assert (
        ScheduleEntry(**base, queue="a").rev() == ScheduleEntry(**base, queue="b").rev()
    )
    assert (
        ScheduleEntry(**base).rev()
        != ScheduleEntry(name="e", task="t", schedule=interval(11)).rev()
    )
    assert ScheduleEntry(**base).rev() != ScheduleEntry(**base, enabled=False).rev()


def test_entry_dict_roundtrip():
    entry = ScheduleEntry(
        name="nightly",
        task="app.report",
        schedule=crontab(hour=3, minute=0, timezone="Europe/Berlin"),
        args=[1, "x"],
        kwargs={"deep": True},
        queue="reports",
        expires=300.0,
        idempotent=True,
        on_missed="skip",
        max_lateness=600.0,
        source="code",
    )
    rebuilt = ScheduleEntry.from_dict(entry.to_dict())
    assert rebuilt.to_dict() == entry.to_dict()
    assert rebuilt.max_lateness_ms == 600_000


def test_entry_validates_on_missed():
    with pytest.raises(ValueError, match="on_missed"):
        ScheduleEntry(name="e", task="t", schedule=interval(1), on_missed="explode")


def test_entry_validates_max_lateness():
    # A negative max_lateness makes lateness > max_lateness_ms always true
    # (lateness is never negative for a due slot), so the entry would never
    # fire again, silently.
    with pytest.raises(ValueError, match="max_lateness"):
        ScheduleEntry(name="e", task="t", schedule=interval(1), max_lateness=-1.0)


# ------------------------------------------------ DST: instant order vs wall order


def fires_during_local_day(sch, tz_name, y, m, d):
    """Every instant ``sch`` fires inside one local calendar day."""
    tz = ZoneInfo(tz_name)
    lo = ms(datetime(y, m, d, 0, 0, tzinfo=tz)) - 1
    after = datetime(y, m, d, tzinfo=tz) + timedelta(days=1)
    hi = ms(datetime(after.year, after.month, after.day, 0, 0, tzinfo=tz))
    out, cur = [], lo
    while True:
        cur = sch.next_after(cur)
        if cur >= hi:
            return out
        out.append(cur)


def real_matching_instants(sch, tz_name, y, m, d):
    """Ground truth: real minutes of that local day whose wall time matches.

    Walks absolute time, so a nonexistent local time simply never appears and a
    repeated one appears twice. This is what a wall clock cron would fire.
    """
    tz = ZoneInfo(tz_name)
    lo = int(datetime(y, m, d, 0, 0, tzinfo=tz).timestamp())
    after = datetime(y, m, d, tzinfo=tz) + timedelta(days=1)
    hi = int(datetime(after.year, after.month, after.day, 0, 0, tzinfo=tz).timestamp())
    out = []
    for t in range(lo, hi, 60):
        local = datetime.fromtimestamp(t, tz=tz)
        if (
            local.hour in sch.hours
            and local.minute in sch.minutes
            and sch._date_matches(local.date())
        ):
            out.append(t * 1000)
    return out


def test_spring_forward_over_two_hours_does_not_skip_a_real_slot():
    """Antarctica/Troll jumps +00 to +02 on 2025-03-30, a TWO hour gap.

    Wall 02:30 does not exist and resolves to 02:30Z, while the real wall 03:30
    is 01:30Z. Wall order and instant order therefore disagree, and a scan that
    returns the first wall match silently loses the real 03:30 slot forever.
    """
    tz = "Antarctica/Troll"
    s = crontab(minute=30, hour="2,3", timezone=tz)
    fired = fires_during_local_day(s, tz, 2025, 3, 30)
    assert fired == [at(2025, 3, 30, 1, 30), at(2025, 3, 30, 2, 30)]
    assert set(real_matching_instants(s, tz, 2025, 3, 30)) <= set(fired)


def test_spring_forward_emits_instants_in_increasing_order():
    tz = "Antarctica/Troll"
    for expr in [("30", "2,3"), ("0,30", "2,3"), ("0", "1,2,3,4"), ("*/20", "*")]:
        s = crontab(minute=expr[0], hour=expr[1], timezone=tz)
        fired = fires_during_local_day(s, tz, 2025, 3, 30)
        assert fired == sorted(fired), expr
        assert len(fired) == len(set(fired)), expr
        missed = set(real_matching_instants(s, tz, 2025, 3, 30)) - set(fired)
        assert not missed, f"{expr} lost real slots {sorted(missed)}"


@pytest.mark.parametrize(
    "tz_name, day, minute, hour, expected",
    [
        # A 23 hour day: the wall times inside the gap do not exist, so the
        # firing count is the number of REAL matching instants, not the number
        # of wall slots written in the expression.
        ("America/New_York", (2025, 3, 9), "30", "2,3", 1),
        ("America/New_York", (2025, 3, 9), "0,30", "2,3", 2),
        ("America/New_York", (2025, 3, 9), "15", "1,2,3", 2),
        ("America/New_York", (2025, 3, 9), "0", "3", 1),
        ("Europe/Berlin", (2025, 3, 30), "30", "2,3", 1),
        ("Europe/Berlin", (2025, 3, 30), "0,30", "2,3", 2),
        ("Europe/Berlin", (2025, 3, 30), "0", "0-5", 5),
        ("Europe/Berlin", (2025, 3, 30), "0", "3", 1),
    ],
)
def test_one_hour_spring_forward_fires_every_instant_that_exists(
    tz_name, day, minute, hour, expected
):
    s = crontab(minute=minute, hour=hour, timezone=tz_name)
    fired = fires_during_local_day(s, tz_name, *day)
    assert len(fired) == expected
    assert set(real_matching_instants(s, tz_name, *day)) <= set(fired)


DST_DAYS = [
    ("America/New_York", (2025, 3, 9)),
    ("America/New_York", (2025, 11, 2)),
    ("Europe/Berlin", (2025, 3, 30)),
    ("Europe/Berlin", (2025, 10, 26)),
    ("Europe/London", (2025, 3, 30)),
    ("Europe/Dublin", (2025, 10, 26)),
    ("Australia/Sydney", (2025, 10, 5)),
    ("Australia/Lord_Howe", (2025, 10, 5)),
    ("Australia/Lord_Howe", (2025, 4, 6)),
    ("Pacific/Chatham", (2025, 9, 28)),
    ("Pacific/Auckland", (2025, 4, 6)),
    ("America/Santiago", (2025, 9, 7)),
    ("America/Havana", (2025, 3, 9)),
    ("Asia/Beirut", (2025, 10, 26)),
    ("Antarctica/Troll", (2025, 3, 30)),
    ("Antarctica/Troll", (2025, 10, 26)),
]


@pytest.mark.parametrize("tz_name, day", DST_DAYS)
def test_no_real_slot_is_ever_lost_on_a_transition_day(tz_name, day):
    """The only permitted deviation from a wall clock cron is the documented
    fall back rule: the SECOND occurrence of a repeated wall time never fires.
    Anything else missing is a silent miss."""
    tz = ZoneInfo(tz_name)
    for minute, hour in [
        ("*/15", "*"),
        ("0", "*"),
        ("30", "2,3"),
        ("0,30", "2,3"),
        ("15", "1,2,3"),
        ("0", "0-5"),
        ("*/20", "1-4"),
        ("0,15,30,45", "*"),
    ]:
        s = crontab(minute=minute, hour=hour, timezone=tz_name)
        fired = set(fires_during_local_day(s, tz_name, *day))
        for want in real_matching_instants(s, tz_name, *day):
            if want in fired:
                continue
            local = datetime.fromtimestamp(want / 1000, tz=tz).replace(tzinfo=None)
            first = local.replace(tzinfo=tz, fold=0)
            second = local.replace(tzinfo=tz, fold=1)
            repeated = first.utcoffset() != second.utcoffset()
            assert repeated and ms(second) == want, (
                f"{tz_name} {day} '{minute} {hour}' lost a real slot at {want}"
            )
