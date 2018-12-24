from src.computed import compute_price_changes
from src.models import Trade, DeltaPoint
import pendulum
import pytest

def test_raise_on_invalid_arguments():
    start_point = DeltaPoint(pendulum.now(), 10, 0, 0, 0)
    not_supported_interval = 10
    with pytest.raises(ValueError):
        compute_price_changes(start_point, [], not_supported_interval)

    valid_interval = 20
    invalid_trades = []
    with pytest.raises(ValueError):
        compute_price_changes(start_point, invalid_trades, valid_interval)

    not_sorted_trades = [
        Trade(pendulum.now(), 10, 0, 'BTC'),
        Trade(pendulum.now().subtract(seconds=10), 10, 0, 'BTC')
    ]
    with pytest.raises(ValueError):
        compute_price_changes(start_point, not_sorted_trades, valid_interval)

def test_compute_price_changes():
    # Given starting point
    start_time = pendulum.parse('2018-01-01T00:00:00Z')
    start_point = DeltaPoint(start_time, 10, 0, 0, 0)

    # Given trades
    trades = [
        Trade(start_time.add(minutes=1, seconds=10), 20, 0, 'BTCUSD'),
        Trade(start_time.add(minutes=1, seconds=30), 22, 0, 'BTCUSD')
    ]

    # Given interval in seconds
    interval = 20

    # Output new points
    result = compute_price_changes(start_point, trades, interval)

    expect = [
        Trade(start_time.add(seconds=interval), 0,0,0),
        Trade(start_time.add(seconds=interval*2), 0,0,0),
        Trade(start_time.add(seconds=interval*3), 0,0,0),
        Trade(start_time.add(seconds=interval*4), 0,0,0),
    ]

    assert len(result) == len(expect)        

    for i in range(len(expect)):
        assert result[i].time.diff(expect[i].time).in_seconds() == 0

    # Case: no trade between interval
    first = result[0]
    assert first.price == start_point.price
    assert first.d == 0
    assert first.pct == 0
    assert first.n == 0

    # Case: some trade between interval
    fourth = result[3]
    assert fourth.price == 20
    assert fourth.d == 20 - 10
    assert fourth.pct == 100
    assert fourth.n == 1