from fryer.counter.monitor import (
    get_bytes_io,
    get_cpu_core_temperatures,
    get_cpu_temperature,
    get_network_stats,
    get_stats_dict,
)


def test_get_cpu_core_temperatures():
    core_temps = get_cpu_core_temperatures()
    assert isinstance(core_temps, list)
    assert all(isinstance(x, float) for x in core_temps)


def test_get_cpu_temperature():
    cpu_temp = get_cpu_temperature()
    assert isinstance(cpu_temp, float)
    assert (
        0 <= cpu_temp <= 100
    )  # CPU temperature should be between 0 and 100 degrees Celsius


def test_get_bytes_io():
    result = get_bytes_io("lo")
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert all(isinstance(x, int) for x in result)


def test_get_network_stats():
    result = get_network_stats("lo")
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert all(isinstance(x, float) for x in result)


def test_get_stats_dict():
    result = get_stats_dict("lo")
    assert isinstance(result, dict)
    assert len(result) == 6
    assert all(isinstance(x, (float | str)) for x in result.values())
    assert result.keys() == {
        "cpu_temp",
        "netin",
        "netout",
        "cpu_percentage",
        "ram_percentage",
        "timestamp",
    }
