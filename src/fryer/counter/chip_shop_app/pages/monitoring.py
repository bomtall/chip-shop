import fcntl
import json
import threading
import time
from collections import deque
from pathlib import Path

import plotly.graph_objects as go
import streamlit as st

import fryer.datetime
import fryer.logger
import fryer.path
from fryer.counter import monitor

st.set_page_config(
    page_title="chip-shop",
    page_icon="🍟",
    layout="wide",
    initial_sidebar_state="expanded",
)


def system_monitoring_stats(
    network_interface: str,
    logger: fryer.logger.TypeLogger,
) -> None:
    """Get the system monitoring statistics and save to file in JSON format continuously."""
    logger.info(f"Starting system monitoring stats {fryer.datetime.now()}")
    while True:
        monitoring_json = json.dumps(
            monitor.get_stats_dict(network_interface), indent=4
        )
        directory = fryer.path.data() / KEY
        directory.mkdir(parents=True, exist_ok=True)
        with (directory / "monitor.json").open("w") as file:
            fcntl.flock(file.fileno(), fcntl.LOCK_EX)
            file.write(monitoring_json)
            fcntl.flock(file.fileno(), fcntl.LOCK_UN)


usage_chart_container = st.empty()
temperature_chart_container = st.empty()
network_chart_container = st.empty()

KEY = Path(__file__).stem
path_log = fryer.path.log() / KEY

logger = fryer.logger.get(key=KEY, path_log=path_log)
thread = threading.Thread(
    target=system_monitoring_stats,
    daemon=True,
    args=("enp11s0", logger),
)
thread.start()

metrics = [
    "timestamp",
    "cpu_temp",
    "cpu_percentage",
    "netin",
    "netout",
    "ram_percentage",
]
deques: dict[str, deque[str | float | int]] = {
    metric: deque(maxlen=30) for metric in metrics
}


def make_chart(
    x: list[str | float | int],
    y: list[str | float | int] | list[list[str | float | int]],
    trace_names: list[str],
    title: str,
) -> go.Figure:
    fig = go.Figure(
        layout={"height": 230},
    )
    for i in range(len(y)):
        fig.add_trace(go.Scatter(x=x, y=y[i], mode="lines", name=trace_names[i]))

    fig.update_layout(
        title={"text": title, "font": {"size": 10}},
        margin={"l": 15, "r": 15, "t": 15, "b": 15},
    )
    return fig


i = 0
while True:
    with (fryer.path.data() / KEY / "monitor.json").open("r", encoding="utf-8") as file:
        fcntl.flock(file.fileno(), fcntl.LOCK_SH)
        json_data = json.load(file)
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)

    for metric in metrics:
        deques[metric].append(json_data[metric])

    usage_chart = make_chart(
        x=list(deques["timestamp"]),
        y=[list(deques["ram_percentage"]), list(deques["cpu_percentage"])],
        trace_names=["Ram % Utilisation", "CPU % Utilisation"],
        title="Chip-Shop Utilisation Monitoring",
    )
    usage_chart_container.plotly_chart(
        usage_chart, use_container_width=True, key=f"usage_chart_{i}"
    )

    temperature_chart = make_chart(
        x=list(deques["timestamp"]),
        y=list(deques["cpu_temp"]),
        trace_names=["CPU Temperature"],
        title="Chip-Shop Temperature Monitoring",
    )
    temperature_chart_container.plotly_chart(
        temperature_chart, use_container_width=True, key=f"temperature_chart_{i + 1}"
    )

    network_chart = make_chart(
        x=list(deques["timestamp"]),
        y=[list(deques["netin"]), list(deques["netout"])],
        trace_names=["Network mbps In", "Network mbps Out"],
        title="Chip-Shop Network Monitoring",
    )
    network_chart_container.plotly_chart(
        network_chart, use_container_width=True, key=f"network_chart_{i + 2}"
    )

    i += 3

    time.sleep(2)
