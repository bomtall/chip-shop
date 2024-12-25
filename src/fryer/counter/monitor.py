import sys
import json
import time
import psutil
import signal
import threading
import subprocess
import socketserver
from http import server
from pathlib import Path

import fryer.path
import fryer.logger
import fryer.datetime
from fryer.typing import TypePathLike

# close port manually: fuser -k 12669/tcp

# Network interfaces on chip-shop
# lo - loopback interface: Internal communication within the local machine (e.g., services communicating with each other on 127.0.0.1).
# enp11s0 - Ethernet interface: Wired network connection.
# wlp12s0 - Wireless interface: Wireless network connection.
# tailscale0 - Tailscale interface: VPN connection.
# docker0 - Docker interface: Docker network connection.

KEY = Path(__file__).stem


def get_cpu_core_temperatures() -> list[float]:
    """
    Get the temperature of each CPU core in degrees Celsius.
    """
    temps = (
        subprocess.run(
            "cat /sys/class/hwmon/hwmon*/temp1_input",
            shell=True,
            stdout=subprocess.PIPE,
        )
        .stdout.decode("utf-8")
        .split("\n")
    )
    return [int(i) / 1000 for i in temps if i]


def get_cpu_temperature() -> float:
    """
    Get the average CPU temperature of all cores in degrees Celsius.
    """
    temps = get_cpu_core_temperatures()
    return sum(temps) / len(temps)


def get_bytes_io(network_interface: str) -> tuple[int, int]:
    """
    Get the number of bytes received and sent on a network interface.
    """
    netstats = psutil.net_io_counters(pernic=True, nowrap=True)[network_interface]
    return netstats.bytes_recv, netstats.bytes_sent


def get_network_stats(network_interface: str):
    """
    Get the network speed in megabytes per second (MB/s) for a network interface.
    """
    bytes_in_1, bytes_out_1 = get_bytes_io(network_interface=network_interface)
    time.sleep(1)
    bytes_in_2, bytes_out_2 = get_bytes_io(network_interface=network_interface)

    mbps_in = round((bytes_in_2 - bytes_in_1) / 1048576, 3)
    mbps_out = round((bytes_out_2 - bytes_out_1) / 1048576, 3)

    return mbps_in, mbps_out


def system_monitoring_stats(network_interface: str, logger) -> None:
    """
    Get the system monitoring statistics and save to file in JSON format continuously.
    """
    logger.info(f"Starting system monitoring stats {fryer.datetime.now()}")
    while True:
        data_dict = {}
        mbytesin, mbytesout = get_network_stats(network_interface=network_interface)
        cputemp = get_cpu_temperature()
        rampc = psutil.virtual_memory().percent
        cpupc = psutil.cpu_percent(interval=1, percpu=False)

        data_dict["cpu_temp"] = cputemp
        data_dict["netin"] = mbytesin
        data_dict["netout"] = mbytesout
        data_dict["cpu_percentage"] = cpupc
        data_dict["ram_percentage"] = rampc
        data_dict["timestamp"] = fryer.datetime.now().strftime("%H:%M:%S")

        monitoring_json = json.dumps(data_dict, indent=4)
        directory = fryer.path.data() / KEY
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "monitor.json").write_text(monitoring_json)


def signal_handler(
    sig,
    frame,
    path_log: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> None:
    logger = fryer.logger.get(key=KEY, path_log=path_log, path_env=path_env)
    print("Shutting down the server...")
    subprocess.run("fuser -k 12669/tcp", shell=True)
    logger.info("Shutting down the server...")
    sys.exit(0)


class StreamingHandler(server.BaseHTTPRequestHandler):
    """
    Handles the streaming of the monitoring data to the client.
    """

    def do_GET(self):
        if self.path == "/":
            self.send_response(301)
            self.send_header("Location", "/index.html")
            self.end_headers()
        elif self.path == "/monitoring.json":
            file = (fryer.path.data() / KEY / "monitor.json").read_text()
            content = file.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/json")
            self.send_header("Content-Length", len(content))
            self.end_headers()
            self.wfile.write(content)

        elif self.path == "/index.html":
            page = (Path(__file__).parent / "monitor.html").read_text()
            content = page.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", len(content))
            self.end_headers()
            self.wfile.write(content)

        else:
            self.send_error(404)
            self.end_headers()


class StreamingServer(socketserver.ThreadingMixIn, server.HTTPServer):
    allow_reuse_address = True
    daemon_threads = True


def main(
    path_log: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    logger = fryer.logger.get(key=KEY, path_log=path_log, path_env=path_env)
    y = threading.Thread(
        target=system_monitoring_stats, daemon=True, args=("enp11s0", logger)
    )
    y.start()
    shutdown_event = threading.Event()

    try:
        address = ("", 12669)
        global SERVER
        SERVER = StreamingServer(address, StreamingHandler)
        print("Server started")
        logger.info(f"Server started on port {address[1]}")
        signal.signal(signal.SIGINT, signal_handler)
        while not shutdown_event.is_set():
            SERVER.handle_request()
    finally:
        y.join()


if __name__ == "__main__":
    main()
