import io
import time
import json
import subprocess
import threading
import socketserver
from http import server
from threading import Condition
import psutil
from pathlib import Path

# from fryer.constants import FORMAT_ISO_DATE
import fryer.datetime
import fryer.logger
import fryer.path
# from fryer.typing import TypeDatetimeLike, TypePathLike


# close port manually: fuser -k 12669/tcp

KEY = Path(__file__).stem
logger = fryer.logger.get(key=KEY)


def get_cpu_temperature():
    temp = -1

    temps = (
        subprocess.run(
            "cat /sys/class/hwmon/hwmon*/temp1_input",
            shell=True,
            stdout=subprocess.PIPE,
        )
        .stdout.decode("utf-8")
        .split("\n")
    )
    temps = [int(i) for i in temps if i]
    temp = sum(temps) / len(temps)
    return temp / 1000


def get_bytes_io():
    # Network interfaces on chip-shop
    # lo - loopback interface: Internal communication within the local machine (e.g., services communicating with each other on 127.0.0.1).
    # enp11s0 - Ethernet interface: Wired network connection.
    # wlp12s0 - Wireless interface: Wireless network connection.
    # tailscale0 - Tailscale interface: VPN connection.
    # docker0 - Docker interface: Docker network connection.

    netstats = psutil.net_io_counters(pernic=True, nowrap=True)["enp11s0"]
    return netstats.bytes_recv, netstats.bytes_sent


def get_network_stats():
    bytes_in_1, bytes_out_1 = get_bytes_io()
    time.sleep(1)
    bytes_in_2, bytes_out_2 = get_bytes_io()

    mbps_in = round((bytes_in_2 - bytes_in_1) / 1048576, 3)
    mbps_out = round((bytes_out_2 - bytes_out_1) / 1048576, 3)

    return mbps_in, mbps_out


def system_monitoring_stats():
    while True:
        data_dict = {}
        # graph_dict = {}
        mbytesin, mbytesout = get_network_stats()
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


class StreamingOutput(io.BufferedIOBase):
    def __init__(self):
        self.frame = None
        self.condition = Condition()

    def write(self, buf):
        with self.condition:
            self.frame = buf
            self.condition.notify_all()


class StreamingHandler(server.BaseHTTPRequestHandler):
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
            content = PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", len(content))
            self.end_headers()
            self.wfile.write(content)

        elif self.path == "/netstatsdata":
            self.send_response(200)
            self.send_header("Age", 0)
            self.send_header("Cache-Control", "no-cache, private")
            self.send_header("Pragma", "no-cache")
            self.send_header(
                "Content-Type", "multipart/x-mixed-replace; boundary=FRAME"
            )  # ; boundary=FRAME
            self.end_headers()
            while True:
                content = system_monitoring_stats().encode("utf-8")
                self.wfile.write(b"--FRAME\r\n")
                self.send_header("Content-Type", "multipart/x-mixed-replace")
                self.send_header("Content-Length", len(content))
                self.end_headers()
                self.wfile.write(content)
                self.wfile.write(b"\r\n")

        else:
            self.send_error(404)
            self.end_headers()


class StreamingServer(socketserver.ThreadingMixIn, server.HTTPServer):
    allow_reuse_address = True
    daemon_threads = True


PAGE = ""
path = Path(__file__).parent / "monitor.html"
PAGE = path.read_text()

y = threading.Thread(target=system_monitoring_stats, daemon=True)
y.start()

try:
    address = ("", 12669)
    server = StreamingServer(address, StreamingHandler)
    print("Server started")
    server.serve_forever()

finally:
    y.join()
