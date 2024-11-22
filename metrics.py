import time
from threading import Lock


class Metrics:
    def __init__(self):
        self.latencies = []
        self.lock = Lock()

    def record_latency(self, start_time, end_time):
        latency = end_time - start_time
        with self.lock:
            self.latencies.append(latency)

    def calculate_throughput(self, total_operations, duration):
        return total_operations / duration

    def report_metrics(self):
        with self.lock:
            if not self.latencies:
                return {
                    "average_latency": None,
                    "max_latency": None,
                    "min_latency": None,
                    "total_operations": 0
                }
            total_operations = len(self.latencies)
            total_time = sum(self.latencies)
            return {
                "average_latency": total_time / total_operations,
                "max_latency": max(self.latencies),
                "min_latency": min(self.latencies),
                "total_operations": total_operations
            }
