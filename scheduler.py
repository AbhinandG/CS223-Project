from metrics import Metrics
import threading
import time

class Scheduler:
    def __init__(self, nodes):
        self.nodes = nodes
        self.schedule = []
        self.schedule_lock = threading.Lock()
        self.metrics = Metrics() 

    def execute_hop(self, hop):
        start_time = time.time()
        node = self.nodes[hop["node"]]
        method = getattr(node, hop["operation"])
        args = hop["args"]
        with self.schedule_lock:
            method(*args, hop_id=hop["id"], schedule=self.schedule)
        end_time = time.time()

        self.metrics.record_latency(start_time, end_time)

    def execute_chains_concurrently(self, chains):
        max_hops = max(len(chain) for chain in chains)
        for hop_index in range(max_hops):
            threads = []
            for chain in chains:
                if hop_index < len(chain):
                    hop = chain[hop_index]
                    thread = threading.Thread(target=self.execute_hop, args=(hop,))
                    threads.append(thread)
                    thread.start()
            for thread in threads:
                thread.join()

    def report_metrics(self):
        return self.metrics.report_metrics()
