from metrics import Metrics
import threading
import time
from concurrent.futures import ThreadPoolExecutor

class Scheduler:
    def __init__(self, nodes):
        self.nodes = nodes
        self.schedule = []
        self.schedule_lock = threading.Lock()
        self.metrics = Metrics()
        self.execution_order = []  
        self.completed_transactions = set()  
    
    def build_sc_graph(self, chains, new_transaction=None):
        graph = {}

        for chain in chains:
            for hop in chain:
                graph[hop["id"]] = {"s_edges": set(), "c_edges": set()}

        for chain in chains:
            for i in range(len(chain) - 1):
                graph[chain[i]["id"]]["s_edges"].add(chain[i + 1]["id"])

        for chain1 in chains:
            for chain2 in chains:
                if chain1 == chain2:
                    continue  
                for hop1 in chain1:
                    for hop2 in chain2:
                        if self.conflicts(hop1, hop2):
                            print(f"Adding conflict edge: {hop1['id']} -> {hop2['id']}")  
                            graph[hop1["id"]]["c_edges"].add(hop2["id"])

        if new_transaction:
            for hop in new_transaction:
                graph[hop["id"]] = {"s_edges": set(), "c_edges": set()}

            for i in range(len(new_transaction) - 1):
                graph[new_transaction[i]["id"]]["s_edges"].add(new_transaction[i + 1]["id"])

            for chain in chains:
                for hop1 in chain:
                    for hop2 in new_transaction:
                        if self.conflicts(hop1, hop2):
                            print(f"Adding conflict edge between chain and new transaction: {hop1['id']} -> {hop2['id']}")  # Debug output
                            graph[hop1["id"]]["c_edges"].add(hop2["id"])

        return graph

    def conflicts(self, hop1, hop2):
        if hop1["operation"] == "write" and hop2["operation"] == "write":
            if hop1["args"][0] == hop2["args"][0] and hop1["args"][1] == hop2["args"][1]:
                print(f"Write-Write Conflict detected: {hop1['id']} and {hop2['id']}")
                return True

        elif hop1["operation"] == "read" and hop2["operation"] == "write":
            if hop1["args"][0] == hop2["args"][0] and hop1["args"][1] == hop2["args"][1]:
                print(f"Read-Write Conflict detected: {hop1['id']} and {hop2['id']}")
                return True

        elif hop1["operation"] == "write" and hop2["operation"] == "read":
            if hop1["args"][0] == hop2["args"][0] and hop1["args"][1] == hop2["args"][1]:
                print(f"Write-Read Conflict detected: {hop1['id']} and {hop2['id']}")
                return True

        return False


    def detect_sc_cycle(self, graph):
        visited = set()
        stack = set()

        def dfs(vertex):
            if vertex in stack:
                print(f"Cycle detected at {vertex}")
                return True
            if vertex in visited:
                return False
            
            visited.add(vertex)
            stack.add(vertex)

            for neighbor in graph[vertex]["s_edges"]:
                if dfs(neighbor):
                    return True

            for neighbor in graph[vertex]["c_edges"]:
                if dfs(neighbor):
                    return True

            stack.remove(vertex)
            return False

        for vertex in graph:
            if dfs(vertex):
                return True

        return False

    def execute_chains_concurrently(self, chains, new_transaction=None):
        if new_transaction and new_transaction[0]["id"] in self.completed_transactions:
            print(f"Transaction {new_transaction[0]['id']} already completed, skipping execution.")
            return
        
        if new_transaction:  
            chains.append(new_transaction)

        sc_graph = self.build_sc_graph(chains)

        if self.detect_sc_cycle(sc_graph):
            print("SC-cycle detected! Waiting for current transactions to finish.")
            self.handle_sc_cycle(chains, new_transaction) 
            print("Executing the transactions after resolving the cycle.")

        self.execute_chains(chains)
    
    def execute_limited_chains(self, chains, max_threads):
        max_hops = max(len(chain) for chain in chains)
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            for hop_index in range(max_hops):
                futures = []
                for chain in chains:
                    if hop_index < len(chain):
                        hop = chain[hop_index]
                        if hop["id"] not in self.completed_transactions:
                            self.completed_transactions.add(hop["id"])
                            futures.append(executor.submit(self.execute_hop, hop))
                for future in futures:
                    future.result()


    def handle_sc_cycle(self, chains, new_transaction):
        if new_transaction is None:
            print("No new transaction to handle during SC-cycle")
            return

        if new_transaction[0]["id"] in self.completed_transactions:
            print(f"Transaction {new_transaction[0]['id']} has already been completed.")
            return
        
        dependent_chain = self.get_dependent_chain(chains, new_transaction)
        if dependent_chain:
            print(f"Waiting for {dependent_chain} to complete before proceeding with {new_transaction}")
            self.execute_limited_chains([dependent_chain], max_threads=50) 
            #self.execute_chains_concurrently([dependent_chain])  
            chains.append(new_transaction)  
    
    def get_dependent_chain(self, chains, new_transaction):
        for chain in chains:
            if self.transaction_depends_on(chain, new_transaction):
                return chain
        return None

    def transaction_depends_on(self, chain, new_transaction):
        for hop in chain:
            for hop_new in new_transaction:
                if hop['node'] == hop_new['node'] and hop['operation'] == "write" and hop['args'][0] == hop_new['args'][0]:
                    return True
        return False

    
    def execute_chains(self, chains):
        max_hops = max(len(chain) for chain in chains)
        for hop_index in range(max_hops):
            threads = []
            for chain in chains:
                if hop_index < len(chain):
                    hop = chain[hop_index]
                    if hop["id"] not in self.completed_transactions:
                        self.completed_transactions.add(hop["id"])
                        thread = threading.Thread(target=self.execute_hop, args=(hop,))
                        threads.append(thread)
                        thread.start()
            for thread in threads:
                thread.join()

    def execute_hop(self, hop):
        start_time = time.time()
        node = self.nodes[hop["node"]]
        method = getattr(node, hop["operation"])
        args = hop["args"]
        with self.schedule_lock:
            method(*args, hop_id=hop["id"], schedule=self.schedule)
        end_time = time.time()

        self.metrics.record_latency(start_time, end_time)

    def report_metrics(self):
        return self.metrics.report_metrics()
