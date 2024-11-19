import threading
import time
import random
from collections import defaultdict

class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.locks = defaultdict(threading.Lock)
        self.data = {}

    def acquire_lock(self, key):
        self.locks[key].acquire()

    def release_lock(self, key):
        self.locks[key].release()

    def read(self, key):
        return self.data.get(key, None)

    def write(self, key, value):
        self.data[key] = value

class TransactionManager:
    def __init__(self):
        self.nodes = [Node(1), Node(2), Node(3)]
        self.log = []

    def execute_chain(self, chain):
        for hop in chain:
            node = self.nodes[hop['node'] - 1]
            self.log.append(f"Start hop {hop['name']} on Node {node.node_id}")
            
            for key in hop['read_set'] + hop['write_set']:
                node.acquire_lock(key)
                self.log.append(f"  Acquired lock for {key} on Node {node.node_id}")

            for key in hop['read_set']:
                value = node.read(key)
                self.log.append(f"  Read {key}={value} on Node {node.node_id}")

            for key, value in hop['write_set']:
                node.write(key, value)
                self.log.append(f"  Wrote {key}={value} on Node {node.node_id}")

            for key in hop['read_set'] + hop['write_set']:
                node.release_lock(key)
                self.log.append(f"  Released lock for {key} on Node {node.node_id}")

            self.log.append(f"End hop {hop['name']} on Node {node.node_id}")

    def process_transactions(self, transactions):
        threads = []
        for t in transactions:
            thread = threading.Thread(target=self.execute_chain, args=(t,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

# Define transactions
T1 = [
    {'name': 'T11', 'node': 2, 'read_set': ['courses'], 'write_set': []},
    {'name': 'T12', 'node': 2, 'read_set': [], 'write_set': [('courses', 'new_course')]}
]

T2 = [
    {'name': 'T21', 'node': 2, 'read_set': ['courses'], 'write_set': []},
    {'name': 'T22', 'node': 1, 'read_set': ['enrollments'], 'write_set': []},
    {'name': 'T23', 'node': 1, 'read_set': ['students'], 'write_set': []},
    {'name': 'T24', 'node': 1, 'read_set': [], 'write_set': [('enrollments', 'new_enrollment')]}
]

T3 = [
    {'name': 'T31', 'node': 1, 'read_set': ['students'], 'write_set': []},
    {'name': 'T32', 'node': 1, 'read_set': [], 'write_set': [('students', 'new_student')]}
]

T4 = [
    {'name': 'T41', 'node': 1, 'read_set': ['enrollments'], 'write_set': []},
    {'name': 'T42', 'node': 3, 'read_set': [], 'write_set': [('feedback', 'new_feedback')]}
]

# Simulate transaction processing
tm = TransactionManager()
transactions = [T1, T2, T3, T4]
random.shuffle(transactions)  # Randomize transaction order
tm.process_transactions(transactions)

# Print the schedule
for entry in tm.log:
    print(entry)