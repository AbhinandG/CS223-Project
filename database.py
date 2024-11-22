import csv
from collections import defaultdict
from readerwriterlock.rwlock import RWLockWrite
import threading


class Node:
    def __init__(self, node_id, tables):
        self.node_id = node_id
        self.tables = {table: table for table in tables} 
        self.locks = defaultdict(RWLockWrite)
        self.origin_queue = []
        self.origin_lock = threading.Lock()

    def acquire_lock(self, table, key, exclusive=False):
        lock = self.locks[(table, key)]
        if exclusive:
            lock_lock = lock.gen_wlock()
            lock_lock.acquire()
            print(f"Node {self.node_id}: Acquired exclusive lock for {table}:{key}")
        else:
            lock_lock = lock.gen_rlock()
            lock_lock.acquire()
            print(f"Node {self.node_id}: Acquired shared lock for {table}:{key}")
        return lock_lock

    def release_lock(self, lock_lock):
        lock_lock.release()
        print("Lock released")

    def load_csv(self, table):
        """ Load data from the CSV file for the given table """
        try:
            with open(f"{table}.csv", 'r') as f:
                reader = csv.reader(f)
                next(reader, None) 
                return list(reader)
        except FileNotFoundError:
            return []  

    def read(self, table, key, column=None, hop_id=None, schedule=None):
        lock_lock = self.acquire_lock(table, key, exclusive=False)
        try:
            data = self.load_csv(table)  
            if schedule is not None:
                schedule.append(hop_id) 
            print(f"Node {self.node_id}: Reading from {table}:{key}")
            for row in data:
                if len(row) > 0 and row[0] == str(key):  
                    if column is not None:
                        return row[column]
                    return row
            return None
        finally:
            self.release_lock(lock_lock)

    def write(self, table, row, hop_id=None, schedule=None):
        lock_lock = self.acquire_lock(table, row[0], exclusive=True)
        try:
            data = self.load_csv(table)  
            if schedule is not None:
                schedule.append(hop_id)  
            print(f"Node {self.node_id}: Writing to {table}:{row[0]}")

            for existing_row in data:
                if len(existing_row) > 0 and existing_row[0] == str(row[0]):
                    print(f"Node {self.node_id}: Duplicate found, skipping append.")
                    return  

            with open(f"{table}.csv", 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(row) 
            print(f"Node {self.node_id}: Appended new row to {table}")
        finally:
            self.release_lock(lock_lock)
