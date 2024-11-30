import random
import time
from database import Node
from scheduler import Scheduler
from transactions import TransactionManager


def simulate_transactions(transaction_manager):
    transactions = [
        (transaction_manager.add_course, (101, 30, "CS", "Intro to Programming")),
        (transaction_manager.add_course, (102, 25, "Math", "Calculus I")),
        (transaction_manager.add_student, (1001, "Alice")),
        (transaction_manager.add_student, (1002, "Bob")),
        (transaction_manager.enroll_course, (1, 1001, 101)),  
        (transaction_manager.enroll_course, (2, 1002, 101)), 
        (transaction_manager.enter_feedback, (1, 1001, 101, "Great course!")),
        (transaction_manager.enter_feedback, (2, 1002, 102, "Challenging!")),
        (transaction_manager.add_course, (1201, 30, "CS", "Intro to Java")),
        (transaction_manager.add_course, (1202, 25, "Math", "Calculus II")),
        (transaction_manager.add_student, (10501, "Alice")),
        (transaction_manager.add_student, (10502, "Bob")),
        (transaction_manager.enroll_course, (6, 10501, 1201)),  
        (transaction_manager.enroll_course, (5, 10502, 1201)), 
        (transaction_manager.enter_feedback, (3, 10501, 1201, "Great course!")),
        (transaction_manager.enter_feedback, (4, 10502, 1202, "Challenging!"))

    ]

    chains = []
    for transaction, args in transactions:
        chain = transaction(*args)
        chains.append(chain)

    start_time = time.time()
    transaction_manager.scheduler.execute_chains_concurrently(chains)
    end_time = time.time()

    duration = end_time - start_time
    metrics = transaction_manager.scheduler.report_metrics()
    throughput = transaction_manager.scheduler.metrics.calculate_throughput(
        metrics["total_operations"], duration
    )

    print("=============================================")
    print("\nMetrics:")
    print(f"Average Latency: {metrics['average_latency']} seconds")
    print(f"Max Latency: {metrics['max_latency']} seconds")
    print(f"Min Latency: {metrics['min_latency']} seconds")
    print(f"Throughput: {throughput} operations/second")


    print("\nFinal Schedule:")
    print(" -> ".join(transaction_manager.scheduler.schedule))
    print("=============================================")


def simulate_SC_cycles(transaction_manager):
    chains = [
        [
            {"node": 2, "operation": "read", "args": ("Courses", 101, 1), "id": "T21"},
            {"node": 1, "operation": "read", "args": ("Enrollments", 101), "id": "T22"},
            {"node": 1, "operation": "read", "args": ("Students", 1001), "id": "T23"},
            {"node": 1, "operation": "write", "args": ("Enrollments", [507, 1001, 101, time.time()]), "id": "T24"}
        ]
    ]


    new_transaction = [
        {"node": 1, "operation": "write", "args": ("Enrollments", [507, 1001, 101, time.time()]), "id": "T31"},
        {"node": 2, "operation": "write", "args": ("Courses", [101, 50, "CS", "Updated Programming"]), "id": "T32"}
    ]

    scheduler.execute_chains_concurrently(chains, new_transaction)


    metrics = scheduler.report_metrics()
    print("=============================================")
    print("\nMetrics:")
    print(f"Average Latency: {metrics['average_latency']} seconds")
    print(f"Max Latency: {metrics['max_latency']} seconds")
    print(f"Min Latency: {metrics['min_latency']} seconds")
    print(f"Throughput: {metrics['total_operations'] / sum(metrics.values())} operations/second")
    print("\nFinal Schedule:")
    print(" -> ".join(scheduler.schedule))
    print("=============================================")



if __name__ == "__main__":
    nodes = {
        1: Node(1, ["Students", "Enrollments"]),
        2: Node(2, ["Courses"]),
        3: Node(3, ["Feedback"])
    }
    scheduler = Scheduler(nodes)
    transaction_manager = TransactionManager(scheduler)
    simulate_transactions(transaction_manager)
    #simulate_SC_cycles(transaction_manager)
