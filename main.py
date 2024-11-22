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
        (transaction_manager.enter_feedback, (2, 1002, 102, "Challenging!"))
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


if __name__ == "__main__":
    nodes = {
        1: Node(1, ["Students", "Enrollments"]),
        2: Node(2, ["Courses"]),
        3: Node(3, ["Feedback"])
    }
    scheduler = Scheduler(nodes)
    transaction_manager = TransactionManager(scheduler)
    simulate_transactions(transaction_manager)
