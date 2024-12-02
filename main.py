import random
import time
from database import Node
from scheduler import Scheduler
from transactions import TransactionManager


def simulate_transactions(transaction_manager):
    transactions = []

    # Add 200 courses
    for i in range(101, 301):
        transactions.append((transaction_manager.add_course, (i, random.randint(20, 100), "Dept", f"Course {i}")))

    # Add 150 students
    for i in range(1001, 1151):
        transactions.append((transaction_manager.add_student, (i, f"Student{i}")))

    # Enroll 150 students into random courses
    for i in range(1, 151):
        student_id = random.randint(1001, 1150)
        course_id = random.randint(101, 300)
        transactions.append((transaction_manager.enroll_course, (i, student_id, course_id)))

    # Add 150 feedback entries
    for i in range(1, 151):
        student_id = random.randint(1001, 1150)
        course_id = random.randint(101, 300)
        feedback = random.choice([
            "Excellent!", "Challenging!", "Very informative!",
            "Could be better.", "Loved it!", "Too fast-paced."
        ])
        transactions.append((transaction_manager.enter_feedback, (i, student_id, course_id, feedback)))

    # Add 100 duplicate feedback operations for load testing
    for i in range(151, 201):
        student_id = random.randint(1001, 1150)
        course_id = random.randint(101, 300)
        transactions.append((transaction_manager.enter_feedback, (i, student_id, course_id, "Repeated Feedback")))

    # Add 50 invalid transactions (edge cases)
    for i in range(201, 251):
        student_id = random.choice([9999, 1000])  # Invalid/edge case student IDs
        course_id = random.randint(101, 300)
        transactions.append((transaction_manager.enroll_course, (i, student_id, course_id)))

    # Add 50 additional course updates
    for i in range(301, 351):
        course_id = random.randint(101, 300)
        transactions.append((transaction_manager.add_course, (course_id, random.randint(20, 100), "UpdatedDept", f"UpdatedCourse {course_id}")))

    # Add 70 additional enrollments
    for i in range(251, 321):
        student_id = random.randint(1001, 1150)
        course_id = random.randint(101, 300)
        transactions.append((transaction_manager.enroll_course, (i, student_id, course_id)))

    # Add 80 feedback fillers
    for i in range(321, 401):
        student_id = random.randint(1001, 1150)
        course_id = random.randint(101, 300)
        feedback = random.choice([
            "Fantastic course!", "Needs improvement.", "Too theoretical.",
            "Best class ever!", "Not worth it."
        ])
        transactions.append((transaction_manager.enter_feedback, (i, student_id, course_id, feedback)))

    # Add 100 advanced edge-case operations
    for i in range(401, 501):
        student_id = random.randint(1001, 1150)
        course_id = random.randint(101, 300)
        if random.random() < 0.5:
            # Simulate enrollments with potentially invalid course IDs
            transactions.append((transaction_manager.enroll_course, (i, student_id, random.choice([999, 2001]))))
        else:
            # Simulate feedback with a mix of valid and invalid data
            feedback = random.choice(["", None, "Edge case feedback."])
            transactions.append((transaction_manager.enter_feedback, (i, student_id, course_id, feedback)))

    # Add 50 random operations
    for i in range(501, 551):
        operation = random.choice([
            transaction_manager.add_course,
            transaction_manager.add_student,
            transaction_manager.enroll_course,
            transaction_manager.enter_feedback
        ])
        if operation == transaction_manager.add_course:
            course_id = random.randint(301, 350)
            transactions.append((operation, (course_id, random.randint(20, 100), "GeneratedDept", f"GeneratedCourse {course_id}")))
        elif operation == transaction_manager.add_student:
            student_id = random.randint(1151, 1170)
            transactions.append((operation, (student_id, f"Student{student_id}")))
        elif operation == transaction_manager.enroll_course:
            student_id = random.randint(1001, 1170)
            course_id = random.randint(101, 350)
            transactions.append((operation, (len(transactions) + 1, student_id, course_id)))
        elif operation == transaction_manager.enter_feedback:
            student_id = random.randint(1001, 1170)
            course_id = random.randint(101, 350)
            feedback = random.choice([
                "Great learning experience!", "Not enough examples.", "Too many lectures.",
                "Loved the group work.", "Could use more practical examples."
            ])
            transactions.append((operation, (len(transactions) + 1, student_id, course_id, feedback)))

    # Output count to confirm
    print(f"Generated {len(transactions)} transactions.")

    chains = []
    for transaction, args in transactions:
        chain = transaction(*args)
        chains.append(chain)

    start_time = time.time()
    #transaction_manager.scheduler.execute_chains_concurrently(chains)
    transaction_manager.scheduler.execute_limited_chains(chains, max_threads=50)
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


    #print("\nFinal Schedule:")
    #print(" -> ".join(transaction_manager.scheduler.schedule))
    #print("=============================================")


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

    #scheduler.execute_chains_concurrently(chains, new_transaction)
    transaction_manager.scheduler.handle_sc_cycle(chains, new_transaction)


    metrics = scheduler.report_metrics()
    print("=============================================")
    print("\nMetrics:")
    print(f"Average Latency: {metrics['average_latency']} seconds")
    print(f"Max Latency: {metrics['max_latency']} seconds")
    print(f"Min Latency: {metrics['min_latency']} seconds")
    print(f"Throughput: {metrics['total_operations'] / sum(metrics.values())} operations/second")
    #print("\nFinal Schedule:")
    #print(" -> ".join(scheduler.schedule))
    #print("=============================================")



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
