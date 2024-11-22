# transactions.py

import time


class TransactionManager:
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.transaction_count = 1

    def add_course(self, course_id, class_size, department, name):
        transaction_id = self.transaction_count
        self.transaction_count += 1
        return [
            {"node": 2, "operation": "read", "args": ("Courses", course_id), "id": f"T{transaction_id}1"},
            {"node": 2, "operation": "write", "args": ("Courses", [course_id, class_size, department, name]), "id": f"T{transaction_id}2"}
        ]

    def enroll_course(self, enrollment_id, student_id, course_id):
        transaction_id = self.transaction_count
        self.transaction_count += 1
        return [
            {"node": 2, "operation": "read", "args": ("Courses", course_id, 1), "id": f"T{transaction_id}1"},
            {"node": 1, "operation": "read", "args": ("Enrollments", course_id), "id": f"T{transaction_id}2"},
            {"node": 1, "operation": "read", "args": ("Students", student_id), "id": f"T{transaction_id}3"},
            {"node": 1, "operation": "write", "args": ("Enrollments", [enrollment_id, student_id, course_id, time.time()]), "id": f"T{transaction_id}4"}
        ]

    def add_student(self, student_id, name):
        transaction_id = self.transaction_count
        self.transaction_count += 1
        return [
            {"node": 1, "operation": "read", "args": ("Students", student_id), "id": f"T{transaction_id}1"},
            {"node": 1, "operation": "write", "args": ("Students", [student_id, name]), "id": f"T{transaction_id}2"}
        ]

    def enter_feedback(self, feedback_id, student_id, course_id, feedback):
        transaction_id = self.transaction_count
        self.transaction_count += 1
        return [
            {"node": 1, "operation": "read", "args": ("Enrollments", student_id), "id": f"T{transaction_id}1"},
            {"node": 3, "operation": "write", "args": ("Feedback", [feedback_id, student_id, course_id, time.time(), feedback]), "id": f"T{transaction_id}2"}
        ]
