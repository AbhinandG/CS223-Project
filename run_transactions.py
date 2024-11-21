import csv

def append_to_csv(file_name, row):
    with open(file_name, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(row)

def add_course(course_id, class_size, department, name):
    row = [course_id, class_size, department, name]
    courses = []
    courses.append({
        "course_id": course_id,
        "class_size": class_size,
        "department": department,
        "name": name
    })
    append_to_csv('Courses.csv', row)
    print(f"Course '{name}' added successfully.")

def enroll_student(enrollment_id, student_id, course_id, timestamp):
    #if any(course["course_id"] == course_id for course in courses) and \
    #   any(student["student_id"] == student_id for student in students):
    row = [enrollment_id, student_id, course_id, timestamp]
    enrollments=[]
    enrollments.append({
            "enrollment_id": enrollment_id,
            "student_id": student_id,
            "course_id": course_id,
            "timestamp": timestamp
    })
    append_to_csv('Enrollments.csv', row)
    print(f"Student '{student_id}' enrolled in course '{course_id}'.")
    #else:
    #    print("Error: Invalid student ID or course ID.")

def add_student(student_id, name):
    row = [student_id, name]
    students=[]
    students.append({
        "student_id": student_id,
        "name": name
    })
    append_to_csv('Students.csv', row)
    print(f"Student '{name}' added successfully.")

def submit_feedback(feedback_id, student_id, course_id, timestamp, feedback_text):
    #if any(enrollment["student_id"] == student_id and enrollment["course_id"] == course_id for enrollment in enrollments):
    row = [feedback_id, student_id, course_id, timestamp, feedback_text]
    feedback = []
    feedback.append({
        "feedback_id": feedback_id,
        "student_id": student_id,
        "course_id": course_id,
        "timestamp": timestamp,
        "feedback": feedback_text
    })
    append_to_csv('Feedback.csv', row)
    print(f"Feedback submitted by student '{student_id}' for course '{course_id}'.")
    #else:
     #   print("Error: Student is not enrolled in the course.")


if __name__ == "__main__":
    # Add some courses
    add_course("C101", 25, "Computer Science", "Introduction to Programming")
    add_course("C102", 30, "Mathematics", "Calculus I")

    # Add some students
    add_student("ST2345", "Sarah Johnson")
    add_student("ST3456", "Michael Brown")

    # Enroll students in courses
    enroll_student("E1001", "ST2345", "C101", "2024-08-22 09:15:00")
    enroll_student("E1002", "ST3456", "C102", "2024-08-22 09:45:00")

    # Submit feedback
    submit_feedback("F1001", "ST2345", "C101", "2024-08-22 09:50:00", "Great course, very engaging!")
    submit_feedback("F1002", "ST3456", "C102", "2024-08-22 10:00:00", "The content was clear but could use more examples.")
