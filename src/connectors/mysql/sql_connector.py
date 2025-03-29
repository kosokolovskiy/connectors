import mysql.connector

class SQLConnector:
    def __init__(self, dbname="tasks_done", username='main_user', password=None, rds_endpoint=None):
        self.username = username
        self.password = password
        self.rds_endpoint = rds_endpoint
        self.dbname = dbname
        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connection = mysql.connector.connect(
            host=self.rds_endpoint,
            user=self.username,
            password=self.password,
            port=3306,
            connect_timeout=5,
            database=self.dbname
        )
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def execute_commit(self, query):
        self.cursor.execute(query)
        self.connection.commit()

    def insert_record(self, student, subject, variant, nums):
        for num in nums:
            query = f"""
                INSERT INTO Main (StudentID, Subject, variant, num)
                VALUES ({student}, {subject}, {variant}, {num})
            """
            print(query)
            self.execute_commit(query)

    def check_record_exists(self, student, subject, variant, nums):
        for num in nums:
            query = f"""
                SELECT * FROM Main
                WHERE studentid = {student}
                AND subject = {subject}
                AND variant = {variant}
                AND num = {num}
            """
            result = self.execute_query(query)
            if result:
                print(f"Num {num} in Variant {variant} for Student {student} is already done")
                return False
        return True
