import os
from datetime import datetime
import pandas as pd
from pymongo import MongoClient


class MongoConnector:
    def __init__(self, db_log="log_db", db_answers="inf_answers"):
        """
        Initializes the MongoStatistics class.
        Establishes connections to MongoDB and loads initial data.
        """
        self.db_log = db_log
        self.db_answers = db_answers
        self.client_log = self.create_connection(self.db_log)
        self.client_answers = self.create_connection(self.db_answers)
        self.df = self.load_data()

    @staticmethod
    def create_connection(database_name):
        """
        Creates a connection to the MongoDB database.
        
        :param database_name: Name of the database to connect to.
        :return: MongoDB client object.
        """
 
        host = os.getenv("MONGO_HOST", "localhost")
        port = int(os.getenv("MONGO_PORT", 27017))
        username = os.getenv("MONGO_USERNAME", "")
        password = os.getenv("MONGO_PASSWORD", "")

        mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/{database_name}"

        try:
            client = MongoClient(mongo_uri)
            return client
        except Exception as e:
            print(f"MongoDB connection error: {e}")
            return None

    def load_data(self):
        """
        Loads and caches data from MongoDB.
        
        :return: Pandas DataFrame containing the processed log data.
        """
        return self.check_inf_answers_mongo()

    def get_student_names(self, aws_obj):
        """
        Retrieves student names from an S3 bucket.
        
        :return: List of student names.
        """
        file = aws_obj.get_from_s3("students/student_tasks.csv")
        return pd.read_csv(file["Body"], index_col="Student").index.to_list()

    def filter_data(self, user=None, task=None, true_value=None, sort_column=None, ascending=True):
        """
        Filters and sorts data based on provided parameters.
        
        :param user: Filter by student name (case insensitive).
        :param task: Filter by task number.
        :param true_value: Filter by boolean value (True/False).
        :param sort_column: Column to sort by.
        :param ascending: Sort order (True for ascending, False for descending).
        :return: Filtered Pandas DataFrame.
        """
        df_filtered = self.df.copy()

        if user:
            df_filtered = df_filtered[df_filtered["USER"].str.lower() == user.lower()]

        if task is not None:
            try:
                task = int(task)
                df_filtered = df_filtered[df_filtered["task"] == task]
            except ValueError:
                print("Error: Task must be a number")

        if true_value is not None:
            df_filtered = df_filtered[df_filtered["True?"] == true_value]

        if sort_column and sort_column in df_filtered.columns:
            df_filtered = df_filtered.sort_values(by=sort_column, ascending=ascending)

        return df_filtered

    def check_inf_answers_mongo(self):
        """
        Checks student answers in MongoDB and processes the log data.
        
        :return: Pandas DataFrame containing processed log data.
        """
        db_log = self.client_log[self.db_log]
        db_answers = self.client_answers[self.db_answers]

        cutoff_date = datetime(2024, 9, 1)

        answers_dict = {}
        for task_name in db_answers.list_collection_names():
            collection = db_answers.get_collection(task_name)
            answers_dict[task_name] = {doc["num"]: doc["ans"] for doc in collection.find({}, {"num": 1, "ans": 1})}

        results = []

        for coll in db_log.logs.aggregate([
            {
                "$addFields": {
                    "A": "$answer",
                    "D": {
                        "$dateAdd": {
                            "startDate": {
                                "$dateFromString": {
                                    "dateString": "$date",
                                    "format": "%Y-%m-%d %H-%M-%S"
                                }
                            },
                            "unit": "hour",
                            "amount": 3
                        }
                    },
                    "USER": "$username"
                }
            },
            {"$match": {"D": {"$gt": cutoff_date}}},
            {"$project": {"_id": 0, "task": 1, "num": 1, "A": 1, "D": 1, "USER": 1}}
        ]):
            real_ans = answers_dict.get(str(coll["task"]), {}).get(coll["num"])

            coll["True?"] = real_ans == coll["A"] if real_ans is not None else False
            results.append(coll)

        final_df = pd.DataFrame(results)

        final_df = final_df[~final_df["A"].str.contains(r"QWE", case=False, regex=True, na=False)]
        df_clean = final_df.reset_index(drop=True)

        return df_clean


if __name__ == "__main__":
    stats = MongoConnector()


    df = stats.df
    print(df.head())

    filtered_df = stats.filter_data(user="ruslan_24_25", task=5, true_value=True, sort_column="D", ascending=False)
    print(filtered_df)
