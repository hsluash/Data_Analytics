from faker import Faker
import polars as pl
import random
import os
from typing import Dict, List, Union
from datetime import datetime

class DatasetGenerator:
    """
    A class to generate synthetic datasets containing user activities.

    Attributes:
        num_records (int): Number of records to generate.
        unique_user_count (int): Number of unique users in the dataset.
        dataset_path (str): Path to the directory where datasets will be stored.
    """

    INVALID_SESSION_THRESHOLD = 0.1  # Threshold for generating invalid sessions
    UNKNOWN_PLATFORM_THRESHOLD = 0.1  # Threshold for labeling platform as unknown
    MISSING_ACTIVITY_TIME_THRESHOLD = 0.1  # Threshold for missing activity time
    UNKNOWN_ACTIVITY_THRESHOLD = 0.1  # Threshold for unknown activity type

    def __init__(self, num_records: int, unique_user_count: int):
        self.dataset_path=os.path.dirname(os.path.abspath(__file__))
        self.num_records = num_records
        self.unique_user_count = unique_user_count
        self.faker = Faker()
    
    def generate_data(self) -> Dict[str, List[Union[str, datetime, None]]]:
        """
        Generate synthetic user activity data with specified probabilities of errors.

        Returns:
              Dict[str, List[Union[str, datetime, None]]]: A dictionary containing columns of the dataset.
        """
        data = {
            "user_id": [random.choice(range(1, self.unique_user_count + 1)) for _ in range(self.num_records)],
            "session_id": [self.faker.uuid4() if random.random() > self.INVALID_SESSION_THRESHOLD else "invalid_session" for _ in range(self.num_records)],  # 10% invalid session ID
            "platform": [random.choice(['iOS', 'Android', 'Web', None]) if random.random() > self.UNKNOWN_PLATFORM_THRESHOLD else "Unknown" for _ in range(self.num_records)],  # 10% Unknown
            "activity_time": [self.faker.date_time_this_year() if random.random() > self.MISSING_ACTIVITY_TIME_THRESHOLD else None for _ in range(self.num_records)],  # 10% None
            "activity_type": [random.choice(['login', 'logout', 'view', 'click', 'purchase', None]) if random.random() > self.UNKNOWN_ACTIVITY_THRESHOLD else "unknown_activity" for _ in range(self.num_records)]  # 10% unknown activity
        }
        return data

    def create_dataframe(self) -> pl.LazyFrame:
        """
        Convert generated data into a Polars LazyFrame.

        Returns:
              pl.LazyFrame: A LazyFrame containing the generated data.
        """
        fake_data_with_errors = self.generate_data()
        lf = pl.LazyFrame(fake_data_with_errors)
        return lf

    def generate_dataset_csv(self):
        """
        Generate a CSV file from the synthetic data and save it to the specified path.
        """
        output_path=os.path.join(self.dataset_path, "datasets")
        print(output_path)
        if not os.path.exists(output_path):
           os.makedirs(output_path)
        dt_string = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
        filename=os.path.join(output_path, f"Dataset_{dt_string}.csv")
        df = self.create_dataframe()
        df.collect().write_csv(filename) # Inform polars to execute the lazy API
        

