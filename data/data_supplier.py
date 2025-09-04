"""
Real time Data feeds supplied.
"""


import random
from faker import Faker
import polars as pl

class DataSupplier:
    """
    Simulates real time data feeds
    """
    INVALID_SESSION_THRESHOLD = 0.1
    UNKNOWN_PLATFORM_THRESHOLD = 0.1
    MISSING_ACTIVITY_TIME_THRESHOLD = 0.1
    UNKNOWN_ACTIVITY_THRESHOLD = 0.1
    
    def __init__(self):
      
       
        self.faker = Faker()
           
    def generate_real_feeds(self):
        
        new_data = {
            
            "user_id": random.choice(range(1, 1000)),
            "session_id": self.faker.uuid4() if random.random() > self.INVALID_SESSION_THRESHOLD else "invalid_session",
            "platform": random.choice(['iOS', 'Android', 'Web',
                                       None]) if random.random() > self.UNKNOWN_PLATFORM_THRESHOLD else "Unknown",
            "activity_time": self.faker.date_time_this_year() if random.random() > self.MISSING_ACTIVITY_TIME_THRESHOLD else None,
            "activity_type": random.choice(['login', 'logout', 'view', 'click', 'purchase',
                                            None]) if random.random() > self.UNKNOWN_ACTIVITY_THRESHOLD else "unknown_activity"
        }
        
        
        lf = pl.LazyFrame(new_data)
         
        return lf
    
    


