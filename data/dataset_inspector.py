"""
Module for inspecting datasets, designed to clean and identify the most active users
on each platform within the dataset.
"""
import asyncio
import nest_asyncio
import polars as pl
import os 
from pathlib import Path 
from user_data_analytics.data.data_supplier import DataSupplier
from datetime import datetime

class DatasetInspector:
    """
    clean and inspect the dataset to find most active user
    on each platform defined in the dataset.
    Attributes:
        top_N (int): Number of top users to retrieve.
        file_path (str): Path to the dataset files.
        mode (str): Mode of dataset ('csv', 'parquet', or 'streaming').
    """
    def __init__(self, top_N:int, file_path:str, mode:str):
        self.top_N=top_N
        self.file_path=file_path  
        self.mode=mode
        self.supplier = DataSupplier()
    def scan_csv_dataset(self) -> pl.LazyFrame:
        """
        Scan CSV datasets, applying filters and returning a cleaned LazyFrame.
        """
        lazy_df=pl.scan_csv(self.file_path).drop(["activity_time","activity_type"])
        if lazy_df is None:
            raise ValueError("CSV not loaded")
        else:
            cleaned_lazy_df=self.filter_dataset(lazy_df)
            return cleaned_lazy_df                
    
    def scan_parquet_dataset(self,inspect_column:str) -> pl.LazyFrame:
        """
        Scan and load parquet datasets based on the platform partition.
        """
        partition_folders=[f.path for f in os.scandir(self.file_path) if f.name.startswith("platform=")]
        results=[]
        for partition_folder in partition_folders:
            platform_name=partition_folder.split("platform=")[-1]
            parquet_glob_pattern = os.path.join(partition_folder, "**/*.parquet")
            lazy_df = pl.scan_parquet(parquet_glob_pattern).drop(["activity_time", "activity_type"])
            if not inspect_column or platform_name in inspect_column:
                result = self.process_parquet_dataset_chunk(lazy_df)
                result = result.with_columns(pl.lit(platform_name).alias("platform"))
                results.append(result)
                #print(f"results before polars: {type(results)}")
        results_df=pl.concat(results)
        #print(f"results after polars: {type(results_df)}")
        dt_string=datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
        results_output_file = Path(__file__).resolve().parent.parent / f"Result_{dt_string}.csv"
        #print(results_output_file)
        results_df.collect().write_csv(results_output_file)
        return results_df
    
    def filter_dataset(self,lf:pl.LazyFrame) -> pl.LazyFrame:
        """"
         filter row if user_id is empty or contains unsupported characters
         filter row if session_id is empty or contains str  "invalid_session"
         filter row if platform is empty
        """
        lf_cleaned = lf.filter((pl.col("user_id").is_not_null()) & (pl.col("session_id").is_not_null()) & (pl.col("platform").is_not_null()) &
                               (pl.col("session_id") != "invalid_session") &                       
                               (pl.col("platform")!="Unknown"))
        
        return lf_cleaned
    
    def process_dataset_chunk(self,df:pl.LazyFrame) -> pl.LazyFrame:
        """
        Process data chunks to find the top N most active users per platform.
        """
        df_grouped = (df.group_by(["platform", "user_id"]).agg([
                        pl.col("session_id").n_unique().alias("unique_session_count")
                    ])
                )
        df_top_users = (df_grouped.sort(["platform", "unique_session_count"], descending=True).group_by("platform").head(self.top_N))
        final_df = df_top_users.collect()
        dt_string=datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
        results_output_file = Path(__file__).resolve().parent.parent / f"Result_{dt_string}.csv"
        #print(results_output_file)
        final_df.write_csv(results_output_file)
        return df_top_users
    
    def process_parquet_dataset_chunk(self,df) -> pl.LazyFrame:
        unique_sessions_by_user = (df.group_by([ "user_id"]).agg([pl.col("session_id").n_unique().alias("unique_sessions_count")]))
        df_top_users=(unique_sessions_by_user.sort("unique_sessions_count",descending=True).head(self.top_N))
        return  df_top_users
    
    async def consumer(self):
        """Asynchronously consume data from the queue."""
        while True:
            data = await self.queue.get()
            await asyncio.sleep(2)
            self.queue.task_done()
                

    async def send_data(self, data):
        """Asynchronously produce data and handle full queue."""
        if self.queue.full():
            await self.process_data(data)  # Send directly to consumer.
        else:
            await self.queue.put(data)
            

    async def process_data(self, data):
        self.process_dataset_chunk(data)
        
    
    async def start_streaming(self):
        while True:  # Change this condition as needed.
            data = self.supplier.generate_real_feeds() # Generate random data.
            await self.send_data(data)
            await asyncio.sleep(1)  # Wait for a second before sending next data.
        await self.queue.join() 
    
    def data_inspector(self,inspect_column=None):
        """
        Inspect data based on the specified mode and optionally filter by column.
        """
        if self.mode=="csv":
            lazy_csv_df=self.scan_csv_dataset()
            self.process_dataset_chunk(lazy_csv_df)
            
        elif self.mode=="parquet":
            top_N_users_df =self.scan_parquet_dataset(inspect_column)
        
        elif self.mode=="streaming":
            nest_asyncio.apply()
            self.queue = asyncio.Queue(maxsize=1000)
            self.consumer_task = asyncio.create_task(self.consumer())
            asyncio.run(self.start_streaming()) 
            
            
        
        
