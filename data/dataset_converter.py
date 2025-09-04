""""
Module for converting the CSV data format to parquet data format for efficient data storage.
"""
from datetime import datetime

import dask.dataframe as dd
import os

class DatasetConverter:
    """
    Converts CSV files to Parquet format with options for filtering and casting data types.
    """
    def __init__(
        self, file_path:str, block_size:str,
        partition_column:str, parquet_output_dir:str,
        cast_column:bool = False, filtering_required:bool = False
    ):
        self.csv_file=file_path
        self.block_size=block_size
        self.dtypes = {"platform":"category","user_id":str,"session_id":str}
        self.partition_column = partition_column
        self.parquet_output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), parquet_output_dir)
        self.columns_to_cast=["platform","user_id","session_id"]
        self.cast_column=cast_column
        self.filtering_required=filtering_required
      
    def convert_csv_to_parquet(self):
        """
       Converts the CSV file to a Parquet file, applying data type conversions and filtering as configured.
       """
        try:
            df = dd.read_csv(self.csv_file, blocksize=self.block_size, dtype=self.dtypes, assume_missing=True)
            df=df.dropna(subset=self.columns_to_cast)
            if self.cast_column:
                df[self.columns_to_cast]=df[self.columns_to_cast].astype(str)
                df["activity_time"]=dd.to_datetime(df["activity_time"])
            if self.filtering_required:
                df=df[df["platform"]!="Unknown"]
                df=df[df["session_id"]!="invalid_session"]
            if df[self.partition_column].dtype != 'category':
                df[self.partition_column] = df[self.partition_column].astype("category")
            dask_df = df.persist()
            timestamp = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
            output_file = os.path.join(self.parquet_output_dir, f"{os.path.basename(self.csv_file)}_{timestamp}.parquet")
            print(output_file)
            print(self.parquet_output_dir)
            os.makedirs(self.parquet_output_dir, exist_ok=True)
            dask_df.to_parquet(
                output_file, engine="pyarrow",
                write_index=False, partition_on=[self.partition_column],
                compression="snappy",  # Use Snappy compression for a balance of speed and storage efficiency
                write_metadata_file=True, schema="infer")
        except Exception as e:
            # Handle or log the exception as needed
            print(f"An error occurred: {str(e)}")
