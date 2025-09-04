"""
Main Class [datainspector]
"""

from user_data_analytics.data.dataset_generator import DatasetGenerator
from user_data_analytics.data.dataset_converter import DatasetConverter
from user_data_analytics.data.dataset_inspector import DatasetInspector
import argparse

def setup_parser():
    """
    Setup the command line argument parser.
    """
    parser = argparse.ArgumentParser(
        description="This script supports generating datasets, converting them to Parquet format, and inspecting datasets.\n"
                    "Choose an operation mode: generator, converter, or inspector.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--op", type=str, choices=['generator', 'converter', 'inspector'], required=True,
                        help="Operation mode:\n"
                             "  generator - Generate new dataset.\n"
                             "  converter - Convert dataset to Parquet.\n"
                             "  inspector - Inspect dataset for top active users.")
    parser.add_argument("--n", type=int, required=False, default=3, help="Number of top users to retrieve.")
    parser.add_argument("--filepath", type=str, required=False, default="output.csv", help="File path to the dataset.")
    parser.add_argument("--mode", type=str, choices=['csv', 'parquet', 'streaming'], required=False, default="csv", help="Dataset processing mode.")
    parser.add_argument("--platform", type=str, choices=['Android', 'iOS', 'Web'], help="Platform to filter by.")
    return parser


def main(args):
    """
    Starts DatasetGenerator.
    """
    if args.op == "generator":
        generator = DatasetGenerator(num_records=1000000, unique_user_count=100)
        generator.generate_dataset_csv()
    elif args.op in {"converter","inspector"}:
        if not args.filepath or not args.mode or not args.n:
            parser.error("--filepath, --n and --mode are required for converter/inspector")
        if (args.op == "converter") & (args.mode == "csv"):
            converter = DatasetConverter(file_path=args.filepath,block_size="200MB",
                                    partition_column="platform",
                                    parquet_output_dir="dataset_parquets",
                                    cast_column=True,filtering_required=True)
            converter.convert_csv_to_parquet()
        elif args.op == "inspector":
            inspector = DatasetInspector(top_N=args.n, file_path=args.filepath, mode=args.mode)
            if args.platform:
                inspector.data_inspector(inspect_column=[args.platform])
            else:
                inspector.data_inspector()
    else:
        raise ValueError("Invalid operation specified. Use 'generator', 'converter', or 'inspector'.")


if __name__ == '__main__':
    parser = setup_parser()
    args = parser.parse_args()
    main(args)
