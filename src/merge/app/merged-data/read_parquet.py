import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--parquet_path', required=True, default="./")
args = parser.parse_args()

df = pd.read_parquet(args.parquet_path)
print(df.head())
