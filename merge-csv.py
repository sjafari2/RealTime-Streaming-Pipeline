import pandas as pd
import os
import glob

def merge_csv_files(input_folder, output_file):
    # Find all CSV files in the input folder
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {input_folder}")
        return

    df_list = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            df['source_file'] = os.path.basename(file)  # Add source filename column if needed
            df_list.append(df)
            print(f"[INFO] Loaded {file} with shape {df.shape}")
        except Exception as e:
            print(f"[ERROR] Failed to read {file}: {e}")

    # Concatenate all dataframes
    merged_df = pd.concat(df_list, ignore_index=True)

    # Save to a single CSV file
    merged_df.to_csv(output_file, index=False)
    print(f"[DONE] Merged {len(csv_files)} files into {output_file} with shape {merged_df.shape}")

if __name__ == "__main__":
    input_folder = "./results/2025-06-30_11-31-21/merge"         # replace with your input folder path
    output_file = "./results/merged_output.csv"   # replace with your desired output file path

    merge_csv_files(input_folder, output_file)

