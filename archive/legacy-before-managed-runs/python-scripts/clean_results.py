import os
import shutil

base_path = "./results"
proceed_path = "./results/empty"

# Create the proceed folder if it doesn't exist
os.makedirs(proceed_path, exist_ok=True)

# Loop through all timestamp-named subdirectories in the base path
for dir_name in os.listdir(base_path):
    full_dir_path = os.path.join(base_path, dir_name)

    if not os.path.isdir(full_dir_path):
        continue  # Skip non-directory files

    subdirs = ["producer", "consumer", "merge"]
    all_exist_and_empty = True

    for sub in subdirs:
        sub_path = os.path.join(full_dir_path, sub)
        if os.path.exists(sub_path):
            if os.listdir(sub_path):  # Subdir is not empty
                all_exist_and_empty = False
                break
        else:
            # Missing subdir counts as not empty
            all_exist_and_empty = False
            break

    if all_exist_and_empty:
        target_path = os.path.join(proceed_path, dir_name)
        print(f"[INFO] Moving empty folder to ./results/empty/: {full_dir_path}")
        shutil.move(full_dir_path, target_path)
    else:
        print(f"[INFO] Keeping: {full_dir_path}")

