import os

def delete_empty_folders(path):
    """
    Recursively deletes all empty subfolders in the given path.
    """
    for root, dirs, files in os.walk(path, topdown=False):
        for folder in dirs:
            folder_path = os.path.join(root, folder)
            if not os.listdir(folder_path):  # Folder is empty
                os.rmdir(folder_path)
                print(f"Deleted empty folder: {folder_path}")

def ask_and_delete(base_path, subfolder_name):
    """
    Asks the user whether to delete empty folders inside a specific subfolder.
    """
    target_path = os.path.join(base_path, subfolder_name)
    if not os.path.exists(target_path):
        print(f"'{subfolder_name}' folder not found at: {target_path}")
        return
    
    response = input(f"Do you want to delete empty folders inside '{subfolder_name}'? (y/n): ").strip().lower()
    if response == 'y':
        delete_empty_folders(target_path)
    else:
        print(f"Skipping deletion inside '{subfolder_name}'.")

# -------- Main script starts here --------

if __name__ == "__main__":
    base_path = input("Enter the base directory path: ").strip()

    ask_and_delete(base_path, "consumer-result")
    ask_and_delete(base_path, "logs")
