import os
import subprocess
from datetime import datetime
import shutil

# === Pod/container/src_path → destination subfolder name config ===
result_configs = {
    "producer":    "producer-sts-0:producer-container:/app/request-producer-data/producer-result:producer",
    "consumer":    "consumer-application-sts-0:consumer-container:/app/consumer-app-data/consumer-result:consumer",
    "application": "consumer-application-sts-0:application-container:/app/app-merge-data/application-result:application",
    "merge":       "merge-sts-0:merge-container:/app/merged-data/merge-result:merge"
}
ordered_keys = ["producer", "consumer", "application", "merge"]

def copy_from_pods(base_dir, fetch_logs=False):
    print("[INFO] Starting to fetch from pods...")

    for key in ordered_keys:
        pod, container, src_path, dst_subdir = result_configs[key].split(":")
        
        if fetch_logs:
            # Explicit correct logs paths
            if key == "producer":
                src_path = "/app/request-producer-data/logs"
            elif key == "consumer":
                src_path = "/app/consumer-app-data/logs"
            elif key == "application":
                src_path = "/app/app-merge-data/logs"
            elif key == "merge":
                src_path = "/app/merged-data/logs"
            dst_subdir = dst_subdir + "_logs"

        dst_path = os.path.join(base_dir, dst_subdir)
        os.makedirs(dst_path, exist_ok=True)

        print(f"[INFO] Attempting to copy from {pod}:{src_path} (container: {container}) to {dst_path}")

        # Check if path exists inside pod before copying
        check_cmd = [
            "kubectl", "exec", pod, "-c", container, "--",
            "test", "-e", src_path
        ]
        result = subprocess.run(check_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if result.returncode != 0:
            print(f"[WARNING] ❌ Path {src_path} does not exist in {pod} ({container}). Skipping to next.")
            continue

        try:
            subprocess.run(
                ["kubectl", "cp", f"{pod}:{src_path}", dst_path, "-c", container],
                check=True,
                stderr=subprocess.STDOUT
            )
            print(f"[INFO] ✅ Completed copying {dst_subdir}")
        except subprocess.CalledProcessError:
            print(f"[WARNING] ❌ Failed to copy from {pod}:{src_path}. Skipping to next.")

def delete_on_pods(delete_logs=False):
    print("[INFO] Starting deletion on pods...")

    for key in ordered_keys:
        pod, container, src_path, _ = result_configs[key].split(":")
        
        if delete_logs:
            # Explicit correct logs paths
            if key == "producer":
                src_path = "/app/request-producer-data/logs"
            elif key == "consumer":
                src_path = "/app/consumer-app-data/logs"
            elif key == "application":
                src_path = "/app/app-merge-data/logs"
            elif key == "merge":
                src_path = "/app/merged-data/logs"

        print(f"[INFO] Attempting to delete {src_path} in {pod} ({container})")

        # Check if path exists before deletion
        check_cmd = [
            "kubectl", "exec", pod, "-c", container, "--",
            "test", "-e", src_path
        ]
        result = subprocess.run(check_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if result.returncode != 0:
            print(f"[WARNING] ❌ Path {src_path} does not exist in {pod} ({container}). Skipping to next.")
            continue

        cmd = ["kubectl", "exec", pod, "-c", container, "--", "rm", "-rf", src_path]
        try:
            subprocess.run(cmd, check=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
            print(f"[INFO] ✅ Deleted {src_path} in {pod} ({container})")
        except subprocess.CalledProcessError:
            print(f"[WARNING] ❌ Could not delete {src_path} in {pod} ({container}). Skipping to next.")

if __name__ == "__main__":
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_dir = f"./results/{timestamp}"
    os.makedirs(base_dir, exist_ok=True)

    # === Ordered block: RESULTS ===
    if input("Fetch results from pods? [y/N]: ").strip().lower() == "y":
        copy_from_pods(base_dir, fetch_logs=False)

    if input("Delete results from pods after fetching? [y/N]: ").strip().lower() == "y":
        delete_on_pods(delete_logs=False)

    # === Ordered block: LOGS ===
    if input("Fetch logs from pods? [y/N]: ").strip().lower() == "y":
        copy_from_pods(base_dir, fetch_logs=True)

    if input("Delete logs from pods after fetching? [y/N]: ").strip().lower() == "y":
        delete_on_pods(delete_logs=True)

    # === Optional local cleanup ===
    if input(f"Do you want to delete the local results folder '{base_dir}'? [y/N]: ").strip().lower() == "y":
        shutil.rmtree(base_dir)
        print(f"[INFO] ✅ Deleted local folder: {base_dir}")

    print("[DONE] ✅ All fetch/delete operations completed.")

