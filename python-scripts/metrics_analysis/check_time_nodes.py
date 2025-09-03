import subprocess

NAMESPACE = "kafkastreamingdata"
pods = subprocess.run(
    ["kubectl", "get", "pods", "-n", NAMESPACE, "-o", "name"],
    capture_output=True, text=True
).stdout.strip().split('\n')

for pod in pods:
    pod_name = pod.split('/')[-1]
    cmd = ["kubectl", "exec", "-n", NAMESPACE, pod_name, "--", "python3", "-c", "import time; print(time.time())"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(f"{pod_name:30}: {result.stdout.strip()}")

