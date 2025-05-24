import docker
from datetime import timedelta

client = docker.from_env()

def exec_cmd_in_container(container_name, command):
    container = client.containers.get(container_name)
    exit_code, output = container.exec_run(command)
    print(f"[{container_name}] Output: {output.decode()} (exit={exit_code})")
    if exit_code != 0:
        raise Exception(f"Command failed in {container_name}")

exec_cmd_in_container("cf-spark-flatten", "bash /opt/spark/work-dir/submit_flatten.sh")