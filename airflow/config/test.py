import docker
from datetime import timedelta

client = docker.from_env()

def exec_cmd_in_container(container_name, command):
        container = client.containers.get(container_name)
        exec_result = container.exec_run(command, stream=True)
        
        for line in exec_result.output:
            print(f"[{container_name}] {line.decode().strip()}")


exec_cmd_in_container("cf-spark-flatten", "bash /opt/spark/work-dir/submit_flatten.sh")