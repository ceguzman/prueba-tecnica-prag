import subprocess


def run_docker_compose():
    # Comando para ejecutar Docker Compose
    command = ["docker-compose", "up", "-d"]

    # Ejecutar el comando
    subprocess.run(command, check=True)


if __name__ == "__main__":
    run_docker_compose()
