import subprocess


def run_docker_compose():
    """
    1. Primer paso
    Función encargada de ejecutar comando para ejecutar Docker Compose
    """
    command = ["docker-compose", "up", "-d"]

    # Ejecutar el comando
    subprocess.run(command, check=True)


if __name__ == "__main__":
    run_docker_compose()
