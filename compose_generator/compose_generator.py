import sys
import os
from .utils.logger import logger
from .utils.parser import parse_args

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
BASE_PATH = os.path.join(TEMPLATE_DIR, "base.yaml")
NETWORKS_PATH = os.path.join(TEMPLATE_DIR, "networks.yaml")
CLIENT_PATH = os.path.join(TEMPLATE_DIR, "client.yaml")
EXPECTED_ARGS = 3


def generate_compose(output_file: str, num_clients: int) -> None:
    """
    Generates a Docker Compose file with the specified number of client services.
    """
    with open(BASE_PATH, "r") as base_file, open(
        NETWORKS_PATH, "r"
    ) as networks_file, open(CLIENT_PATH, "r") as client_file:
        base_config = base_file.read()
        networks_config = networks_file.read()
        client_template = client_file.read()

    clients_config = "".join(
        client_template.format(cli_id=i) for i in range(1, num_clients + 1)
    )

    with open(output_file, "w") as file:
        file.write(f"{base_config}\n{clients_config}\n{networks_config}")

    logger.info(f"Generated {output_file} with {num_clients} clients.")


def main():
    """
    Main function to validate command-line arguments and generate the Docker Compose file.
    """
    args = parse_args()

    try:
        generate_compose(args.output_file, args.num_clients)
    except Exception as e:
        logger.error(f"An error occurred while generating the Docker Compose file: {e}")
