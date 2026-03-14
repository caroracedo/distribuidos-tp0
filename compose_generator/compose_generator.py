import sys
import os
from .utils.logger import logger
from .utils.file_utils import read_file_content, write_file_content
from .utils.validation import has_expected_number_of_arguments, is_valid_client_count

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
BASE_PATH = os.path.join(TEMPLATE_DIR, "base.yaml")
NETWORKS_PATH = os.path.join(TEMPLATE_DIR, "networks.yaml")
CLIENT_PATH = os.path.join(TEMPLATE_DIR, "client.yaml")
EXPECTED_ARGS = 3


def generate_compose(output_file: str, num_clients: int) -> None:
    base_config = read_file_content(BASE_PATH)
    networks_config = read_file_content(NETWORKS_PATH)
    client_template = read_file_content(CLIENT_PATH)

    clients_config = "".join(
        client_template.format(cli_id=i) for i in range(1, num_clients + 1)
    )

    write_file_content(
        output_file, f"{base_config}\n{clients_config}\n{networks_config}"
    )

    logger.info(f"Generated {output_file} with {num_clients} clients.")


def main():
    if not has_expected_number_of_arguments(sys.argv, EXPECTED_ARGS):
        logger.error(f"Usage: python3 {sys.argv[0]} <output_file> <num_clients>")
        sys.exit(1)

    if not is_valid_client_count(sys.argv[2]):
        logger.error("num_clients must be a positive integer.")
        sys.exit(1)

    generate_compose(sys.argv[1], int(sys.argv[2]))
