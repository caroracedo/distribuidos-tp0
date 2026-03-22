import argparse


def parse_args():
    """
    Parses command-line arguments for the Docker Compose generator.
    """
    parser = argparse.ArgumentParser(description="Generate a Docker Compose file.")

    parser.add_argument("output_file", help="Path to output file")
    parser.add_argument(
        "num_clients",
        type=int,
        help="Number of clients (must be positive)",
    )

    args = parser.parse_args()

    if args.num_clients < 0:
        parser.error("num_clients must be a positive integer.")

    return args
