from .logger import logger
import sys


def read_file_content(file_path: str) -> str:
    """
    Reads the content of a file and returns it as a string.
    """
    try:
        with open(file_path, "r") as file:
            return file.read()
    except FileNotFoundError:
        logger.error(f"File {file_path} not found.")
        sys.exit(1)
    except IOError as e:
        logger.error(f"Error reading {file_path}: {e}")
        sys.exit(1)


def write_file_content(file_path: str, content: str) -> None:
    """
    Writes the given content to a file.
    """
    try:
        with open(file_path, "w") as file:
            file.write(content)
    except IOError as e:
        logger.error(f"Error writing to {file_path}: {e}")
        sys.exit(1)
