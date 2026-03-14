def has_expected_number_of_arguments(argv: list, expected: int) -> bool:
    """
    Checks if the number of arguments in the list matches the expected count.
    """
    return len(argv) == expected


def is_valid_client_count(num_clients_str: str) -> bool:
    """
    Checks if the given string represents a valid non-negative integer.
    """
    try:
        return int(num_clients_str) >= 0
    except ValueError:
        return False
