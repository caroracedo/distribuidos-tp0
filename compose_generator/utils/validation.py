def has_expected_number_of_arguments(argv: list, expected: int) -> bool:
    return len(argv) == expected


def is_valid_client_count(num_clients_str: str) -> bool:
    try:
        return int(num_clients_str) >= 0
    except ValueError:
        return False
