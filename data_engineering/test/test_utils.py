from data_engineering.utils import hash_id


def test_hash_id():
    """
        a este tambien XD
    """
    assert hash_id("test_string") == "fbb2c0178edee895eb0a988974462c2e", "Hashed output does not match expected value"