import pytest

import fryer


@pytest.fixture
def logger(key, temp_dir):
    yield fryer.logger.get(key=key, path_log=temp_dir)


@pytest.mark.parametrize(
    "key",
    [
        "key",
        "test/longer/key",
    ],
)
def test_get_logging_exists(key, temp_dir, logger):
    assert logger is not None
    path_log_file = temp_dir.joinpath(f"{key}/log.log")
    assert path_log_file.exists()


@pytest.mark.parametrize(
    "key",
    [
        "key",
        "test/longer/key",
    ],
)
def test_get_logging_exists_with_path_log_str(key, temp_dir):
    fryer.logger.get(key=key, path_log=str(temp_dir))
    path_log_file = temp_dir.joinpath(f"{key}/log.log")
    assert path_log_file.exists()


@pytest.mark.parametrize(
    "key, messages_with_level",
    [
        ("key", {"test message 1": "info"}),
        ("key", {"test message 1": "info", "test message 2": "debug"}),
        (
            "key",
            {
                "test message 1": "info",
                "test message 2": "debug",
                "test message 3": "warning",
            },
        ),
    ],
)
def test_get_logging_messages(key, messages_with_level, temp_dir, logger):
    path_log_file = temp_dir.joinpath(f"{key}/log.log")

    messages_logged = []
    for message, level in messages_with_level.items():
        getattr(logger, level)(message)
        messages_logged.append(message)

        # Check all the messages logged are in the log file
        log_contents = path_log_file.read_text()
        for message_logged in messages_logged:
            assert message_logged in log_contents


@pytest.mark.parametrize(
    "key",
    [
        "key",
        "test/longer/key",
    ],
)
def test_get_logging_multiple(key, temp_dir, logger):
    path_log_file = temp_dir.joinpath(f"{key}/log.log")

    message = "message 1"
    logger.info(message)
    assert message in path_log_file.read_text()

    another_logger = fryer.logger.get(
        key=key,
        path_log=temp_dir,
    )

    another_message = "message 2"
    another_logger.info(another_message)
    log_contents = path_log_file.read_text()
    assert message in log_contents and another_message in log_contents
