from pathlib import Path
import pytest

from fryer.constants import FRYER_ENV_PATH_LOG
import fryer.logger


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
def test_get_exists(key, temp_dir, logger):
    assert logger is not None
    path_log_file = temp_dir / key / "log.log"
    assert path_log_file.exists()


@pytest.mark.parametrize(
    "key",
    [
        "key",
        "test/longer/key",
    ],
)
def test_get_exists_with_path_log_str(key, temp_dir):
    fryer.logger.get(key=key, path_log=temp_dir)
    path_log_file = temp_dir / key / "log.log"
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
def test_get_messages(key, messages_with_level, temp_dir, logger):
    path_log_file = temp_dir / key / "log.log"

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
def test_get_multiple(key, temp_dir, logger):
    path_log_file = temp_dir / key / "log.log"

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


def test_get_via_path_env(test_env, path_test_env):
    key = "test_key"
    fryer.logger.get(key=key, path_env=path_test_env)
    path_log_file = Path(test_env[FRYER_ENV_PATH_LOG]) / key / "log.log"
    assert path_log_file.exists()
