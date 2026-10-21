import os
import pwd
from typing import TYPE_CHECKING

import pytest

from fryer.core.settings import (
    SPACE_MARKER,
    SettingsError,
    current_user,
    load_settings,
    space_of,
)
from tests.conftest import refuse_real_spaces

if TYPE_CHECKING:
    from pathlib import Path


def _space(*, root: Path, name: str) -> Path:
    root.mkdir(parents=True)
    (root / SPACE_MARKER).write_text(f"{name}\n", encoding="utf-8")
    return root


def _environ(
    *, env: str, write: Path | None = None, read: tuple[Path, ...] = ()
) -> dict[str, str]:
    environ = {"FRYER_ENV": env, "FRYER_PATH_READ": os.pathsep.join(map(str, read))}
    if write is not None:
        environ["FRYER_PATH_WRITE"] = str(write)
    return environ


def test_lookup_is_write_then_read_in_order(tmp_path: Path) -> None:
    personal, shared, prod = (
        tmp_path / "personal",
        tmp_path / "shared",
        tmp_path / "prod",
    )
    settings = load_settings(
        environ=_environ(env="dev", write=personal, read=(shared, prod)), user="someone"
    )

    assert settings.lookup == (personal, shared, prod)


def test_lookup_searches_each_place_once(tmp_path: Path) -> None:
    prod = _space(root=tmp_path / "prod", name="prod")
    settings = load_settings(
        environ=_environ(env="prod", write=prod, read=(prod,)), user="fryer-prod"
    )

    assert settings.lookup == (prod,)


def test_reader_has_no_write_location(tmp_path: Path) -> None:
    prod = _space(root=tmp_path / "prod", name="prod")
    settings = load_settings(
        environ=_environ(env="prod", read=(prod,)), user="fryer-console"
    )

    assert settings.path_write is None
    assert settings.lookup == (prod,)


@pytest.mark.parametrize(
    ("space", "env", "user"),
    [("prod", "prod", "fryer-prod"), ("test", "test", "fryer-test")],
)
def test_service_account_writes_its_space(
    tmp_path: Path, space: str, env: str, user: str
) -> None:
    root = _space(root=tmp_path / space, name=space)

    assert (
        load_settings(environ=_environ(env=env, write=root), user=user).path_write
        == root
    )


@pytest.mark.parametrize(
    ("space", "env", "user"),
    [
        pytest.param("prod", "dev", "someone", id="a person"),
        pytest.param("prod", "prod", "someone", id="a person claiming FRYER_ENV=prod"),
        pytest.param(
            "prod", "dev", "fryer-prod", id="the service account with the wrong env"
        ),
        pytest.param("prod", "test", "fryer-test", id="the test account"),
        pytest.param("test", "dev", "someone", id="a person writing to test"),
        pytest.param(
            "test", "prod", "fryer-prod", id="the prod account writing to test"
        ),
    ],
)
def test_guard_refuses_other_writers(
    tmp_path: Path, space: str, env: str, user: str
) -> None:
    root = _space(root=tmp_path / space, name=space)

    with pytest.raises(SettingsError) as raised:
        load_settings(environ=_environ(env=env, write=root), user=user)

    assert f"refusing to write to the {space} space" in str(raised.value)


def test_guard_covers_a_folder_inside_prod(tmp_path: Path) -> None:
    inside = _space(root=tmp_path / "prod", name="prod") / "open" / "raw"

    with pytest.raises(SettingsError) as raised:
        load_settings(environ=_environ(env="dev", write=inside), user="someone")

    assert "refusing to write to the prod space" in str(raised.value)


@pytest.mark.parametrize("space", ["shared", "personal"])
def test_anyone_writes_shared_and_personal(tmp_path: Path, space: str) -> None:
    root = _space(root=tmp_path / space, name=space)

    assert (
        load_settings(
            environ=_environ(env="dev", write=root), user="someone"
        ).path_write
        == root
    )


def test_unreadable_marker_is_refused(tmp_path: Path) -> None:
    if os.geteuid() == 0:
        pytest.skip("root reads any file, so the marker cannot be made unreadable")
    root = _space(root=tmp_path / "prod", name="prod")
    (root / SPACE_MARKER).chmod(0)

    with pytest.raises(SettingsError) as raised:
        load_settings(environ=_environ(env="dev", write=root), user="someone")

    assert "cannot tell which space" in str(raised.value)


def test_no_marker_is_no_space(tmp_path: Path) -> None:
    assert space_of(path=tmp_path / "anywhere") is None


@pytest.mark.parametrize(
    ("environ", "expected"),
    [
        pytest.param(
            {}, "FRYER_ENV must be one of prod, test, dev, not None", id="no env"
        ),
        pytest.param({"FRYER_ENV": "production"}, "not 'production'", id="unknown env"),
        pytest.param(
            {"FRYER_ENV": "dev", "FRYER_PATH_WRITE": "data/mine"},
            "FRYER_PATH_WRITE must hold absolute paths",
            id="relative write",
        ),
        pytest.param(
            {"FRYER_ENV": "dev", "FRYER_PATH_READ": f"/data/prod{os.pathsep}shared"},
            "FRYER_PATH_READ must hold absolute paths",
            id="relative read",
        ),
    ],
)
def test_bad_settings_fail_clearly(environ: dict[str, str], expected: str) -> None:
    with pytest.raises(SettingsError) as raised:
        load_settings(environ=environ, user="someone")

    assert expected in str(raised.value)


def test_current_user_comes_from_the_user_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("USER", "fryer-prod")

    assert current_user() == pwd.getpwuid(os.geteuid()).pw_name


@pytest.mark.parametrize("variable", ["FRYER_PATH_WRITE", "FRYER_PATH_READ"])
@pytest.mark.parametrize("space", ["prod", "test"])
def test_tests_refuse_real_spaces(tmp_path: Path, variable: str, space: str) -> None:
    root = _space(root=tmp_path / space, name=space)

    with pytest.raises(pytest.UsageError) as raised:
        refuse_real_spaces(environ={variable: str(root)})

    assert f"in the {space} space" in str(raised.value)


def test_tests_run_beside_other_spaces(tmp_path: Path) -> None:
    shared = _space(root=tmp_path / "shared", name="shared")

    refuse_real_spaces(
        environ={"FRYER_PATH_WRITE": str(shared), "FRYER_PATH_READ": str(tmp_path)}
    )
