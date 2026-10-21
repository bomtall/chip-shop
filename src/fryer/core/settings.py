import os
import pwd
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from collections.abc import Mapping

Env = Literal["prod", "test", "dev"]
ENVS: tuple[Env, ...] = ("prod", "test", "dev")

# The root of each space holds this file, naming the space; machine setup writes it
SPACE_MARKER = ".fryer-space"
# Only the matching service account, with the matching FRYER_ENV, may write here
GUARDED_SPACES: tuple[Env, ...] = ("prod", "test")


class Settings(BaseModel):
    model_config = ConfigDict(frozen=True)

    env: Env
    path_write: Path | None
    path_read: tuple[Path, ...]

    @property
    def lookup(self) -> tuple[Path, ...]:
        # The write location shadows every read location; each place is searched once
        first = () if self.path_write is None else (self.path_write,)
        return tuple(dict.fromkeys((*first, *self.path_read)))


class SettingsError(Exception):
    pass


def current_user() -> str:
    # From the process's user id, not $USER, which anyone can set
    return pwd.getpwuid(os.geteuid()).pw_name


def load_settings(*, environ: Mapping[str, str], user: str) -> Settings:
    env = environ.get("FRYER_ENV")
    if env not in ENVS:
        msg = f"FRYER_ENV must be one of {', '.join(ENVS)}, not {env!r}"
        raise SettingsError(msg)
    write = environ.get("FRYER_PATH_WRITE")
    path_write = _absolute(name="FRYER_PATH_WRITE", value=write) if write else None
    path_read = tuple(
        _absolute(name="FRYER_PATH_READ", value=value)
        for value in environ.get("FRYER_PATH_READ", "").split(os.pathsep)
        if value
    )
    if path_write is not None:
        _guard_write(path=path_write, env=env, user=user)
    return Settings(env=env, path_write=path_write, path_read=path_read)


def space_of(*, path: Path) -> str | None:
    for place in (path, *path.parents):
        try:
            return (place / SPACE_MARKER).read_text(encoding="utf-8").strip()
        except FileNotFoundError, NotADirectoryError:
            continue
        except OSError as error:
            # A marker that cannot be read could be prod's, so it is not skipped
            msg = f"cannot tell which space {path} is in: {error}"
            raise SettingsError(msg) from error
    return None


def _absolute(*, name: str, value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        msg = (
            f"{name} must hold absolute paths, so that it does not depend on"
            f" where fryer runs: {value!r}"
        )
        raise SettingsError(msg)
    return path


def _guard_write(*, path: Path, env: Env, user: str) -> None:
    space = space_of(path=path)
    if space in GUARDED_SPACES and (env != space or user != f"fryer-{space}"):
        msg = (
            f"refusing to write to the {space} space at {path}: only fryer-{space}"
            f" with FRYER_ENV={space} may, not {user} with FRYER_ENV={env}"
        )
        raise SettingsError(msg)
