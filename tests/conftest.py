import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from fryer.core.settings import GUARDED_SPACES, space_of

if TYPE_CHECKING:
    from collections.abc import Mapping


def refuse_real_spaces(*, environ: Mapping[str, str]) -> None:
    # Tests write wherever settings point, so they must never point at prod or test
    places = [environ.get("FRYER_PATH_WRITE", "")]
    places += environ.get("FRYER_PATH_READ", "").split(os.pathsep)
    for place in filter(None, places):
        space = space_of(path=Path(place))
        if space in GUARDED_SPACES:
            msg = f"tests refuse to run with a FRYER_PATH in the {space} space: {place}"
            raise pytest.UsageError(msg)


def pytest_configure() -> None:
    refuse_real_spaces(environ=os.environ)
