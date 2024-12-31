from pathlib import Path

import fryer.data.all


def test_import():
    path_file = Path(__file__)
    path_modules = (path_file.parents[2] / "src" / "fryer").iterdir()
    for path_module in path_modules:
        module = path_module.stem
        if path_module.suffix == ".py" and module not in {"all", "__init__"}:
            assert module in fryer.data.all.__all__
