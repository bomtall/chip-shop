from typing import TYPE_CHECKING

from fryer.validate.completeness import RowCountBounds

if TYPE_CHECKING:
    from collections.abc import Mapping

    from fryer.validate.validator import Validator, ValidatorName

# Built from the classes, so a name cannot point at the wrong validator
VALIDATORS: Mapping[ValidatorName, type[Validator]] = {
    validator.name: validator for validator in (RowCountBounds,)
}


def load_validator(*, name: ValidatorName, params: Mapping[str, object]) -> Validator:
    # params come from a dataset's config; the validator's class checks them
    return VALIDATORS[name].model_validate(params)
