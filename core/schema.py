from enum import Enum
from typing import Any, FrozenSet, Optional, Tuple

import attr
import yaml


@attr.s(auto_attribs=True)
class SchemaValidationError(Exception):
    """
    Exception raised when a schema validation fails.
    """

    msg: str


class FieldTypes(str, Enum):
    """Enum of all possible field types. This is used to validate the schema."""

    str = "str"
    int = "int"
    enum = "enum"
    bool = "bool"


@attr.s(frozen=True, slots=True)
class FieldType:
    """A field type is a part of a schema."""

    type: FieldTypes = attr.ib(converter=FieldTypes)
    variants: Optional[FrozenSet[str]] = attr.ib(
        default=None,
        converter=lambda v: frozenset(v) if v else None,
    )

    def __attrs_post_init__(self) -> None:
        if self.type != FieldTypes.enum and self.variants:
            raise ValueError("Only enum type can have variants")
        if self.type == FieldTypes.enum and not self.variants:
            raise ValueError("Enum type must have variants")

    def validate(self, value: Any) -> bool:
        """
        Validate a value against the field type.

        :param value: The value to validate.
        :return: whether the value type is valid.
        """
        if self.type == FieldTypes.enum:
            return str(value).lower() in self.variants
        return self.type.value == type(value).__name__


@attr.s(frozen=True, slots=True)
class Field:
    """
    A field represents a part of a schema.
    """

    name: str = attr.ib()
    type: FieldType = attr.ib()
    required: bool = attr.ib(default=True)

    def validate(self, value: Any) -> bool:
        return self.type.validate(value)


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Schema:
    """A schema to validate events."""

    _fields: Tuple[Field, ...]

    @staticmethod
    def from_file(file_path: str) -> "Schema":
        """
        Load a schema from a file.

        :param file_path: path to the schema file.
        :return: Schema object.
        """
        with open(file_path, "r") as fd:
            raw_schema = yaml.safe_load(fd)
            assert raw_schema["kind"] == "Schema", "Schema file must have kind `Schema`"

            fields = [
                Field(
                    name=field["name"],
                    type=FieldType(type=field["type"], variants=field.get("variants", None)),
                    required=field.get("required", True),
                )
                for field in raw_schema["schema"]
            ]
            return Schema(tuple(fields))

    def validate(self, raw_event: dict) -> None:
        """
        Validate an event against the schema.

        Iterate over all schema's fields and validate them:
        1. If a field is optional and it's not in schema - OK.
        2. If a field is required and it has a valid type - OK.
        3. If a field is unknown: it's in the raw_event, but not in the schema - raise error.
        4. If a field is required and it's not in schema - raise error.
        5. If a field is required and it's type is not valid - raise error.

        :raise: SchemaValidationError if validation is failed.
        :param raw_event: a dict representing an event.
        :return: None
        """
        seen = set()
        for field in self._fields:
            if field.name not in raw_event:
                if not field.required:
                    continue
                raise SchemaValidationError(f"Missing required field {field.name}")

            if field.required and not field.validate(raw_event[field.name]):
                raise SchemaValidationError(
                    f"Invalid type for field {field.name}: expected {field.type.type.value}, "
                    f"but got {type(raw_event[field.name]).__name__}"
                )
            seen.add(field.name)

        if len(raw_event) != len(seen):
            raise SchemaValidationError(f"Unknown fields: {set(raw_event.keys()) - seen}")
