from typing import Optional, Union

import pytest

from core.schema import Field, FieldType, FieldTypes, Schema, SchemaValidationError


@pytest.fixture
def test_schema() -> Schema:
    fields = (
        Field(name="event_id", type=FieldType(type=FieldTypes.str)),
        Field(name="created_at", type=FieldType(type=FieldTypes.str)),
        Field(name="user_id", type=FieldType(type=FieldTypes.int)),
        Field(
            name="permission",
            type=FieldType(type=FieldTypes.enum, variants=frozenset(["read", "write"])),
        ),
        Field(name="is_active", type=FieldType(type=FieldTypes.bool), required=False),
    )
    yield Schema(fields=fields)


@pytest.mark.parametrize(
    "event",
    [
        pytest.param(
            {
                "event_id": "a",
                "created_at": "2020-01-01T00:00:00.000",
                "user_id": 1,
                "permission": "read",
                "is_active": True,
            },
            id="valid",
        ),
        pytest.param(
            {
                "event_id": "a",
                "created_at": "2020-01-01T00:00:00.000",
                "user_id": 1,
                "permission": "read",
            },
            id="valid w/o is_active",
        ),
    ],
)
def test_validate_valid_events(test_schema: Schema, event: dict) -> None:
    assert test_schema.validate(event) is None, "Event is valid"


@pytest.mark.parametrize(
    "event",
    [
        pytest.param(
            {
                "created_at": "2020-01-01T00:00:00.000",
                "user_id": 1,
                "permission": "read",
                "is_active": True,
            },
            id="missing required field",
        ),
        pytest.param(
            {
                "event_id": "a",
                "created_at": "2020-01-01T00:00:00.000",
                "user_id": 1,
                "permission": "jump",
                "is_active": True,
            },
            id="invalid enum value",
        ),
        pytest.param(
            {
                "event_id": "a",
                "created_at": "2020-01-01T00:00:00.000",
                "user_id": 1,
                "permission": "jump",
                "is_active": True,
            },
            id="invalid enum value",
        ),
        pytest.param(
            {
                "event_id": "a",
                "created_at": "2020-01-01T00:00:00.000",
                "user_id": 1,
                "permission": "jump",
                "is_active": True,
                "extra": "extra",
            },
            id="not described field",
        ),
    ],
)
def test_validate_invalid_events(test_schema: Schema, event: dict) -> None:
    with pytest.raises(SchemaValidationError):
        test_schema.validate(event)


@pytest.mark.parametrize(
    "field_type, variants",
    [
        pytest.param("unknown", None, id="unknown type"),
        pytest.param(FieldTypes.enum, None, id="enum without variants"),
        pytest.param(FieldTypes.str, frozenset(["a", "b"]), id="not enum with variants"),
    ],
)
def test_init_schema_with_non_existing_type(
    field_type: Union[FieldTypes, str], variants: Optional[frozenset[str]]
) -> None:
    with pytest.raises(ValueError):
        f = Field(name="a", type=FieldType(type=field_type, variants=variants))  # noqa
        Schema(fields=(f,))


def test_load_schema_from_file(test_schema: Schema) -> None:
    assert Schema.from_file("./tests/data/test_schema.yaml") == test_schema
