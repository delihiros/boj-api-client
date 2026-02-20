from __future__ import annotations

import pytest

from boj_api_client.core.errors import BojValidationError
from boj_api_client.timeseries.queries import DataCodeQuery, DataLayerQuery, MetadataQuery
from boj_api_client.timeseries.validators import (
    normalize_data_code_query,
    normalize_data_layer_query,
    strict_validate_data_code_query,
    strict_validate_data_layer_query,
    strict_validate_metadata_query,
)


def test_resilient_normalize_dedupes_code_keep_order():
    query = DataCodeQuery(db="CO", code=["A", "B", "A", "C"])
    normalized = normalize_data_code_query(query)
    assert normalized.code == ("A", "B", "C")
    assert isinstance(normalized.code, tuple)


def test_data_code_query_with_codes_returns_new_instance():
    query = DataCodeQuery(db="CO", code=["A", "B"])
    replaced = query.with_codes(["X"])
    assert replaced is not query
    assert query.code == ("A", "B")
    assert replaced.code == ("X",)


def test_data_code_query_rejects_scalar_string_input():
    with pytest.raises(TypeError):
        DataCodeQuery(db="CO", code="ABC")


def test_data_code_query_rejects_non_string_entries():
    with pytest.raises(TypeError):
        DataCodeQuery(db="CO", code=["A", 1])  # type: ignore[list-item]


def test_strict_rejects_duplicate_codes():
    query = DataCodeQuery(db="CO", code=["A", "A"])
    with pytest.raises(BojValidationError):
        strict_validate_data_code_query(query)


def test_strict_rejects_more_than_250_codes():
    query = DataCodeQuery(db="CO", code=[f"C{i}" for i in range(251)])
    with pytest.raises(BojValidationError):
        strict_validate_data_code_query(query)


def test_layer_requires_layer1_and_frequency():
    query = DataLayerQuery(db="MD10", frequency="Q", layer1="*")
    strict_validate_data_layer_query(query)

    with pytest.raises(BojValidationError):
        strict_validate_data_layer_query(DataLayerQuery(db="MD10", frequency="", layer1="*"))


def test_layer_must_be_contiguous():
    query = DataLayerQuery(db="MD10", frequency="Q", layer1="*", layer2="A", layer4="B")
    with pytest.raises(BojValidationError):
        strict_validate_data_layer_query(query)
    with pytest.raises(BojValidationError):
        normalize_data_layer_query(query)


def test_metadata_requires_db():
    with pytest.raises(BojValidationError):
        strict_validate_metadata_query(MetadataQuery(db=""))
