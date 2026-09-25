"""Lakeflow Declarative Pipelines API with a fallback to the legacy `dlt` module."""

try:
    from pyspark import pipelines as _dp

    table = _dp.table
    materialized_view = _dp.materialized_view
    temporary_view = _dp.temporary_view
    expect_all = _dp.expect_all
    expect_all_or_drop = _dp.expect_all_or_drop
    create_streaming_table = _dp.create_streaming_table
    create_auto_cdc_flow = _dp.create_auto_cdc_flow
except ImportError:  # older Databricks runtimes
    import dlt as _dp  # type: ignore

    table = _dp.table
    materialized_view = _dp.table
    temporary_view = _dp.view
    expect_all = _dp.expect_all
    expect_all_or_drop = _dp.expect_all_or_drop
    create_streaming_table = _dp.create_streaming_table
    create_auto_cdc_flow = _dp.apply_changes


def with_rules(fn, drop_rules=None, warn_rules=None):
    """Attach expectations only when a rule set is non-empty."""
    if warn_rules:
        fn = expect_all(warn_rules)(fn)
    if drop_rules:
        fn = expect_all_or_drop(drop_rules)(fn)
    return fn
