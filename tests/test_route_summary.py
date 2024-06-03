# -*- coding: utf-8 -*-
"""Tests for the GTFS route summary methods."""

import pandas as pd
import pytest

from gtfslite import GTFS


@pytest.fixture(name="gtfs_data")
def fix_gtfs_data(feed_zipfile) -> GTFS:
    """Load GTFS data."""
    return GTFS.load_zip(feed_zipfile)


def test_routes_summary(gtfs_data: GTFS, test_date):
    """Test `routes_summary` method produces result."""
    summary = gtfs_data.routes_summary(test_date)
    assert isinstance(summary, pd.DataFrame)

    expected_columns = {
        "route_id",
        "trips",
        "first_departure",
        "last_arrival",
        "average_headway",
    }

    assert (
        set(summary.columns.tolist()) >= expected_columns
    ), "missing routes summary columns"
