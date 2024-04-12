import calendar
import datetime
import json
import math
import os
import warnings
from zipfile import ZipFile

import numpy as np
import pandas as pd

from .exceptions import (
    DateNotSetException,
    DateNotValidException,
    FeedNotValidException,
)


class GTFS:
    """A representation of a single static GTFS feed and associated data.

    All parameters should be valid Pandas DataFrames that follow
    the structure corresponding to the dataset as defined by the GTFS
    standard (http://gtfs.org/reference/static).

    Parameters"stop_timezone"
        Transit agencies with service represented in this dataset.
    stops : `pandas.DataFrame`
        Stops where vehicles pick up or drop off riders. Also defines
        stations and station entrances.
    routes : `pandas.DataFrame`
        Transit routes. A route is a group of trips that are
        displayed to riders as a single service.
    trips : `pandas.DataFrame`
        Trips for each route. A trip is a sequence of two or more
        stops that occur during a specific time period.
    stop_times : `pandas.DataFrame`
        Times that a vehicle arrives at and departs from stops for each trip.
    trips : `pandas.DataFrame`
        Trips for each route. A trip is a sequence of two or more
        stops that occur during a specific time period.
    trips : `pandas.DataFrame`
        Trips for each route. A trip is a sequence of two or more
        stops that occur during a specific time period.
    calendar : `pandas.DataFrame`, conditionally required
        Service dates specified using a weekly schedule with
        start and end dates. This file is required unless all dates of service
        are defined in calendar_dates.
    calendar : `pandas.DataFrame`, conditionally required
        Exceptions for the services defined in `calendar`. If `calendar` is omitted, then calendar_dates is required and must contain all dates of service.
    fare_attributes : `pandas.DataFrame`, default None
        Fare information for a transit agency's routes.
    fare_rules : `pandas.DataFrame`, default None
        Rules to apply fares for itineraries.
    shapes : `pandas.DataFrame`, default None
        Rules for mapping vehicle travel paths, sometimes referred to as route alignments.
    frequencies : `pandas.DataFrame`, default None
        Headway (time between trips) for headway-based service or a compressed representation of fixed-schedule service.
    transfers : `pandas.DataFrame`, default None
        Rules for making connections at transfer points between routes.
    pathways : `pandas.DataFrame`, default None
        Pathways linking together locations within stations.
    levels : `pandas.DataFrame`, default None
        Levels within stations.
    feed_info : `pandas.DataFrame`, default None
        Dataset metadata, including publisher, version, and expiration information.
    translations: `pandas.DataFrame`, default None
        In regions that have multiple official languages,
        transit agencies/operators typically have language-specific names and
        web pages. In order to best serve riders in those regions, it is useful
        for the dataset to include these language-dependent values..
    attributions : `pandas.DataFrame`, default None
        Dataset attributions.

    Raises
    ------
    FeedNotValidException
        If the feed doesnt' contain the required files or is otherwise invalid.
    """

    REQUIRED_FILES = [
        "agency.txt",
        "stops.txt",
        "routes.txt",
        "trips.txt",
        "stop_times.txt",
    ]

    OPTIONAL_FILES = [
        "calendar.txt",
        "calendar_dates.txt",
        "fare_attributes.txt",
        "fare_rules.txt",
        "shapes.txt",
        "frequencies.txt",
        "transfers.txt",
        "pathways.txt",
        "levels.txt",
        "translations.txt",
        "feed_info.txt",
        "attributions.txt",
    ]

    def __init__(
        self,
        agency,
        stops,
        routes,
        trips,
        stop_times,
        calendar=None,
        calendar_dates=None,
        fare_attributes=None,
        fare_rules=None,
        shapes=None,
        frequencies=None,
        transfers=None,
        pathways=None,
        levels=None,
        translations=None,
        feed_info=None,
        attributions=None,
    ):
        """Constructs and validates the datasets for the GTFS object."""

        # Mandatory Files
        self.agency = agency
        self.stops = stops
        self.routes = routes
        self.trips = trips
        self.stop_times = stop_times

        # Pairwise Mandatory Files
        self.calendar = calendar
        self.calendar_dates = calendar_dates

        if self.calendar is None and self.calendar_dates is None:
            raise FeedNotValidException(
                "One of calendar or calendar_dates is required."
            )

        # Optional Files
        self.fare_attributes = fare_attributes
        self.fare_rules = fare_rules
        self.shapes = shapes
        self.frequencies = frequencies
        self.transfers = transfers
        self.pathways = pathways
        self.levels = levels
        self.attributions = attributions
        self.translations = translations
        self.feed_info = feed_info

    @staticmethod
    def _load_clean_feed(filepath, optional=False, dtype=None, **pandas_kwargs):
        """Load a feed cleanly by stripping column names.

        Loads a feed. If the feed is empty (produces an empty dataframe) and the
        item is optional, a None is returned, otherwise an error is raised.

        Keyword arguments can be passd also to make parsing easier.

        Parameters
        ----------
        filepath : str
            path to the file

        Returns
        -------
        pd.DataFrame or None
            A dataframe that is loaded.
        """
        try:
            df = pd.read_csv(filepath, dtype=dtype, **pandas_kwargs)
            df.columns = df.columns.str.strip()

            # Deal with column names not having the right types
            for (
                k,
                v,
            ) in dtype.items():
                if k in df.columns:
                    df[k] = df[k].astype(v)

            if df.empty:
                if optional:
                    return None
                else:
                    raise pd.errors.EmptyDataError("This required file is empty")
            # Strip all column whitespace on load
            if dtype is not None:
                for c in df.columns:
                    try:
                        if dtype[c] is str:
                            df[c] = df[c].str.strip()
                            df[c] = df[c].str.replace("nan", "")
                    except KeyError:
                        pass
            return df
        except pd.errors.EmptyDataError:
            if optional:
                return None
            else:
                raise
        except UnicodeDecodeError as unicode_error:
            print(f"UnicodeDecodeError loading {filepath}: {unicode_error}")

    @classmethod
    def load_zip(
        self, filepath, ignore_optional_files: None | str = None, **pandas_kwargs
    ):
        """Creates a GTFS object based on a provided zipfolder.

        For parsing feeds with different encodings, you can pass any Pandas
        read_csv keyword arguments along.

        Parameters
        ----------
        filepath : str
            The path to the zipfile
        ignore_files: None or str
            Optional files to ignore. Can be None (keep all files), "all"
            (ignore all optional files), or "keep_shapes" which only keeps the
            optional shapes.txt file.

        Returns
        -------
        GTFS
            A GTFS object with loaded data.
        """

        if ignore_optional_files not in [None, "all", "keep_shapes"]:
            raise ValueError(
                "Ignore optional files must be None, 'all', 'or 'keep_shapes'"
            )

        if ignore_optional_files == None:
            to_ignore = []
        if ignore_optional_files == "all":
            to_ignore = GTFS.OPTIONAL_FILES
        if ignore_optional_files == "keep_shapes":
            to_ignore = [i for i in GTFS.OPTIONAL_FILES if i != "shapes.txt"]

        with ZipFile(filepath, "r") as zip_file:
            # Deal with nested files
            filepaths = dict()
            for req in GTFS.REQUIRED_FILES + GTFS.OPTIONAL_FILES:
                filepaths[req] = None
            for file in zip_file.namelist():
                for req in GTFS.REQUIRED_FILES + GTFS.OPTIONAL_FILES:
                    if req == os.path.basename(file):
                        filepaths[req] = file

            # Create pandas objects of the entire feed
            agency = self._load_clean_feed(
                zip_file.open(filepaths["agency.txt"]),
                dtype={
                    "agency_id": str,
                    "agency_name": str,
                    "agency_url": str,
                    "agency_timezone": str,
                    "agency_lang": str,
                    "agency_phone": str,
                    "agency_fare_url": str,
                    "agency_email": str,
                },
                skipinitialspace=True,
                **pandas_kwargs,
            )
            stops = self._load_clean_feed(
                zip_file.open(filepaths["stops.txt"]),
                dtype={
                    "stop_id": str,
                    "stop_code": str,
                    "stop_name": str,
                    "stop_desc": str,
                    "stop_lat": float,
                    "stop_lon": float,
                    "zone_id": str,
                    "stop_url": str,
                    "location_type": "Int64",
                    "parent_station": str,
                    "stop_timezone": str,
                    "wheelchair_boarding": "Int64",
                    "level_id": str,
                    "platform_code": str,
                },
                skipinitialspace=True,
                **pandas_kwargs,
            )
            routes = self._load_clean_feed(
                zip_file.open(filepaths["routes.txt"]),
                dtype={
                    "route_id": str,
                    "agency_id": str,
                    "route_short_name": str,
                    "route_long_name": str,
                    "route_desc": str,
                    "route_type": int,
                    "route_url": str,
                    "route_color": str,
                    "route_text_color": str,
                    "route_short_order": int,
                },
                skipinitialspace=True,
                **pandas_kwargs,
            )
            trips = self._load_clean_feed(
                zip_file.open(filepaths["trips.txt"]),
                dtype={
                    "route_id": str,
                    "service_id": str,
                    "trip_id": str,
                    "trip_headsign": str,
                    "trip_short_name": str,
                    "direction_id": "Int64",
                    "block_id": str,
                    "shape_id": str,
                    "wheelchair_accessible": "Int64",
                    "bikes_allowed": "Int64",
                },
                skipinitialspace=True,
                **pandas_kwargs,
            )
            stop_times = self._load_clean_feed(
                zip_file.open(filepaths["stop_times.txt"]),
                dtype={
                    "trip_id": str,
                    "arrival_time": str,
                    "departure_time": str,
                    "stop_id": str,
                    "stop_sequence": int,
                    "stop_headsign": str,
                    "pickup_type": "Int64",
                    "drop_off_type": "Int64",
                    "shape_dist_traveled": float,
                    "timepoint": "Int64",
                },
                skipinitialspace=True,
                **pandas_kwargs,
            )

            if filepaths["calendar.txt"] in zip_file.namelist():
                calendar = self._load_clean_feed(
                    zip_file.open(filepaths["calendar.txt"]),
                    dtype={
                        "service_id": str,
                        "monday": int,
                        "tuesday": int,
                        "wednesday": int,
                        "thursday": int,
                        "friday": int,
                        "saturday": int,
                        "sunday": int,
                        "start_date": str,
                        "end_date": str,
                    },
                    skipinitialspace=True,
                    optional=True,
                    **pandas_kwargs,
                )

            else:
                calendar = None

            if filepaths["calendar_dates.txt"] in zip_file.namelist():
                calendar_dates = self._load_clean_feed(
                    zip_file.open(filepaths["calendar_dates.txt"]),
                    dtype={"service_id": str, "date": str, "exception_type": int},
                    skipinitialspace=True,
                    optional=True,
                    **pandas_kwargs,
                )
            else:
                calendar_dates = None

            if (
                filepaths["fare_attributes.txt"] in zip_file.namelist()
                and "fare_attributes.txt" not in to_ignore
            ):
                fare_attributes = self._load_clean_feed(
                    zip_file.open(filepaths["fare_attributes.txt"]),
                    dtype={
                        "fare_id": str,
                        "price": float,
                        "currency_type": str,
                        "payment_method": int,
                        "transfers": "Int64",
                        "agency_id": str,
                        "transfer_duration": "Int64",
                    },
                    skipinitialspace=True,
                    optional=True,
                    **pandas_kwargs,
                )
            else:
                fare_attributes = None

            if (
                filepaths["fare_rules.txt"] in zip_file.namelist()
                and "fare_rules.txt" not in to_ignore
            ):
                fare_rules = self._load_clean_feed(
                    zip_file.open(filepaths["fare_rules.txt"]),
                    dtype={
                        "fare_id": str,
                        "route_id": str,
                        "origin_id": str,
                        "destination_id": str,
                        "contains_id": str,
                    },
                    skipinitialspace=True,
                    optional=True,
                    **pandas_kwargs,
                )
            else:
                fare_rules = None

            if (
                filepaths["shapes.txt"] in zip_file.namelist()
                and "shapes.txt" not in to_ignore
            ):
                shapes = self._load_clean_feed(
                    zip_file.open(filepaths["shapes.txt"]),
                    dtype={
                        "shape_id": str,
                        "shape_pt_lat": float,
                        "shape_pt_lon": float,
                        "shape_pt_sequence": int,
                        "shape_dist_traveled": float,
                    },
                    skipinitialspace=True,
                    optional=True,
                    **pandas_kwargs,
                )
            else:
                shapes = None

            if (
                filepaths["frequencies.txt"] in zip_file.namelist()
                and "frequencies.txt" not in to_ignore
            ):
                frequencies = self._load_clean_feed(
                    zip_file.open(filepaths["frequencies.txt"]),
                    dtype={
                        "trip_id": str,
                        "start_time": str,
                        "end_time": str,
                        "headway_secs": int,
                        "exact_times": "Int64",
                    },
                    skipinitialspace=True,
                    optional=True,
                    **pandas_kwargs,
                )
            else:
                frequencies = None

            if (
                filepaths["transfers.txt"] in zip_file.namelist()
                and "transfers.txt" not in to_ignore
            ):
                transfers = self._load_clean_feed(
                    zip_file.open(filepaths["transfers.txt"]),
                    dtype={
                        "from_stop_id": str,
                        "to_stop_id": str,
                        "transfer_type": "Int64",
                        "min_transfer_time": "Int64",
                    },
                    skipinitialspace=True,
                    optional=True,
                    **pandas_kwargs,
                )
            else:
                transfers = None

            if (
                filepaths["pathways.txt"] in zip_file.namelist()
                and "pathways.txt" not in to_ignore
            ):
                pathways = self._load_clean_feed(
                    zip_file.open(filepaths["pathways.txt"]),
                    dtype={
                        "pathway_id": str,
                        "from_stop_id": str,
                        "to_stop_id": str,
                        "pathway_mode": int,
                        "is_bidirectional": str,
                        "length": "float64",
                        "traversal_time": "Int64",
                        "stair_count": "Int64",
                        "max_slope": "float64",
                        "min_width": "float64",
                        "signposted_as": str,
                        "reverse_signposted_as": str,
                    },
                    skipinitialspace=True,
                    optional=True,
                    **pandas_kwargs,
                )
            else:
                pathways = None

            if (
                filepaths["levels.txt"] in zip_file.namelist()
                and "levels.txt" not in to_ignore
            ):
                levels = self._load_clean_feed(
                    zip_file.open(filepaths["levels.txt"]),
                    dtype={"level_id": str, "level_index": float, "level_name": str},
                    skipinitialspace=True,
                    optional=True,
                )
            else:
                levels = None

            if (
                filepaths["translations.txt"] in zip_file.namelist()
                and "translations.txt" not in to_ignore
            ):
                translations = self._load_clean_feed(
                    zip_file.open(filepaths["translations.txt"]),
                    dtype={
                        "table_name": str,
                        "field_name": str,
                        "language": str,
                        "translation": str,
                        "record_id": str,
                        "record_sub_id": str,
                        "field_value": str,
                    },
                    skipinitialspace=True,
                    optional=True,
                )
                feed_info = self._load_clean_feed(
                    zip_file.open(filepaths["feed_info.txt"]),
                    dtype={
                        "feed_publisher_name": str,
                        "feed_publisher_url": str,
                        "feed_lang": str,
                        "default_lang": str,
                        "feed_start_date": str,
                        "feed_end_date": str,
                        "feed_version": str,
                        "feed_contact_email": str,
                        "feed_contact_url": str,
                    },
                    skipinitialspace=True,
                    optional=True,
                    **pandas_kwargs,
                )
            elif (
                filepaths["feed_info.txt"] in zip_file.namelist()
                and "feed_info.txt" not in to_ignore
            ):
                feed_info = self._load_clean_feed(
                    zip_file.open(filepaths["feed_info.txt"]),
                    dtype={
                        "feed_publisher_name": str,
                        "feed_publisher_url": str,
                        "feed_lang": str,
                        "default_lang": str,
                        "feed_start_date": str,
                        "feed_end_date": str,
                        "feed_version": str,
                        "feed_contact_email": str,
                        "feed_contact_url": str,
                    },
                    skipinitialspace=True,
                    optional=True,
                    **pandas_kwargs,
                )
                translations = None
            else:
                translations = None
                feed_info = None

            if (
                filepaths["attributions.txt"] in zip_file.namelist()
                and "attributions.txt" not in to_ignore
            ):
                attributions = pd.read_csv(
                    zip_file.open(filepaths["attributions.txt"]),
                    dtype={
                        "attribution_id": str,
                        "agency_id": str,
                        "route_id": str,
                        "trip_id": str,
                    },
                    skipinitialspace=True,
                    **pandas_kwargs,
                )
            else:
                attributions = None

        return GTFS(
            agency,
            stops,
            routes,
            trips,
            stop_times,
            calendar=calendar,
            calendar_dates=calendar_dates,
            fare_attributes=fare_attributes,
            fare_rules=fare_rules,
            shapes=shapes,
            frequencies=frequencies,
            transfers=transfers,
            pathways=pathways,
            levels=levels,
            translations=translations,
            feed_info=feed_info,
            attributions=attributions,
        )

    def summary(self) -> pd.Series:
        """Assemble a series of attributes summarizing the GTFS feed with the
        following columns:

        * *agencies*: list of agencies in feed
        * *total_stops*: the total number of stops in the feed
        * *total_routes*: the total number of routes in the feed
        * *total_trips*: the total number of trips in the feed
        * *total_stops_made*: the total number of stop_times events
        * *first_date*: (`datetime.date`) the first date the feed is valid for
        * *last_date*: (`datetime.date`) the last date the feed is valid for
        * *total_shapes* (optional): the total number of shapes.

        Returns
        -------
        pandas.Series
            A Pandas series containing the required data.
        """

        summary = pd.Series(dtype=str)
        summary["agencies"] = self.agency.agency_name.tolist()
        summary["total_stops"] = self.stops.shape[0]
        summary["total_routes"] = self.routes.shape[0]
        summary["total_trips"] = self.trips.shape[0]
        summary["total_stops_made"] = self.stop_times.shape[0]
        first_dates = []
        last_dates = []
        if self.calendar is not None:
            first_dates.append(self.calendar.start_date.min())
            last_dates.append(self.calendar.end_date.max())
        if self.calendar_dates is not None:
            first_dates.append(self.calendar_dates.date.min())
            last_dates.append(self.calendar_dates.date.max())

        summary["first_date"] = min(first_dates)
        summary["last_date"] = max(last_dates)

        if self.shapes is not None:
            summary["total_shapes"] = self.shapes.shape[0]

        return summary

    def valid_date(self, date_to_check: datetime.date):
        """Checks whether the provided date falls within the feed's date range.

        Note that this does not check whether any trips run on a given date,
        only whether or not the calendar and calendar dates files span or
        include the provided date in their service.

        Parameters
        ----------
        date : `datetime.date`
            A date or object to be validated against the feed

        Returns
        -------
        bool
            Whether the date is valid or not.
        """

        summary = self.summary()

        first_date = summary.first_date
        last_date = summary.last_date

        # First we check the calendar dates to see if it falls within that
        if self.calendar is not None:
            first_date = datetime.datetime.strptime(
                self.calendar.start_date.min(), "%Y%m%d"
            ).date()
            last_date = datetime.datetime.strptime(
                self.calendar.end_date.max(), "%Y%m%d"
            ).date()
            if first_date <= date_to_check and last_date >= date_to_check:
                return True

        # Now we check specifically to see if the provided date appears in calendar dates
        if self.calendar_dates is not None:
            valid_dates = self.calendar_dates[
                (self.calendar_dates.date == date_to_check.strftime("%Y%m%d"))
                & (self.calendar_dates.exception_type == 1)
            ]
            if valid_dates.shape[0] > 0:
                return True

        return False

    def date_trips(self, date: datetime.date) -> pd.DataFrame:
        """Finds all the trips that occur on a specified day. This method
        accounts for exceptions included in the `calendar_dates` dataset.

        Parameters
        ----------
        date : `datetime.date`
            The service day to count trips on

        Returns
        -------
        `DataFrame`
            A dataframe of trips which are run on the provided date.
        """

        dayname = date.strftime("%A").lower()

        if self.calendar is not None:
            # Need a copy so we can apply dates
            calendar = self.calendar.copy()
            calendar["start_date"] = pd.to_datetime(calendar["start_date"]).dt.date
            calendar["end_date"] = pd.to_datetime(calendar["end_date"]).dt.date
            # Get all the service_ids for the desired day of the week
            service_ids = calendar[
                (calendar[dayname] == 1)
                & (calendar.start_date <= date)
                & (calendar.end_date >= date)
            ].service_id.tolist()
            if self.calendar_dates is not None:
                # Add service ids in the calendar_dates
                calendar_dates = self.calendar_dates.copy()
                calendar_dates["date"] = pd.to_datetime(calendar_dates["date"]).dt.date
                service_ids.extend(
                    calendar_dates[
                        (calendar_dates.date == date)
                        & (calendar_dates.exception_type == 1)
                    ].service_id.tolist()
                )
                # Remove service ids from the calendar dates
                remove_service_ids = self.calendar_dates[
                    (calendar_dates.date == date) & (calendar_dates.exception_type == 2)
                ].service_id.tolist()

                service_ids = [i for i in service_ids if i not in remove_service_ids]
        else:
            calendar_dates = self.calendar_dates.copy()
            calendar_dates["date"] = pd.to_datetime(calendar_dates["date"]).dt.date
            service_ids = calendar_dates[
                (calendar_dates.date == date) & (calendar_dates.exception_type == 1)
            ].service_id.tolist()
        return self.trips[self.trips.service_id.isin(service_ids)]

    def route_summary(self, date, route_id):
        """Assemble a series of attributes summarizing a route on a particular
        day.

        The following columns are returned:
        * *route_id*: The ID of the route summarized
        * *total_trips*: The total number of trips made on the route that day
        * *first_departure*: The earliest departure of the bus for the day
        * *last_arrival*: The latest arrival of the bus for the day
        * *service_time*: The total service span of the route, in hours
        * *average_headway*: Average time in minutes between trips on the route

        Parameters
        ----------
        date : `datetime.date`
            The calendar date to summarize.
        route_id : str
            The ID of the route to summarize

        Returns
        -------
        `pandas.Series`
            A series with summary attributes for the date
        """

        trips = self.date_trips(date)
        trips = trips[trips.route_id == route_id]
        stop_times = self.stop_times[self.stop_times.trip_id.isin(trips.trip_id)]
        summary = pd.Series()
        summary["route_id"] = route_id
        summary["total_trips"] = len(trips.index)
        summary["first_departure"] = stop_times.departure_time.min()
        summary["last_arrival"] = stop_times.arrival_time.max()
        summary["service_time"] = (
            int(summary.last_arrival.split(":")[0])
            + int(summary.last_arrival.split(":")[1]) / 60.0
            + int(summary.last_arrival.split(":")[2]) / 3600.0
        ) - (
            int(summary.first_departure.split(":")[0])
            + int(summary.first_departure.split(":")[1]) / 60.0
            + int(summary.first_departure.split(":")[2]) / 3600.0
        )
        stop_id = stop_times.iloc[0].stop_id
        min_dep = stop_times[stop_times.stop_id == stop_id].departure_time.min()
        max_arr = stop_times[stop_times.stop_id == stop_id].arrival_time.max()
        visits = stop_times[stop_times.stop_id == stop_id].trip_id.count()
        route_headway = (
            int(max_arr.split(":")[0])
            + int(max_arr.split(":")[1]) / 60.0
            + int(max_arr.split(":")[2]) / 3600.0
            - (
                int(min_dep.split(":")[0])
                + int(min_dep.split(":")[1]) / 60.0
                + int(min_dep.split(":")[2]) / 3600.0
            )
        )
        summary["average_headway"] = 60 * route_headway / visits
        return summary

    def routes_summary(self, date):
        """Summarizes all routes in a given day. The columns of the resulting
        dataset match the columns of :func:`route_summary`

        :param date: The day to summarize.
        :type date: :py:mod:`datetime.date`
        :return: A :py:mod:`pandas.DataFrame` object containing the summarized
            data.
        """

        trips = self.date_trips(date)
        if "direction_id" in trips.columns:
            trips = trips[trips.direction_id == 0]
        stop_times = self.stop_times[self.stop_times.trip_id.isin(trips.trip_id)]
        stop_times = pd.merge(stop_times, trips, on="trip_id", how="left")
        route_trips = (
            trips[["route_id", "trip_id"]].groupby("route_id", as_index=False).count()
        )
        route_trips["trips"] = route_trips.trip_id
        first_departures = (
            stop_times[["route_id", "departure_time"]]
            .groupby("route_id", as_index=False)
            .min()
        )
        last_arrivals = (
            stop_times[["route_id", "arrival_time"]]
            .groupby("route_id", as_index=False)
            .max()
        )
        summary = pd.merge(route_trips, first_departures, on=["route_id"])
        summary = pd.merge(summary, last_arrivals, on=["route_id"])
        summary["service_time"] = (
            summary.arrival_time.str.split(":", n=-1, expand=True)[0].astype(int)
            + summary.arrival_time.str.split(":", n=-1, expand=True)[1].astype(int)
            / 60.0
            + summary.arrival_time.str.split(":", n=-1, expand=True)[2].astype(int)
            / 3600.0
        ) - (
            summary.departure_time.str.split(":", n=-1, expand=True)[0].astype(int)
            + summary.departure_time.str.split(":", n=-1, expand=True)[1].astype(int)
            / 60.0
            + summary.departure_time.str.split(":", n=-1, expand=True)[2].astype(int)
            / 3600.0
        )
        summary["average_headway"] = 60 * summary.service_time / summary.trips
        summary["last_arrival"] = summary.arrival_time
        summary["first_departure"] = summary.departure_time
        summary = summary[
            ["route_id", "trips", "first_departure", "last_arrival", "average_headway"]
        ]
        summary = pd.merge(self.routes, summary, on="route_id", how="inner")
        return summary

    def service_hours(
        self,
        date: datetime.date,
        start_time: str = None,
        end_time: str = None,
        time_field: str = "arrival_time",
    ) -> float:
        """Computes the total service hours delivered for a specified date
        within an optionally specified time slice.

        This method measures this value by considering partial trips as having
        stopped at the end of the time slice. In other words, partial trips are
        included in the total service hours during the specified time slice.

        Parameters
        ----------
        date : datetime.date
            The dat of analysis
        start_time : str, optional
            The starttime in the format HH:MM:SS, by default None
        end_time : str, optional
            The starttime in the format HH:MM:SS, by default None
        time_field : {'arrival_time', 'departure_time'}, optional
            The time field to use for the calucation, by default 'arrival_time'

        Returns
        -------
        float
            The total service hours in the specified period.

        Raises
        ------
        DateNotValidException
            The date falls outside of the feed's span.
        """

        trips = self.date_trips(date)

        # Grab all the stop_times
        stop_times = self.stop_times[self.stop_times.trip_id.isin(trips.trip_id)].copy()

        if start_time is not None:
            stop_times = stop_times[stop_times[time_field] >= start_time]
        if end_time is not None:
            stop_times = stop_times[stop_times[time_field] <= end_time]

        # stop_times[time_field] = pd.to_datetime(stop_times[time_field])
        stop_times.dropna(subset=[time_field], inplace=True)
        stop_times["seconds_since_midnight"] = (
            (stop_times[time_field].str.split(":").str[0].astype(int) * 3600)
            + (stop_times[time_field].str.split(":").str[1].astype(int) * 60)
            + (stop_times[time_field].str.split(":").str[2].astype(int))
        )
        grouped = (
            stop_times[["trip_id", "seconds_since_midnight"]]
            .groupby("trip_id", as_index=False)
            .agg({"seconds_since_midnight": ["max", "min"]})
        )
        grouped.columns = ["trip_id", "max", "min"]
        grouped.dropna(inplace=True)
        grouped["diff"] = grouped["max"] - grouped["min"]
        grouped["multiplier"] = 1
        if self.frequencies is not None:
            # Get the number of trips in the frequencies
            frequencies = self.frequencies.copy()
            frequencies.start_time = (
                (frequencies.start_time.str.split(":").str[0].astype(int) * 3600)
                + (frequencies.start_time.str.split(":").str[1].astype(int) * 60)
                + (frequencies.start_time.str.split(":").str[2].astype(int))
            )
            frequencies.end_time = (
                (frequencies.end_time.str.split(":").str[0].astype(int) * 3600)
                + (frequencies.end_time.str.split(":").str[1].astype(int) * 60)
                + (frequencies.end_time.str.split(":").str[2].astype(int))
            )
            if start_time is not None:
                frequencies["_start_time"] = (
                    int(start_time.split(":")[0]) * 3600
                    + int(start_time.split(":")[1]) * 60
                    + int(start_time.split(":")[2])
                )
                frequencies.start_time = frequencies[["start_time", "_start_time"]].max(
                    axis=1
                )
            if end_time is not None:
                frequencies["_end_time"] = (
                    int(end_time.split(":")[0]) * 3600
                    + int(end_time.split(":")[1]) * 60
                    + int(end_time.split(":")[2])
                )
                frequencies.end_time = frequencies[["end_time", "_end_time"]].min(
                    axis=1
                )
            frequencies["total_seconds"] = frequencies.end_time - frequencies.start_time
            frequencies["freq_multiplier"] = (
                frequencies["total_seconds"] / frequencies.headway_secs
            ).astype(int)

            grouped = pd.merge(grouped, frequencies, on="trip_id", how="left").fillna(0)
            grouped["multiplier"] = grouped[["multiplier", "freq_multiplier"]].max(
                axis=1
            )
        grouped["diff"] = grouped["diff"] * grouped["multiplier"]
        return grouped["diff"].sum() / 3600

    def stop_summary(
        self, stop_id: str, start_time: str = None, end_time: str = None
    ) -> pd.Series:
        """Assemble a series of attributes summarizing a stop on a particular
        day. The following columns are returned:

        * *stop_id*: The ID of the stop summarized
        * *total_visits*: The total number of times a stop is visited
        * *first_arrival*: The earliest arrival of the bus for the day
        * *last_arrival*: The latest arrival of the bus for the day
        * *service_time*: The total service span, in hours
        * *average_headway*: Average time in minutes between arrivals

        Parameters
        ----------
        stop_id : str
            The ID of the stop to summarize
        """

        # Create a summary of stops for a given stop_id
        date = self.date
        trips = self.date_trips(date)
        stop_times = self.stop_times[
            self.stop_times.trip_id.isin(trips.trip_id)
            & (self.stop_times.stop_id == stop_id)
        ]

        summary = self.stops[self.stops.stop_id == stop_id].iloc[0]
        summary["total_visits"] = len(stop_times.index)
        summary["first_arrival"] = stop_times.arrival_time.min()
        summary["last_arrival"] = stop_times.arrival_time.max()
        summary["service_time"] = (
            int(summary.last_arrival.split(":")[0])
            + int(summary.last_arrival.split(":")[1]) / 60.0
            + int(summary.last_arrival.split(":")[2]) / 3600.0
        ) - (
            int(stop_times.arrival_time.min().split(":")[0])
            + int(stop_times.arrival_time.min().split(":")[1]) / 60.0
            + int(stop_times.arrival_time.min().split(":")[2]) / 3600.0
        )
        summary["average_headway"] = (summary.service_time / summary.total_visits) * 60
        return summary

    def stop_times_at_stop(
        self,
        stop_id: str,
        date: datetime.date,
        start_time: str = None,
        end_time: str = None,
        time_field: str = "arrival_time",
    ) -> pd.DataFrame:
        """Get the stop times that visit a particular stop over a day or a subset of the day.

        Parameters
        ----------
        stop_id : str
            The stop ID to analyse
        date : datetime.date
            The calendar date to analyse
        start_time : str, optional
            A string representation (HH:MM:SS) of the number of hours since midnight on the
            analysis date. Can be greater than 24:00:00. A `None` value will
            assume consider all trips from the start of the service day, by default None
        end_time : str, optional
            A string representation (HH:MM:SS) of the number of hours since midnight on the
            analysis date. Can be greater than 24:00:00. A `None` value will
            consider all trips through the end of the service day, by default None
        time_field : str, optional
            The name of the time column in `stop_times` to consider, either 'arrival_time' or
            'departure_time'. By default 'arrival_time'


        Returns
        -------
        pd.DataFrame
            A copy of the `stop_trips` dataframe containing the filtered stop
            events.
        """

        stop_id = str(stop_id)
        stop_times = self.stop_times[
            self.stop_times.trip_id.isin(self.date_trips(date).trip_id)
            & (self.stop_times.stop_id == stop_id)
        ].copy()
        # Filter by start time and end time if needed
        if start_time != None:
            stop_times = stop_times[stop_times[time_field] >= start_time]
        if end_time != None:
            stop_times = stop_times[stop_times[time_field] <= end_time]

        return stop_times

    def trip_distribution(self, start_date, end_date):
        """Summarize the distribution of service by day of week for a given
        date range. Repeated days of the week will be counted multiple times.

        Parameters
        ----------
        start_date : `datetime.date`
            The start date for the summary (inclusive)
        end_date : `datetime.date`
            The end date for the summary

        Returns
        -------
        `pandas.Series`
            A series containing as indices the days of the week and as values
            the total number of trips found in the time slice provided.
        """

        days = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ]
        dist = pd.Series(index=days, name="trips", dtype="int")

        # Start with calendar:
        if self.calendar is not None:
            for dow in dist.index:
                # Get rows where that DOW happens in the date range
                for idx, service in self.calendar[
                    (self.calendar[dow] == True)
                    & (self.calendar.start_date.dt.date <= start_date)
                    & (self.calendar.end_date.dt.date >= end_date)
                ].iterrows():
                    # We'll need the number of a given days of the week in that range to multiply the calendar.
                    week = {}
                    for i in range(
                        ((end_date + datetime.timedelta(days=1)) - start_date).days
                    ):
                        day = calendar.day_name[
                            (start_date + datetime.timedelta(days=i + 1)).weekday()
                        ].lower()
                        week[day] = week[day] + 1 if day in week else 1

                    # Get trips with that service id and add them to the total, only if that day is in there.
                    if dow in week.keys():
                        dist[dow] = (
                            dist[dow]
                            + week[dow]
                            * self.trips[
                                self.trips.service_id == service.service_id
                            ].trip_id.count()
                        )

        # Now check exceptions to add and remove
        if self.calendar_dates is not None:
            # Start by going through all the calendar dates within the date range
            # cd = self.calendar_dates.copy()
            for index, cd in self.calendar_dates[
                (self.calendar_dates["date"].dt.date >= start_date)
                & (self.calendar_dates["date"].dt.date <= end_date)
            ].iterrows():
                if cd["exception_type"] == 1:
                    dist[days[cd["date"].dayofweek]] += self.trips[
                        self.trips.service_id == cd["service_id"]
                    ].trip_id.count()
                else:
                    dist[days[cd["date"].dayofweek]] -= self.trips[
                        self.trips.service_id == cd["service_id"]
                    ].trip_id.count()

        return dist

    def route_frequency_matrix(
        self,
        date: datetime.date,
        interval: int = 60,
        start_time: str = None,
        end_time: str = None,
        time_field: str = "arrival_time",
    ) -> pd.DataFrame:
        """Generate a matrix of route headways throughout a given time period.

        Produce a matrix of headways by a given interval (in minutes) for each
        route_id throughout the service period of a given day.

        Parameters
        ----------
        date : datetime.date
            The service day to analyze
        interval : int, optional
            The number of minute bins to divide the day into, by default 60
        start_time : str, optional
            The start time of the analysis. Only trips which *start* after this
            time will be included. Can be greater than 24:00:00.
            A None value will consider all trips from the start of the
            service day, by default None
        end_time : str, optional
            A string representation (HH:MM:SS) of the end time of the analysis.
            Only trips which *end* before this analysis time will be included. '
            Can be greater than 24:00:00. A None value will consider all trips
            through the end of the service day, by default None
        time_field : str, optional
            The name of the time column in `stop_times` to consider, either 'arrival_time' or
            'departure_time'. By default 'arrival_time'

        Returns
        -------
        pd.DataFrame
            A dataframe containing the following columns:
                route_id: The ID of the route
                bin_start: The start of the time interval measured (HH:MM)
                bin_end: The end of the time interval measured (HH:MM)
                trips: The count of the number of trips on that route in that interval
                frequency: The frequency of trips (in trips/hour) on the route
        """

        # Start by getting all of the trips and stop events in a given date
        trips = self.date_trips(date)
        stop_times = self.stop_times[self.stop_times.trip_id.isin(trips.trip_id)].copy()

        # Use a numerical timestamp for filtering
        stop_times["timestamp"] = 60 * stop_times[time_field].str.split(":").str[
            0
        ].astype(int) + stop_times[time_field].str.split(":").str[1].astype(int)

        # Let's get the start_times of all trips
        trip_start_times = (
            stop_times[["trip_id", "stop_sequence", "timestamp", time_field]]
            .sort_values(["trip_id", "stop_sequence"])
            .drop_duplicates("trip_id")
        )

        # Filter out our slice
        if start_time != None:
            start_time_int = 60 * int(start_time.split(":")[0]) + int(
                start_time.split(":")[1]
            )
            stop_times = stop_times[
                stop_times.trip_id.isin(
                    trip_start_times[
                        trip_start_times.timestamp >= start_time_int
                    ].trip_id
                )
            ]

        if end_time != None:
            # We need trip end times for this filter
            trip_end_times = (
                stop_times[["trip_id", "stop_sequence", "timestamp", time_field]]
                .sort_values(["trip_id", "stop_sequence"], ascending=False)
                .drop_duplicates("trip_id")
            )
            end_time_int = 60 * int(end_time.split(":")[0]) + int(
                end_time.split(":")[1]
            )
            stop_times = stop_times[
                stop_times.trip_id.isin(
                    trip_end_times[trip_end_times.timestamp <= end_time_int].trip_id
                )
            ]

        # Now get the trips we're working with
        trip_starts = trip_start_times[
            trip_start_times.trip_id.isin(stop_times.trip_id.unique())
        ]
        trip_starts = pd.merge(
            trip_starts, self.trips[["trip_id", "route_id"]], on="trip_id"
        )

        # Set up a basis for the matrix slices
        mx_start = trip_starts["timestamp"].min().tolist()
        mx_end = trip_starts["timestamp"].max().tolist()

        # We can now figure out how many columns we need
        mx_start_floor = mx_start - mx_start % interval
        mx_end_ceil = int(math.ceil(mx_end / interval) * interval)

        column_count = int((mx_end_ceil - mx_start_floor) / interval)

        # Template dataframe
        template_df = pd.DataFrame({"route_id": trip_starts.route_id.unique()})
        slices = []

        # Populate a matrix of values
        for i in range(column_count):
            # Determine the time slice
            slice_start_int = mx_start_floor + i * interval
            slice_end_int = mx_start_floor + (i + 1) * interval

            # Grab start formatting string
            hours, minutes = divmod(slice_start_int, 60)
            slice_start_str = f"{hours:02}:{minutes:02}"

            # Grab end formatting string
            hours, minutes = divmod(slice_end_int, 60)
            slice_end_str = f"{hours:02}:{minutes:02}"
            # Now we get the trips that start within our slice
            in_slice = trip_starts[
                (trip_starts.timestamp >= slice_start_int)
                & (trip_starts.timestamp < slice_end_int)
            ]
            slice_group = (
                in_slice[["route_id", "trip_id"]]
                .groupby("route_id", as_index=False)
                .count()
            )
            slice_group["frequency"] = (
                slice_group["trip_id"] * (60.0 / interval)
            ).astype(int)
            final = pd.merge(
                template_df, slice_group, on="route_id", how="left"
            ).fillna(0)
            final["frequency"] = final["frequency"].astype(int)
            final["trips"] = final["trip_id"].astype(int)
            final["bin_start"] = slice_start_str
            final["bin_end"] = slice_end_str
            slices.append(
                final[["route_id", "bin_start", "bin_end", "trips", "frequency"]]
            )

        # Assemble final matrix
        mx = pd.concat(slices, axis="index")
        return mx.reset_index(drop=True)

    def unique_trip_count_at_stops(
        self,
        stop_ids: list,
        date: datetime.date,
        start_time: str = None,
        end_time: str = None,
        time_field: str = "arrival_time",
    ) -> int:
        """Get a count of unique trips that visit a given set of stops

        This function returns a subset of the trips table which include trips
        that stop at _any_ of the stops provided, within provided times.

        Parameters
        ----------
        stop_ids : list
            A list of stop_ids to check
        date : `datetime.date`
            The service day to check
        start_time : str, optional
            A string representation (HH:MM:SS) of the number of hours since
            midnight on the analysis date. Can be greater than 24:00:00. A None
            value will consider all trips from the start of the service day, by
            default None
        end_time : str, optional
            A string representation (HH:MM:SS) of the number of hours since
            midnight on the analysis date. Can be greater than 24:00:00. A None
            value will consider all trips through the end of the service day, by
            default None
        time_field : str, optional
            The name of the time column in `stop_times` to consider, either
            'arrival_time' or 'departure_time'. By default 'arrival_time'

        Returns
        -------
        int
            An integer specifying the total number of unique trips that visit
            the supplied set of stops.

        Notes
        -----
            Not all GTFS datasets include arrival and/or departure times for
            every stop. In cases where times are only set at time points, and no
            interpolation is provided, this function will not work.
        """

        # Start by filtering all stop_trips by the given dateslice
        trips = self.date_trips(date)

        stop_ids = [str(s) for s in stop_ids]

        # First, we account for the stop_time scheduled trips
        stop_trips = self.stop_times[
            (self.stop_times.stop_id.isin(stop_ids))
            & (self.stop_times.trip_id.isin(trips.trip_id))
        ].copy()

        #  Now that we have all the trips in the criteria, remove those that don't have time data
        #  NOTE: This creates a problem since these trips visit certain stops...
        stop_trips = stop_trips[
            (~stop_trips[time_field].isna()) & (stop_trips[time_field] != "")
        ]

        # Create a column of seconds since midnight if we need it for filtering
        if start_time is not None or end_time is not None:
            stop_trips["_ssm"] = (
                (stop_trips[time_field].str.split(":").str[0].astype(int) * 3600)
                + (stop_trips[time_field].str.split(":").str[1].astype(int) * 60)
                + (stop_trips[time_field].str.split(":").str[2].astype(int))
            )

        # Filter by stop times as needed
        if start_time is not None:
            start_time_split = start_time.split(":")
            start_time_ssm = (
                int(start_time_split[0]) * 3600
                + int(start_time_split[1]) * 60
                + int(start_time_split[2])
            )
            stop_trips = stop_trips[stop_trips["_ssm"] >= start_time_ssm]

        if end_time is not None:
            end_time_split = end_time.split(":")
            end_time_ssm = (
                int(end_time_split[0]) * 3600
                + int(end_time_split[1]) * 60
                + int(end_time_split[2])
            )
            stop_trips = stop_trips[stop_trips["_ssm"] <= end_time_ssm]

        # We can now build a list showing the number of trips
        stop_trips = stop_trips[["trip_id"]]
        stop_trips["trip_count"] = 1
        # Now that we have that, we can move on to the headways
        if self.frequencies is not None:
            frequencies = self.frequencies.copy()
            frequencies.start_time = (
                (frequencies.start_time.str.split(":").str[0].astype(int) * 3600)
                + (frequencies.start_time.str.split(":").str[1].astype(int) * 60)
                + (frequencies.start_time.str.split(":").str[2].astype(int))
            )
            frequencies.end_time = (
                (frequencies.end_time.str.split(":").str[0].astype(int) * 3600)
                + (frequencies.end_time.str.split(":").str[1].astype(int) * 60)
                + (frequencies.end_time.str.split(":").str[2].astype(int))
            )
            if start_time is not None:
                frequencies["_start_time"] = (
                    int(start_time.split(":")[0]) * 3600
                    + int(start_time.split(":")[1]) * 60
                    + int(start_time.split(":")[2])
                )
                frequencies.start_time = frequencies[["start_time", "_start_time"]].max(
                    axis=1
                )
            if end_time is not None:
                frequencies["_end_time"] = (
                    int(end_time.split(":")[0]) * 3600
                    + int(end_time.split(":")[1]) * 60
                    + int(end_time.split(":")[2])
                )
                frequencies.end_time = frequencies[["end_time", "_end_time"]].min(
                    axis=1
                )
            frequencies["total_seconds"] = frequencies.end_time - frequencies.start_time
            frequencies["frequency_multiplier"] = (
                frequencies["total_seconds"] / frequencies.headway_secs
            ).astype(int)

            stop_trips = pd.merge(stop_trips, frequencies, how="inner", on="trip_id")
            stop_trips["trip_count"] = stop_trips[
                ["trip_count", "frequency_multiplier"]
            ].max(axis=1)
            stop_trips = stop_trips[["trip_id", "trip_count"]]

        unique_trips = stop_trips.drop_duplicates(subset=["trip_id"])
        return unique_trips.shape[0]

    def delete_routes(self, route_ids: list[str], clean_stops=False):
        """Delete a route with associated trips, stops, shapes, and other data

        This method removes the provided routes from the GTFS by removing all
        reference to specific route ids and the trips that the route runs.

        Parameters
        ----------
        route_ids : list[str]
            A list of routes to remove. A single route_id can also be provided
        clean_stops : bool, optional
            Whether or not to remove stops that are no longer served by the GTFS, by default False
        """
        # If just one is passed we make it a list
        if isinstance(route_ids, str):
            route_ids = list(route_ids)

        # First, we get the trips
        rm_trips = self.trips[self.trips["route_id"].isin(route_ids)]
        rm_trip_ids = rm_trips["trip_id"].tolist()

        # Get the shapes if they exist
        rm_shape_ids = None
        if self.shapes is not None:
            if "shape_id" in self.trips.columns:
                rm_shape_ids = self.trips[self.trips["trip_id"].isin(route_ids)][
                    "shape_id"
                ].tolist()

        # Remove the shapes
        if rm_shape_ids is not None:
            self.shapes = self.shapes[~self.shapes["shape_id"].isin(rm_shape_ids)]

        # Remove the stop times
        self.stop_times = self.stop_times[~self.stop_times["trip_id"].isin(rm_trip_ids)]

        # Remove from fare rules
        if self.fare_rules is not None:
            self.fare_rules = self.fare_rules[
                ~self.fare_rules["route_id"].isin(route_ids)
            ]

        # Remove from frequencies
        if self.frequencies is not None:
            self.frequencies = self.frequencies[
                ~self.frequencies["trip_id"].isin(rm_trip_ids)
            ]

        # Remove from transfer
        if self.transfers is not None:
            if "from_route_id" in self.transfers.columns:
                self.transfers = self.transfers[
                    ~self.transfers["from_route_id"].isin(route_ids)
                ]
            if "to_route_id" in self.transfers.columns:
                self.transfers = self.transfers[
                    ~self.transfers["to_route_id"].isin(route_ids)
                ]
            if "from_trip_id" in self.transfers.columns:
                self.transfers = self.transfers[
                    ~self.transfers["from_trip_id"].isin(rm_trip_ids)
                ]
            if "to_trip_id" in self.transfers.columns:
                self.transfers = self.transfers[
                    ~self.transfers["to_trip_id"].isin(rm_trip_ids)
                ]

        # Remove from attributions
        if self.attributions is not None:
            if "route_id" in self.attributions.columns:
                self.attributions = self.attributions[
                    ~self.attributions["route_id"].isin(route_ids)
                ]
            if "trip_id" in self.attributions.columns:
                self.attributions = self.attributions[
                    ~self.attributions["trip_id"].isin(rm_trip_ids)
                ]

        if clean_stops == True:
            # Now remove stops that have no visits anymore
            orphaned_stops = self.stops[
                ~self.stops["stop_id"].isin(self.stop_times["stop_id"])
            ]["stop_id"].tolist()
            self.stops = self.stops[~self.stops["stop_id"].isin(orphaned_stops)]

        # Remove from trips
        self.trips = self.trips[~self.trips["trip_id"].isin(rm_trip_ids)]

        # Remove from routes
        self.routes = self.routes[~self.routes["route_id"].isin(route_ids)]

    def write_zip(self, filepath, include_optional=True):
        """Write the current GTFS into a zipfile.

        Parameters
        ----------
        filepath : str
            The filepath to write the zip to (should be a .zip extension)
        include_optional : bool, optional
            Whether or not to include files marked optional by the GTFS spec, by default True
        """
        # Start with the required files
        self.agency.to_csv(
            filepath,
            mode="w",
            compression={"method": "zip", "archive_name": "agency.txt"},
            index=False,
        )
        self.stops.to_csv(
            filepath,
            mode="a",
            compression={"method": "zip", "archive_name": "stops.txt"},
            index=False,
        )
        self.routes.to_csv(
            filepath,
            mode="a",
            compression={"method": "zip", "archive_name": "routes.txt"},
            index=False,
        )
        self.trips.to_csv(
            filepath,
            mode="a",
            compression={"method": "zip", "archive_name": "trips.txt"},
            index=False,
        )
        self.stop_times.to_csv(
            filepath,
            mode="a",
            compression={"method": "zip", "archive_name": "stop_times.txt"},
            index=False,
        )
        if self.calendar is not None:
            self.calendar.to_csv(
                filepath,
                mode="a",
                compression={"method": "zip", "archive_name": "calendar.txt"},
                index=False,
            )
        if self.calendar_dates is not None:
            self.calendar_dates.to_csv(
                filepath,
                mode="a",
                compression={"method": "zip", "archive_name": "calendar_dates.txt"},
                index=False,
            )

        if include_optional is True:
            if self.fare_attributes is not None:
                self.fare_attributes.to_csv(
                    filepath,
                    mode="a",
                    compression={
                        "method": "zip",
                        "archive_name": "fare_attributes.txt",
                    },
                    index=False,
                )
            if self.fare_rules is not None:
                self.fare_rules.to_csv(
                    filepath,
                    mode="a",
                    compression={"method": "zip", "archive_name": "fare_rules.txt"},
                    index=False,
                )
            if self.shapes is not None:
                self.shapes.to_csv(
                    filepath,
                    mode="a",
                    compression={"method": "zip", "archive_name": "shapes.txt"},
                    index=False,
                )
            if self.frequencies is not None:
                self.frequencies.to_csv(
                    filepath,
                    mode="a",
                    compression={"method": "zip", "archive_name": "frequencies.txt"},
                    index=False,
                )
            if self.transfers is not None:
                self.transfers.to_csv(
                    filepath,
                    mode="a",
                    compression={"method": "zip", "archive_name": "transfers.txt"},
                    index=False,
                )
            if self.pathways is not None:
                self.pathways.to_csv(
                    filepath,
                    mode="a",
                    compression={"method": "zip", "archive_name": "pathways.txt"},
                    index=False,
                )
            if self.levels is not None:
                self.levels.to_csv(
                    filepath,
                    mode="a",
                    compression={"method": "zip", "archive_name": "levels.txt"},
                    index=False,
                )
            if self.attributions is not None:
                self.attributions.to_csv(
                    filepath,
                    mode="a",
                    compression={"method": "zip", "archive_name": "attributions.txt"},
                    index=False,
                )
            if self.translations is not None:
                self.translations.to_csv(
                    filepath,
                    mode="a",
                    compression={"method": "zip", "archive_name": "translations.txt"},
                    index=False,
                )
            if self.feed_info is not None:
                self.feed_info.to_csv(
                    filepath,
                    mode="a",
                    compression={"method": "zip", "archive_name": "feed_info.txt"},
                    index=False,
                )
