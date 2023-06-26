import calendar
import datetime
import json
import math
from zipfile import ZipFile

import pandas as pd

from .exceptions import (
    DateNotSetException,
    DateNotValidException,
    FeedNotValidException,
)


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


class GTFS:
    """A representation of a single static GTFS feed and associated data.

    All parameters should be valid Pandas DataFrames that follow
    the structure corresponding to the dataset as defined by the GTFS
    standard (http://gtfs.org/reference/static).

    Parameters
    ----------
    agency : `pandas.DataFrame`
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
            raise FeedNotValidException("One of calendar or calendar_dates is required.")

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

        # Set the analysis date as "date unaware"
        self.date = None

    def _load_clean_feed(filepath,dtype=None,parse_dates=False,skipinitialspace=True):
        df = pd.read_csv(
            filepath,
            dtype=dtype,
            parse_dates=parse_dates,
            skipinitialspace=skipinitialspace,
        )
        df.columns = df.columns.str.strip()
        return df
    
    @classmethod
    def load_zip(self, filepath):
        """Creates a :class:`GTFS` object from a zipfile containing the
        appropriate data.

        :param filepath: The filepath of the zipped GTFS feed.
        :type filepath: str

        :return: A :class:`GTFS` object with loaded and validated data.
        """
        with ZipFile(filepath, "r") as zip_file:
            # Deal with nested files
            filepaths = dict()
            for req in REQUIRED_FILES + OPTIONAL_FILES:
                filepaths[req] = None
            for file in zip_file.namelist():
                for req in REQUIRED_FILES + OPTIONAL_FILES:
                    if req in file:
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
            )

            if filepaths["calendar.txt"] in zip_file.namelist():
                calendar = self._load_clean_feed(
                    zip_file.open(filepaths["calendar.txt"]),
                    dtype={
                        "service_id": str,
                        "monday": bool,
                        "tuesday": bool,
                        "wednesday": bool,
                        "thursday": bool,
                        "friday": bool,
                        "saturday": bool,
                        "sunday": bool,
                        "start_date": str,
                        "end_date": str,
                    },
                    skipinitialspace=True,
                )
                calendar["start_date"] = pd.to_datetime(calendar["start_date"], format='%Y%m%d').dt.date
                calendar["end_date"] = pd.to_datetime(calendar["end_date"], format='%Y%m%d').dt.date

            else:
                calendar = None

            if filepaths["calendar_dates.txt"] in zip_file.namelist():
                calendar_dates = self._load_clean_feed(
                    zip_file.open(filepaths["calendar_dates.txt"]),
                    dtype={"service_id": str, "date": str, "exception_type": int},
                    skipinitialspace=True,
                )
                if calendar_dates.shape[0] == 0:
                    calendar_dates = None
                else:
                    calendar_dates["date"] = pd.to_datetime(calendar_dates["date"], format='%Y%m%d').dt.date
            else:
                calendar_dates = None

            if filepaths["fare_attributes.txt"] in zip_file.namelist():
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
                )
            else:
                fare_attributes = None

            if filepaths["fare_rules.txt"] in zip_file.namelist():
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
                )
            else:
                fare_rules = None

            if filepaths["shapes.txt"] in zip_file.namelist():
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
                )
            else:
                shapes = None

            if filepaths["frequencies.txt"] in zip_file.namelist():
                frequencies = self._load_clean_feed(
                    zip_file.open(filepaths["frequencies.txt"]),
                    dtype={
                        "trip_id": str,
                        "start_time": str,
                        "end_time": str,
                        "headway_secs": int,
                        "exact_times": int,
                    },
                    skipinitialspace=True,
                )
                frequencies["start_time"] = pd.to_timedelta(frequencies["start_time"])
                frequencies["end_time"] = pd.to_timedelta(frequencies["end_time"])
            else:
                frequencies = None

            if filepaths["transfers.txt"] in zip_file.namelist():
                transfers = self._load_clean_feed(
                    zip_file.open(filepaths["transfers.txt"]),
                    dtype={
                        "from_stop_id": str,
                        "to_stop_id": str,
                        "transfer_type": "Int64",
                        "min_transfer_time": "Int64",
                    },
                    skipinitialspace=True,
                )
            else:
                transfers = None

            if filepaths["pathways.txt"] in zip_file.namelist():
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
                )
            else:
                pathways = None

            if filepaths["levels.txt"] in zip_file.namelist():
                levels = self._load_clean_feed(
                    zip_file.open(filepaths["levels.txt"]),
                    dtype={"level_id": str, "level_index": float, "level_name": str},
                    skipinitialspace=True,
                )
            else:
                levels = None

            if filepaths["translations.txt"] in zip_file.namelist():
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
                )
            elif filepaths["feed_info.txt"] in zip_file.namelist():
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
                )
                translations = None
            else:
                translations = None
                feed_info = None

            if filepaths["attributions.txt"] in zip_file.namelist():
                attributions = pd.read_csv(
                    zip_file.open(filepaths["attributions.txt"]),
                    dtype={
                        "attribution_id": str,
                        "agency_id": str,
                        "route_id": str,
                        "trip_id": str,
                    },
                    skipinitialspace=True,
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

    def summary(self):
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

        :returns: A :py:mod:`pandas.Series` containing the relevant data.
        """

        summary = pd.Series(dtype=str)
        summary["agencies"] = self.agency.agency_name.tolist()
        summary["total_stops"] = self.stops.shape[0]
        summary["total_routes"] = self.routes.shape[0]
        summary["total_trips"] = self.trips.shape[0]
        summary["total_stops_made"] = self.stop_times.shape[0]
        if self.calendar is not None:
            summary["first_date"] = self.calendar.start_date.min()
            summary["last_date"] = self.calendar.end_date.max()
        else:
            summary["first_date"] = self.calendar_dates.date.min()
            summary["last_date"] = self.calendar_dates.date.max()
        if self.shapes is not None:
            summary["total_shapes"] = self.shapes.shape[0]

        return summary

    def valid_date(self, date_to_check: datetime.date):
        """Checks whether the provided date falls within the feed's date range

        Parameters
        ----------
        date : `datetime.date` or `datetime.datetime`
            A date or datetime object to be validated against the feed

        Returns
        -------
        bool
            Whether the date is valid or not.
        """

        summary = self.summary()

        first_date = summary.first_date
        last_date = summary.last_date
        if first_date > date_to_check or last_date < date_to_check:
            return False
        else:
            return True

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

        if not self.valid_date(date):
            raise DateNotValidException
            # TODO: Move this to a decorator

        dayname = date.strftime("%A").lower()

        if self.calendar is not None:
            # Get all the service_ids for the desired day of the week
            service_ids = self.calendar[
                (self.calendar[dayname] == 1) & (self.calendar.start_date <= date) & (self.calendar.end_date >= date)
            ].service_id.tolist()
            if self.calendar_dates is not None:
                # Add service ids in the calendar_dates
                service_ids.extend(
                    self.calendar_dates[
                        (self.calendar_dates.date == date) & (self.calendar_dates.exception_type == 1)
                    ].service_id.tolist()
                )
                # Remove service ids from the calendar dates
                remove_service_ids = self.calendar_dates[
                    (self.calendar_dates.date == date) & (self.calendar_dates.exception_type == 2)
                ].service_id.tolist()

                service_ids = [i for i in service_ids if i not in remove_service_ids]
        else:
            service_ids = self.calendar_dates[
                (self.calendar_dates.date == date) & (self.calendar_dates.exception_type == 1)
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
            - (int(min_dep.split(":")[0]) + int(min_dep.split(":")[1]) / 60.0 + int(min_dep.split(":")[2]) / 3600.0)
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
        route_trips = trips[["route_id", "trip_id"]].groupby("route_id", as_index=False).count()
        route_trips["trips"] = route_trips.trip_id
        first_departures = stop_times[["route_id", "departure_time"]].groupby("route_id", as_index=False).min()
        last_arrivals = stop_times[["route_id", "arrival_time"]].groupby("route_id", as_index=False).max()
        summary = pd.merge(route_trips, first_departures, on=["route_id"])
        summary = pd.merge(summary, last_arrivals, on=["route_id"])
        summary["service_time"] = (
            summary.arrival_time.str.split(":", -1, expand=True)[0].astype(int)
            + summary.arrival_time.str.split(":", -1, expand=True)[1].astype(int) / 60.0
            + summary.arrival_time.str.split(":", -1, expand=True)[2].astype(int) / 3600.0
        ) - (
            summary.departure_time.str.split(":", -1, expand=True)[0].astype(int)
            + summary.departure_time.str.split(":", -1, expand=True)[1].astype(int) / 60.0
            + summary.departure_time.str.split(":", -1, expand=True)[2].astype(int) / 3600.0
        )
        summary["average_headway"] = 60 * summary.service_time / summary.trips
        summary["last_arrival"] = summary.arrival_time
        summary["first_departure"] = summary.departure_time
        summary = summary[["route_id", "trips", "first_departure", "last_arrival", "average_headway"]]
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

        if not self.valid_date(date):
            raise DateNotValidException(f"Date falls outside of feed span: {date}")

        trips = self.date_trips(date)

        # Grab all the stop_times
        stop_times = self.stop_times[
            self.stop_times.trip_id.isin(trips.trip_id)
            & (self.stop_times.arrival_time >= start_time)
            & (self.stop_times.arrival_time <= end_time)
        ]

        grouped = (
            stop_times[["trip_id", "arrival_time"]]
            .groupby("trip_id", as_index=False)
            .agg({"arrival_time": ["max", "min"]})
        )
        grouped.columns = ["trip_id", "max", "min"]
        grouped.dropna()
        max_split = grouped["max"].str.split(":", expand=True).astype(int)
        min_split = grouped["min"].str.split(":", expand=True).astype(int)
        grouped["diff"] = (
            (max_split[0] - min_split[0]) + (max_split[1] - min_split[1]) / 60 + (max_split[2] - min_split[2]) / 3600
        )
        return grouped["diff"].sum()

    def stop_summary(self, stop_id: str, start_time: str = None, end_time: str = None) -> pd.Series:
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
            self.stop_times.trip_id.isin(trips.trip_id) & (self.stop_times.stop_id == stop_id)
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
            self.stop_times.trip_id.isin(self.date_trips(date).trip_id) & (self.stop_times.stop_id == stop_id)
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
                    for i in range(((end_date + datetime.timedelta(days=1)) - start_date).days):
                        day = calendar.day_name[(start_date + datetime.timedelta(days=i + 1)).weekday()].lower()
                        week[day] = week[day] + 1 if day in week else 1

                    # Get trips with that service id and add them to the total, only if that day is in there.
                    if dow in week.keys():
                        dist[dow] = (
                            dist[dow]
                            + week[dow] * self.trips[self.trips.service_id == service.service_id].trip_id.count()
                        )

        # Now check exceptions to add and remove
        if self.calendar_dates is not None:
            # Start by going through all the calendar dates within the date range
            # cd = self.calendar_dates.copy()
            for index, cd in self.calendar_dates[
                (self.calendar_dates["date"].dt.date >= start_date) & (self.calendar_dates["date"].dt.date <= end_date)
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

    def route_stops_inside(self, path_to_shape, format="geojson"):
        """Count the number of stops a given route has inside a geographical
        boundary or shape.

        :param path_to_shape: A path to the file containing the shapes. This
            file must contain unprojected geospatial information in WGS:84 format.
        :type path_to_shape: str
        :param format: The format of the geospatial file. **Note:** currently,
            only the default `geojson` is supported.
        :type format: str, optional
        :return: A :py:mod:`pandas.DataFrame` object listing each route and
            the number of stops served by that route that fall within the
            provided boundary.
        """

        from shapely.geometry import Point, shape, GeometryCollection

        count = 0
        # For starters, let's load a bounding box and check how many stops are in the point
        if format == "geojson":
            with open(path_to_shape) as f:
                features = json.load(f)["features"]
                boundary = GeometryCollection([shape(feature["geometry"]).buffer(0) for feature in features])
                routes = []
                counts = []
                for idx, route in self.routes.iterrows():
                    # Get all the stops on trips for that route.
                    stops = self.stop_times[
                        self.stop_times.trip_id.isin(self.trips[self.trips.route_id == route.route_id].trip_id)
                    ].stop_id.unique()
                    # NOTE: buffer(0) is a trick for fixing scenarios where polygons have overlapping coordinates
                    count = 0
                    for idx, stop in self.stops[self.stops.stop_id.isin(stops)].iterrows():
                        if Point(stop.stop_lon, stop.stop_lat).within(boundary):
                            count += 1
                    routes.append(route.route_id)
                    counts.append(count)

        stop_count = pd.DataFrame(counts, index=routes)
        return stop_count

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
        stop_times["timestamp"] = 60 * stop_times[time_field].str.split(":").str[0].astype(int) + stop_times[
            time_field
        ].str.split(":").str[1].astype(int)

        # Let's get the start_times of all trips
        trip_start_times = (
            stop_times[["trip_id", "stop_sequence", "timestamp", time_field]]
            .sort_values(["trip_id", "stop_sequence"])
            .drop_duplicates("trip_id")
        )

        # Filter out our slice
        if start_time != None:
            start_time_int = 60 * int(start_time.split(":")[0]) + int(start_time.split(":")[1])
            stop_times = stop_times[
                stop_times.trip_id.isin(trip_start_times[trip_start_times.timestamp >= start_time_int].trip_id)
            ]

        if end_time != None:
            # We need trip end times for this filter
            trip_end_times = (
                stop_times[["trip_id", "stop_sequence", "timestamp", time_field]]
                .sort_values(["trip_id", "stop_sequence"], ascending=False)
                .drop_duplicates("trip_id")
            )
            end_time_int = 60 * int(end_time.split(":")[0]) + int(end_time.split(":")[1])
            stop_times = stop_times[
                stop_times.trip_id.isin(trip_end_times[trip_end_times.timestamp <= end_time_int].trip_id)
            ]

        # Now get the trips we're working with
        trip_starts = trip_start_times[trip_start_times.trip_id.isin(stop_times.trip_id.unique())]
        trip_starts = pd.merge(trip_starts, self.trips[["trip_id", "route_id"]], on="trip_id")

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
                (trip_starts.timestamp >= slice_start_int) & (trip_starts.timestamp < slice_end_int)
            ]
            slice_group = in_slice[["route_id", "trip_id"]].groupby("route_id", as_index=False).count()
            slice_group["frequency"] = (slice_group["trip_id"] * (60.0 / interval)).astype(int)
            final = pd.merge(template_df, slice_group, on="route_id", how="left").fillna(0)
            final["frequency"] = final["frequency"].astype(int)
            final["trips"] = final["trip_id"].astype(int)
            final["bin_start"] = slice_start_str
            final["bin_end"] = slice_end_str
            slices.append(final[["route_id", "bin_start", "bin_end", "trips", "frequency"]])

        # Assemble final matrix
        mx = pd.concat(slices, axis="index")
        return mx.reset_index(drop=True)

    def trips_at_stops(
        self,
        stop_ids: list,
        date: datetime.date,
        start_time: str = None,
        end_time: str = None,
        time_field: str = "arrival_time",
    ) -> pd.DataFrame:
        """Get a set of unique trips that visit a given set of stops

        This function returns a subset of the trips table which include trips
        that stop at _any_ of the stops provided.

        Parameters
        ----------
        stop_ids : list
            A list of stop_ids to check
        date : `datetime.date`
            The service day to check
        start_time : str, optional
            A string representation (HH:MM:SS) of the number of hours since
            midnight on the analysis date. Can be greater than 24:00:00.
            A None value will consider all trips from the start of the
            service day, by default None
        end_time : str, optional
            A string representation (HH:MM:SS) of the number of hours since
            midnight on the analysis date. Can be greater than 24:00:00.
            A None value will consider all trips through the end of the
            service day, by default None
        time_field : str, optional
            The name of the time column in `stop_times` to consider, either 'arrival_time' or
            'departure_time'. By default 'arrival_time'

        Notes
        -----
            Not all GTFS datasets include arrival and/or departure times for
            every stop. In cases where times are only set at time points, and
            no interpolation is provided, this function will not work.
        """

        # Start by filtering all stop_trips by the given dateslice
        trips = self.date_trips(date)

        stop_ids = [str(s) for s in stop_ids]

        # Now let's grab the stop_trips
        stop_trips = self.stop_times[
            (self.stop_times.trip_id.isin(trips.trip_id))
            & (self.stop_times.stop_id.isin(stop_ids))
            & (self.stop_times.arrival_time >= start_time)
            & (self.stop_times.arrival_time <= end_time)
        ]

        # Now we can filter by stop times as needed

        # We've got the stop times, so let's grab the unique trips
        unique_trips = stop_trips.trip_id.unique()

        return self.trips[self.trips.trip_id.isin(unique_trips)]
