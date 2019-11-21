from zipfile import ZipFile
import datetime

import pandas as pd

class DateNotValidException(Exception):
    pass

class GTFS:
    def __init__(self, agency, stops, routes, trips, stop_times, calendar=None, calendar_dates=None, fare_attributes=None, fare_rules=None, shapes=None):
        # Mandatory Files
        self.agency = agency
        self.stops = stops 
        self.routes = routes
        self.trips = trips 
        self.stop_times = stop_times

        # Conditionally Mandatory Files
        self.calendar = calendar
        self.calendar_dates = calendar_dates

        # Optional Files
        self.fare_attributes = fare_attributes
        self.fare_rules = fare_rules
        self.shapes = shapes


    @staticmethod
    def load_zip(filepath):
        with ZipFile(filepath, 'r') as zip_file:
            # Read into pandas file?
            agency = pd.read_csv(zip_file.open("agency.txt"))
            stops = pd.read_csv(zip_file.open("stops.txt"))
            routes = pd.read_csv(zip_file.open("routes.txt"))
            trips = pd.read_csv(zip_file.open("trips.txt"))
            stop_times = pd.read_csv(zip_file.open("stop_times.txt"))
            if "calendar.txt" in zip_file.namelist():
                calendar = pd.read_csv(zip_file.open("calendar.txt"))
            else:
                calendar = None
            if "calendar_dates.txt" in zip_file.namelist():
                calendar_dates = pd.read_csv(zip_file.open("calendar_dates.txt"))
            else:
                calendar_dates = None
        
        return GTFS(agency, stops, routes, trips, stop_times, calendar=calendar, calendar_dates=calendar_dates)
    
    def summary(self):
        # Return a summary of the data in a pandas dataframe
        # TODO: Specify column data types for faster loading
        summary = pd.Series()
        summary['agencies'] = self.agency.agency_name.tolist()
        summary['total_stops'] = len(self.stops.index)
        summary['total_routes'] = len(self.routes.index)
        summary['total_trips'] = len(self.trips.index)
        summary['total_stops_made'] = len(self.stop_times.index)
        if self.calendar is not None:
            summary['first_date'] = self.calendar.start_date.min()
            summary['last_date'] = self.calendar.end_date.max()
        else:
            summary['first_date'] = self.calendar_dates.date.min()
            summary['last_date'] = self.calendar_dates.date.max()

        return summary

    def valid_date(self, datestring):
        summary = self.summary()
        if summary.first_date > int(datestring) or summary.last_date < int(datestring):
            return False
        else:
            return True

    def day_trips(self, datestring):
        # Gets all trips on a specified day, counting for exceptions
        # First, get all standard trips that run on that particular day of the week
        if not self.valid_date(datestring):
            raise DateNotValidException

        dayname = datetime.datetime.strptime(str(datestring), "%Y%m%d").strftime("%A").lower()
        if self.calendar is not None:
            service_ids = self.calendar[(self.calendar[dayname] == 1) & (self.calendar.start_date <= int(datestring)) & (self.calendar.end_date >= int(datestring))].service_id
            service_ids = service_ids.append(self.calendar_dates[(self.calendar_dates.date == int(datestring)) & (self.calendar_dates.exception_type == 1)].service_id)
            service_ids = service_ids[~service_ids.isin(self.calendar_dates[(self.calendar_dates.date == int(datestring)) & (self.calendar_dates.exception_type == 2)].service_id)]
        else:
            service_ids = self.calendar_dates[(self.calendar_dates.date == int(datestring)) & (self.calendar_dates.exception_type == 1)].service_id
        return self.trips[self.trips.service_id.isin(service_ids)]

    def stop_summary(self, datestring, stop_id):
        # Create a summary of stops for a given stop_id

        trips = self.day_trips(datestring)
        stop_times = self.stop_times[self.stop_times.trip_id.isin(trips.trip_id) & (self.stop_times.stop_id == stop_id)]

        summary = self.stops[self.stops.stop_id == stop_id].iloc[0]
        summary['total_visits'] = len(stop_times.index)
        summary['first_arrival'] = stop_times.arrival_time.min()
        summary['last_departure'] = stop_times.departure_time.max()
        summary['service_time'] = (int(summary.last_departure.split(":")[0]) + int(summary.last_departure.split(":")[1])/60.0 + int(summary.last_departure.split(":")[2])/3600.0) - (int(stop_times.arrival_time.min().split(":")[0]) + int(stop_times.arrival_time.min().split(":")[1])/60.0 + int(stop_times.arrival_time.min().split(":")[2])/3600.0)
        summary['average_headway'] = (summary.service_time/summary.total_visits)*60
        return summary

    def route_summary(self, datestring, route_id):
        # Create a route summary

        trips = self.day_trips(datestring)
        trips = trips[trips.route_id == route_id]
        stop_times = self.stop_times[self.stop_times.trip_id.isin(trips.trip_id)]
        summary = pd.Series()
        summary['route_id'] = route_id
        summary['total_trips'] = len(trips.index)
        summary['first_departure'] = stop_times.departure_time.min()
        summary['last_arrival'] = stop_times.arrival_time.max()
        summary['service_time'] = (int(summary.last_arrival.split(":")[0]) + int(summary.last_arrival.split(":")[1])/60.0 + int(summary.last_arrival.split(":")[2])/3600.0) - (int(summary.first_departure.split(":")[0]) + int(summary.first_departure.split(":")[1])/60.0 + int(summary.first_departure.split(":")[2])/3600.0)
        stop_id = stop_times.iloc[0].stop_id
        min_dep = stop_times[stop_times.stop_id == stop_id].departure_time.min()
        max_arr = stop_times[stop_times.stop_id == stop_id].arrival_time.max()
        visits = stop_times[stop_times.stop_id == stop_id].trip_id.count()
        route_headway = int(max_arr.split(":")[0]) + int(max_arr.split(":")[1])/60.0 + int(max_arr.split(":")[2])/3600.0 - (int(min_dep.split(":")[0]) + int(min_dep.split(":")[1])/60.0 + int(min_dep.split(":")[2])/3600.0)
        summary['average_headway'] = 60*route_headway/visits
        return summary
    
    def routes_summary(self, datestring):
        # Get all the routes that run in that date
        # Start with the services ID
        trips = self.day_trips(datestring)
        stop_times = self.stop_times[self.stop_times.trip_id.isin(trips.trip_id)]
        stop_times = pd.merge(stop_times, trips, on="trip_id", how="left")
        route_trips = trips[["route_id", "trip_id"]].groupby("route_id", as_index=False).count()
        route_trips['trips'] = route_trips.trip_id
        first_departures = stop_times[["route_id", "departure_time"]].groupby("route_id", as_index=False).min()
        last_arrivals = stop_times[["route_id", "arrival_time"]].groupby("route_id", as_index=False).max()
        summary = pd.merge(route_trips, first_departures, on=["route_id"])
        summary = pd.merge(summary, last_arrivals, on=["route_id"])
        summary['service_time'] = (summary.arrival_time.str.split(":", -1, expand=True)[0].astype(int) + summary.arrival_time.str.split(":", -1, expand=True)[1].astype(int)/60.0 + summary.arrival_time.str.split(":", -1, expand=True)[2].astype(int)/3600.0) - \
            (summary.departure_time.str.split(":", -1, expand=True)[0].astype(int) + summary.departure_time.str.split(":", -1, expand=True)[1].astype(int)/60.0 + summary.departure_time.str.split(":", -1, expand=True)[2].astype(int)/3600.0)
        summary['average_headway'] = 60*summary.service_time/summary.trips
        summary["last_arrival"] = summary.arrival_time 
        summary["first_departure"] = summary.departure_time     
        return summary[["route_id", "trips", "first_departure", "last_arrival", "average_headway"]]