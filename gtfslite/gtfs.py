from zipfile import ZipFile
import datetime

import pandas as pd

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
        summary = pd.DataFrame()
        summary['agencies'] = self.agency.agency_name.tolist()
        summary['total_stops'] = len(self.stops.index)
        summary['total_routes'] = len(self.routes.index)
        summary['total_trips'] = len(self.trips.index)
        summary['total_stops_made'] = len(self.stop_times.index)
        if self.calendar is not None:
            summary['first_date'] = self.calendar.start_date.min()
            summary['last_date'] = self.calendar.end_date.max()

        return summary.iloc[0]

    def day_trips(self, datestring):
        # Gets all trips on a specified day, counting for exceptions
        # First, get all standard trips that run on that particular day of the week
        dayname = datetime.datetime.strptime(str(datestring), "%Y%m%d").strftime("%A").lower()
        if self.calendar is not None:
            service_ids = self.calendar[(self.calendar[dayname] == 1) & (self.calendar.start_date <= int(datestring)) & (self.calendar.end_date >= int(datestring))].service_id
            service_ids = service_ids.append(self.calendar_dates[(self.calendar_dates.date == int(datestring)) & (self.calendar_dates.exception_type == 1)].service_id)
            service_ids = service_ids[~service_ids.isin(self.calendar_dates[(self.calendar_dates.date == int(datestring)) & (self.calendar_dates.exception_type == 2)].service_id)]
        else:
            service_ids = service_ids.append(self.calendar_dates[(self.calendar_dates.date == int(datestring)) & (self.calendar_dates.exception_type == 1)].service_id)
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