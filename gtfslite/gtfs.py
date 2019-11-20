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

        return summary

    def day_trips(self, datestring):
        # Gets all trips on a specified day, counting for exceptions
        # First, get all standard trips that run on that particular day of the week
        dayname = datetime.datetime.strptime(str(datestring), "%Y%m%d").strftime("%A").lower()
        if self.calendar is not None:
            service_ids = self.calendar[(self.calendar[dayname] == 1) & (self.calendar.start_date <= int(datestring)) & (self.calendar.end_date) >= int(datestring)].service_id
            service_ids = service_ids.append(self.calendar_dates[(self.calendar_dates.date == int(datestring)) & (self.calendar_dates.exception_type == 1)].service_id)
            service_ids = service_ids[~service_ids.isin(self.calendar_dates[(self.calendar_dates.date == int(datestring)) & (self.calendar_dates.exception_type == 2)].service_id)]
        else:
            service_ids = service_ids.append(self.calendar_dates[(self.calendar_dates.date == int(datestring)) & (self.calendar_dates.exception_type == 1)].service_id)
        
        return self.trips[self.trips.service_id.isin(service_ids)]
    
    def weekday_trips(self, week_of):
        # TODO: May only be a standard service
        # Start by grabbing the regular service ID for those that have Monday-Friday trips
        service_ids = self.calendar[(self.calendar.monday > 0) | 
                    (self.calendar.tuesday > 0) | 
                    (self.calendar.wednesday > 0) |
                    (self.calendar.thursday > 0) | 
                    (self.calendar.friday > 0)].service_id

        # Now add in the added service
        if self.calendar_dates is not None:
            service_ids = service_ids.append(self.calendar_dates[(pd.to_datetime(self.calendar_dates.date, format="%Y%m%d").dt.weekday < 5) 
                    & (self.calendar_dates.exception_type == 1)].service_id)

        return self.trips[self.trips.service_id.isin(service_ids)]