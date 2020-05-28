from zipfile import ZipFile
import datetime

import pandas as pd

class DateNotValidException(Exception):
    pass

class FeedNotValidException(Exception):
    pass

class GTFS:
    def __init__(self, agency, stops, routes, trips, stop_times, 
        calendar=None, calendar_dates=None, fare_attributes=None, 
        fare_rules=None, shapes=None, frequencies=None, transfers=None, 
        pathways=None, levels=None, translations=None, feed_info=None, 
        attributions=None):
        # Mandatory Files
        self.agency = agency
        self.stops = stops 
        self.routes = routes
        self.trips = trips 
        self.stop_times = stop_times

        # Conditionally Mandatory Files
        self.calendar = calendar
        self.calendar_dates = calendar_dates

        # Pairwise Mandatory Files
        self.translations = translations
        self.feed_into = feed_info

        # Optional Files
        self.fare_attributes = fare_attributes
        self.fare_rules = fare_rules
        self.shapes = shapes
        self.frequencies = frequencies
        self.transfers = transfers
        self.pathways = pathways
        self.levels = levels
        self.attributions = attributions


    @staticmethod
    def load_zip(filepath):
        with ZipFile(filepath, 'r') as zip_file:
            # Create pandas objects of the entire feed
            agency = pd.read_csv(
                zip_file.open("agency.txt"),
                dtype={
                    'agency_id': str, 'agency_name': str, 'agency_url': str,
                    'agency_timezone': str, 'agency_lang': str,
                    'agency_phone': str, 'agency_fare_url': str,
                    'agency_email': str
                }
            )
            stops = pd.read_csv(
                zip_file.open("stops.txt"),
                dtype={
                    'stop_id': str, 'stop_code': str, 'stop_name': str,
                    'stop_desc': str, 'stop_lat': float, 'stop_lon': float,
                    'zone_id': str, 'stop_url': str, 'location_type': 'Int64',
                    'parent_station': str, 'stop_timezone': str,
                    'wheelchair_boarding': 'Int64', 'level_id': str,
                    'platform_code': str
                }
            )
            routes = pd.read_csv(
                zip_file.open("routes.txt"),
                dtype={
                    'route_id': str, 'agency_id': str, 'route_short_name': str,
                    'route_long_name': str, 'route_desc': str,
                    'route_type': int, 'route_url': str, 'route_color': str,
                    'route_text_color': str, 'route_short_order': int
                }
            )
            trips = pd.read_csv(
                zip_file.open("trips.txt"),
                dtype={
                    'route_id': str, 'service_id': str, 'trip_id': str,
                    'trip_headsign': str, 'trip_short_name': str,
                    'direction_id': int, 'block_id': str, 'shape_id': str,
                    'wheelchair_accessible': 'Int64', 'bikes_allowed': 'Int64'
                })
            stop_times = pd.read_csv(
                zip_file.open("stop_times.txt"),
                dtype={
                    'trip_id': str, 'arrival_time': str, 'departure_time': str,
                    'stop_id': str, 'stop_sequence': int, 'stop_headsign': str,
                    'pickup_type': 'Int64', 'drop_off_type': 'Int64', 
                    'shape_dist_traveled': float, 'timepoint': 'Int64'
                },
            )
            # v = stop_times.arrival_time.str.split(':', expand=True).astype(int)
            # stop_times['arrival_time'] = pd.to_timedelta(v[0], unit='h') + pd.to_timedelta(v[1], unit='m') + pd.to_timedelta(v[2], unit='s')

            # v = stop_times.departure_time.str.split(':', expand=True).astype(int)
            # stop_times['departure_time'] = pd.to_timedelta(v[0], unit='h') + pd.to_timedelta(v[1], unit='m') + pd.to_timedelta(v[2], unit='s')

            if "calendar.txt" in zip_file.namelist():
                calendar = pd.read_csv(
                    zip_file.open("calendar.txt"), 
                    dtype={
                        'service_id': str,'monday': bool, 'tuesday': bool,
                        'wednesday': bool, 'thursday': bool, 'friday': bool,
                        'saturday': bool, 'sunday': bool, 'start_date': str,
                        'end_date': str
                    },
                    parse_dates=['start_date', 'end_date']
                )

            else:
                calendar = None

            if "calendar_dates.txt" in zip_file.namelist():
                calendar_dates = pd.read_csv(
                    zip_file.open("calendar_dates.txt"),
                    dtype={
                        'service_id': str, 'date': str, 'exception_type': int
                    },
                    parse_dates=['date']
                )
            else:
                calendar_dates = None

            if "fare_attributes.txt" in zip_file.namelist():
                fare_attributes = pd.read_csv(
                    zip_file.open("fare_attributes.txt"),
                    dtype={
                        'fare_id': str, 'price': float, 'currency_type': str,
                        'payment_method': int, 'transfers': 'Int64',
                        'agency_id': str, 'transfer_duration': int
                    }
                )
            else:
                fare_attributes = None

            if "fare_rules.txt" in zip_file.namelist():
                fare_rules = pd.read_csv(
                    zip_file.open("fare_rules.txt"),
                    dtype={
                        'fare_id': str, 'route_id': str, 'origin_id': str,
                        'destination_id': str, 'contains_id': str
                    }    
                )
            else:
                fare_rules = None
            
            if "shapes.txt" in zip_file.namelist():
                shapes = pd.read_csv(
                    zip_file.open("shapes.txt"),
                    dtype={
                        'shape_id': str, 'shape_pt_lat': float,
                        'shape_pt_lon': float, 'shape_pt_sequence': int,
                        'shape_dist_traveled': float
                    }
                )
            else:
                shapes = None

            if "frequencies.txt" in zip_file.namelist():
                frequencies = pd.read_csv(
                    zip_file.open("frequencies.txt"),
                    dtype={
                        'trip_id': str, 'start_time': str, 'end_time': str,
                        'headway_secs': int, 'exact_times': int
                    },
                    parse_dates=['start_time', 'end_time']
                )
            else:
                frequencies = None

            if "transfers.txt" in zip_file.namelist():
                transfers = pd.read_csv(
                    zip_file.open("transfers.txt"),
                    dtype={
                        'from_stop_id': str, 'to_stop_id': str,
                        'transfer_type': 'Int64', 'min_transfer_time': 'Int64'
                    }
                )
            else:
                transfers = None

            if "pathways.txt" in zip_file.namelist():
                pathways = pd.read_csv(
                    zip_file.open("pathways.txt"),
                    dtype={
                        'pathway_id': str, 'from_stop_id': str, 
                        'to_stop_id': str, 'pathway_mode': int,
                        'is_bidirectional': str, 'length': 'float64',
                        'traversal_time': 'Int64', 'stair_count': 'Int64',
                        'max_slope': 'float64', 'min_width': 'float64',
                        'signposted_as': str, 'reverse_signposted_as': str
                    }
                )
            else:
                pathways = None
            
            if "levels.txt" in zip_file.namelist():
                levels = pd.read_csv(
                    zip_file.open("levels.txt"),
                    dtype={
                        'level_id': str, 'level_index': float,
                        'level_name': str
                    }
                )
            else:
                levels = None

            if "translations.txt" in zip_file.namelist():
                translations = pd.read_csv(
                    zip_file.open("translations.txt"),
                    dtype={
                        'table_name': str, 'field_name': str, 'language': str,
                        'translation': str, 'record_id': str,
                        'record_sub_id': str, 'field_value': str
                    }
                )
                feed_info = pd.read_csv(
                    zip_file.open("feed_info.txt"),
                    dtype={
                        'feed_publisher_name': str, 'feed_publisher_url': str,
                        'feed_lang': str, 'default_lang': str,
                        'feed_start_date': str, 'feed_end_date': str,
                        'feed_version': str, 'feed_contact_email': str,
                        'feed_contact_url': str
                    }
                )
            elif "feed_info.txt" in zip_file.namelist():
                feed_info = pd.read_csv(
                    zip_file.open("feed_info.txt"),
                    dtype={
                        'feed_publisher_name': str, 'feed_publisher_url': str,
                        'feed_lang': str, 'default_lang': str,
                        'feed_start_date': str, 'feed_end_date': str,
                        'feed_version': str, 'feed_contact_email': str,
                        'feed_contact_url': str
                    }   
                )
                translations=None
            else:
                translations = None
                feed_info = None

            if "attributions.txt" in zip_file.namelist():
                attributions = pd.read_csv(
                    zip_file.open("attributions.txt"),
                    dtype={
                        'attribution_id': str, 'agency_id': str, 
                        'route_id': str, 'trip_id': str,
                    }
                )
            else:
                attributions = None

        return GTFS(agency, stops, routes, trips, stop_times, 
            calendar=calendar, calendar_dates=calendar_dates, 
            fare_attributes=fare_attributes, fare_rules=fare_rules, 
            shapes=shapes, frequencies=frequencies, transfers=transfers,
            pathways=pathways, levels=levels, translations=translations, 
            feed_info=feed_info, attributions=attributions)
    
    def summary(self):
        # Return a summary of the data in a pandas dataframe
        summary = pd.Series()
        summary['agencies'] = self.agency.agency_name.tolist()
        summary['total_stops'] = self.stops.shape[0]
        summary['total_routes'] = self.routes.shape[0]
        summary['total_trips'] = self.trips.shape[0]
        summary['total_stops_made'] = self.stop_times.shape[0]
        if self.calendar is not None:
            summary['first_date'] = self.calendar.start_date.min()
            summary['last_date'] = self.calendar.end_date.max()
        else:
            summary['first_date'] = self.calendar_dates.date.min()
            summary['last_date'] = self.calendar_dates.date.max()
        if self.shapes is not None:
            summary['total_shapes'] = self.shapes.shape[0]

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
        trips = self.day_trips(datestring)
        if "direction_id" in trips.columns: 
            trips = trips[trips.direction_id == 0]
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
        summary = summary[["route_id", "trips", "first_departure", "last_arrival", "average_headway"]]
        summary = pd.merge(self.routes, summary, on="route_id", how="inner")
        return summary

    def compare_by_route(self, other):
        """Compare one GTFS feed to another
        
        TODO: Add description
        """
        #Only keep what doesn't match
        
        # Let's group by route
        this_trips = self.trips[['trip_id', 'route_id']].groupby('route_id', as_index=False).count()
        this_trips_routes = pd.merge(this_trips, self.routes[['route_id', 'route_short_name']], on='route_id')
        other_trips = other.trips[['trip_id', 'route_id']].groupby('route_id', as_index=False).count()
        other_trips_routes = pd.merge(other_trips, other.routes[['route_id', 'route_short_name']], on='route_id')
        combined = pd.merge(this_trips_routes, other_trips_routes, on='route_short_name')
        combined.columns = ['this_route_id', 'this_trips', 'route_short_name', 'other_route_id', 'other_trips']
        combined['difference'] = combined['other_trips'] - combined['this_trips']
        combined['pct_difference'] = 100.0*(combined['other_trips'] - combined['this_trips'])/combined['this_trips']
        combined.to_csv('ttc_difference.csv', index=False)
        print(combined)
    
    def service_hours(self, date, start_time, end_time):
        """Get the total service hours in a specified date and time slice"""

        # First, we need to get the service_ids that apply.
        service_ids = []
        start = start_time.strftime("%H:%M:%S")
        end = end_time.strftime("%H:%M:%S")
        # Start with the calendar
        if self.calendar is not None:
            dow = date.strftime("%A").lower()
            service_ids.extend(self.calendar[(self.calendar[dow] == 1) & (self.calendar.start_date.dt.date <= date) & (self.calendar.end_date.dt.date >= date)].service_id.tolist())
        
        # Now handle exceptions if they are there
        if self.calendar_dates is not None:
            to_add = service_ids.extend(self.calendar_dates[(self.calendar_dates.date.dt == date) & self.calendar_dates.exception_type == 1].service_id.tolist())
            to_del = service_ids.extend(self.calendar_dates[(self.calendar_dates.date.dt == date) & self.calendar_dates.exception_type == 2].service_id.tolist())
            if to_add is not None:
                service_ids.extend(to_add)
            # Remove those that should be removed
            if to_del is not None:
                [service_ids.remove(i) for i in to_del]
        
        # Grab all the trips
        trips = self.trips[self.trips.service_id.isin(service_ids)]
        # Grab all the stop_times
        stop_times = self.stop_times[self.stop_times.trip_id.isin(trips.trip_id) & (self.stop_times.arrival_time >= start) & (self.stop_times.arrival_time <= end)]
        grouped = stop_times[['trip_id', 'arrival_time']].groupby('trip_id', as_index=False).agg({'arrival_time': ['max', 'min']})
        grouped.columns = ['trip_id', 'max', 'min']
        grouped.dropna()
        max_split = grouped['max'].str.split(":", expand=True).astype(int)
        min_split = grouped['min'].str.split(":", expand=True).astype(int)
        grouped['diff'] = (max_split[0] - min_split[0]) + (max_split[1] - min_split[1])/60 + (max_split[2] - min_split[2])/3600
        # print(grouped.head(40))
        return(grouped['diff'].sum())
        # service_ids = self.calendar[self.calendar[dow] == 1].service_id

        # print(self.stop_times.head())
