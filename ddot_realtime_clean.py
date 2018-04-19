from __future__ import division

import utils as u
import pandas as pd

from zipfile import ZipFile
from collections import namedtuple
from stop_dict_generator import get_stop_dict
from utils import get_service_day
import datetime, time,os

#### Named Tuples ####
CalException = namedtuple('CalendarException','service_id exception_type')
Service = namedtuple('CalendarService', 'monday tuesday wednesday thursday friday saturday sunday start_date end_date')
Freq = namedtuple('Freq', 'start_time end_time headway_secs')
Trip = namedtuple('Trip', 'route_id service_id direction_id')
TripFreq = namedtuple('TripFreq', 'route_id service_id direction_id start_time end_time headway_secs')

ServiceClass = namedtuple('NextBusServiceClass', 'monday tuesday wednesday thursday friday saturday sunday')

VehiclePos = namedtuple('VehiclePositionData', 'vehicle_id, route_id, direction_id, dirtag,  lat, lon, timestamp, bearing, speed, odometer')

StopInfo = namedtuple('stopinfo', 'stop_name, lat, lon')

VehicleNew = namedtuple('VehicleData', 'vehicle_id, route_id, lat, lon, timestamp, minsLate, nextStopID, tripID')

#### GLOBAL VARIABLES ####

webloc = 'file location'

nextbus_url = 'next url' #NextBus JSON feed link

path = os.getcwd()
df = pd.read_csv(path+"/new_stop.csv")[['route','direction','gtfs_id','stopID']]
'''
stop_id_dict={}
for index, row in df.iterrows():
    stop_id_dict[int(row["stopID"])]=[int(row["gtfs_id"]),row["direction"]]
#stop_id_dict[113328]=[21]: [1001242, 'w']
'''
agencydict = {
    'dc-circulator': ['yellow','green','blue','turquoise','orange','red'],
    'dc-streetcar' : ['red']
    }

route_id_to_NB_tag = { #Circulator route colours mapped onto NextBus Circulator route tags
    "dc-circulator":
        {
        "yellow": "yellow",
        "green": "green",
        "blue": "blue",
        "turquoise":"rosslyn",
        "orange": "potomac",
        "red": "mall"
        },
                    
    "dc-streetcar":
        {
        "red": "h_route"
        }
    }

NB_tag_to_route_id = {
    "yellow": "Yellow",
    "green": "Green",
    "blue": "Blue",
    "rosslyn":"Turquoise",
    "potomac": "Orange",
    "mall": "Red",
    "h_route": "Red"
    }

route_id_to_code = {
    "dc-circulator":
        {
        "yellow": "GT-US",
        "green": "WP-AM",
        "blue": "US-NY",
        "turquoise":"RS-DP",
        "orange": "PS",
        "red": "NM"
        },
                    
    "dc-streetcar":
        {
            "red": "H-BNNG"
            }
    }

dir_id_dict = {
    "dc-circulator": 
        {
        'blues': 0,
        'bluen': 1,
        'turquoisee': 0,
        'turquoisew': 1,
        'yelloww': 0,
        'yellowe': 1,
        'greenn': 1,
        'greens': 0,
        'orangew': 0,
        'orangee': 1,
        'redw': 0,
        'rede': 1
            },
    "dc-streetcar":
        {
        'redw': 1,
        'rede': 0
            }
    }

serviceClass = {
    'dc-circulator': ['wkd','wkd','wkd','wkd','fri','sat','sun'],
    'dc-streetcar' : ['mtwth','mtwth','mtwth','mtwth','f','sat','sun']
    }

direction_dict = dir_id_dict['dc-circulator']

routes_dict= {11318: "Yellow",#u'GT-US',
              11319: "Green",#u'WP-AM',
              11320: "Blue",#u'US-NY',
              11321: "Turquoise",#u'RS-DP',
              11322: "Orange",#u'PS',
              11323: "Red"}#"u'NM'}

####get dictionary for stop since unchanged, just need to run one time
stopdic_loc = {}
stopdic_tag = {}
for agency in agencydict.keys():
    #stop_dic[agency]=get_stop_dict(agency)
    stopdic_loc[agency], stopdic_tag[agency] = get_stop_dict(agency)

#######get schedule dictionary, since unchanged, just need to run one time
def get_schedule():
    schedule={}
    for a in route_id_to_NB_tag:
        schedule[a]={}
        for b in route_id_to_NB_tag[a]:
            schedule[a][route_id_to_NB_tag[a][b]]={}
            url_schedule = nextbus_url + 'schedule&a=' + a +'&r=' + route_id_to_NB_tag[a][b]
            json_schedule = u.get_json_from_url(url_schedule).get('route')
            for i in json_schedule:
                if i['serviceClass'] not in schedule[a][route_id_to_NB_tag[a][b]]:
                    schedule[a][route_id_to_NB_tag[a][b]][i['serviceClass']]={}
                schedule[a][route_id_to_NB_tag[a][b]][i['serviceClass']][i['direction']]={}
                for k in i.get('tr'):
                    if k.get('blockID') not in schedule[a][route_id_to_NB_tag[a][b]][i['serviceClass']][i['direction']]:
                        schedule[a][route_id_to_NB_tag[a][b]][i['serviceClass']][i['direction']][k.get('blockID')]=[k.get('stop')[0].get('content')]
                    else:
                        schedule[a][route_id_to_NB_tag[a][b]][i['serviceClass']][i['direction']][k.get('blockID')].append(k.get('stop')[0].get('content'))
    return schedule      


schedule=get_schedule()


#### FUNCTIONS ####
def get_nextbus_pred(agency, route, stop):
    """
    Return a dictionary of NextBus arrival predictions given  
    the desired agency id, NB route tag, and NB stop tag.
    :param agency: a str NextBus identifier of a transit agency
    :param route: a str NextBus identifier of a transit route for given agency
    :param stop: a str NextBus identifier of a stop/station on given route
    :returns a dict representation of the JSON feed, listing arrival predictions 
             for the given stop / vehicles on given route. 
    """
    url = nextbus_url + 'predictions&a=' + agency + '&r=' + route + '&s=' + stop
    return u.get_json_from_url(url)

def get_multiplestop_pred(agency,route,stop_list):
    """
    since fetching each url takes time, in order to shorten time for each itteration,
    it's helpful to fetch prediction of several stops at the same time
    this function is to generate the url for multiple stop prediction
    return a dictionary of NextBus arrival predictions given desired agency id, NB route tag, and stop list
    :param agency: a str NextBus identifier of a transit agency
    :param route: a str NextBus identifier of a transit route for given agency
    :param stop_list: a list of stop from route
    :returns a dict representation of the JSON feed, listing arrival predictions 
             for the given stop / vehicles on given route. 
    """
    url = nextbus_url + 'predictionsForMultiStops&a=' + agency 
    for stop in stop_list:
        if stop not in stopdic_tag.get(agency).get(route).keys():
            continue
        stop_tag = stopdic_tag.get(agency).get(route).get(stop)
        url = url+'&stops='+route+'|'+stop_tag

    return u.get_json_from_url(url)

def get_gtfs_df(webloc, archivename, filename):
    """
    Return a Pandas CSV DataFrame object of filename.txt,
    located within the .zip archive, with name
    archivename.zip located within webloc folder.
    :param webloc: str, location of GTFS .zip archive
    :param archivename: str, name of .zip archive, without '.zip'.
    :param filename: str, name of .txt file inside archive, without '.txt'. Must be one of the GTFS .txt files.
    :returns a pandas DateFrame object containing what's in the comma-separated txt/csv.
    """
    archive = ZipFile(webloc+"\\"+archivename+".zip")
    f = archive.open(filename+".txt")
    return pd.io.parsers.read_csv(f)  # @UndefinedVariable because PyDev can't read submodule

def get_day_trips(agency):
    """
    Returns a dict where key is trip id active today and value is a tuple
    concatenation of tuple dict value generated by get_freq_dict and get_trips_dict.
    Assumes route is active in both directions if scheduled as such.
    
    @attention: Call before and pass it as an argument to get_trip_id to save time.
    
    :param agency: a str identifying the transit agency. Assumes a agency.zip GTFS archive.
    :param day: an int representing date, in the format yyyymmdd.
    :returns a dict of tuples representing trips active today.
    """
    day = get_service_day()

    d = {}
    activeservice = get_services(agency, day, exp=True)
    freqs = get_freq_dict(agency)
    trips = get_trips_dict(agency)
    # freqs[row['trip_id']] = (row['start_time'],row['end_time'],row['headway_secs'])
    # trips[row['trip_id']] = (row['route_id'],row['service_id'],row['direction_id'],row['trip_headsign'])
    
    for trip_id, ttup in trips.iteritems():
        if ttup.service_id in activeservice:
            tup = TripFreq(*(ttup + freqs[trip_id]))
            d[trip_id] = tup

    return d
    

def get_trips_dict(agency):
    """
    Read the trips.txt file and return a dict where
    the key is the trip_id and the value is a tuple with
    some of the fields in the file, relating to route, service_id
    and direction.
    
    :param agency: a str identifying the transit agency. Assumes a agency.zip GTFS archive.
    :returns a dict of tuples, representing *all* trips for this transit agency's GTFS.
    """ 
    dicto = {}
    trips = get_gtfs_df(webloc, agency, 'trips')
    # trips.txt header row:
    # route_id,service_id,trip_id,trip_headsign,shape_id,direction_id,wheelchair_accessible,bikes_allowed
    for _, row in trips.iterrows():
        dicto[int(row['trip_id'])] = Trip(row['route_id'],row['service_id'],int(row['direction_id']))

    return dicto

def get_freq_dict(agency):
    """
    Read the frequencies.txt file and return a dict where the
    trip_id is the key and the value is a tuple of start and end time
    for that trip. Frequency is constant at 10 mins for Circulator.
    
    :param agency: a str identifying the transit agency. Assumes a agency.zip GTFS archive.
    :returns a dict of tuples, representing trip times/frequency information for *all* trips in this transit agency's GTFS.
    """ 
    freqs = {}
    freq = get_gtfs_df(webloc, agency, 'frequencies')
    # frequencies header:
    # trip_id,start_time,end_time,headway_secs,exact_times
    for _, row in freq.iterrows():
        freqs[row['trip_id']] = Freq(row['start_time'],row['end_time'],row['headway_secs'])
    return freqs

def get_services(agency, day=None, exp=True):
    """
    Read the calendar.txt file and return a dict where the key
    is the service_id and the value is a tuple with the rest of the
    fields. If today is provided, will filter them and return only
    those service_id's active today. If exp = True (default), accounts
    for service exceptions included in calendar_dates.txt
    
    :param agency: a str identifying the transit agency. Assumes a agency.zip GTFS archive.
    :param day: an 8-char str or int specifying a day. Optional.
    :param exp: bool, whether or not to account for CalException s active today
    :returns a dict of tuples, representing calendar information in the agency GTFS file.
             If today is provided then this dict includes only services active today.
    """
    dicto = {}
    cal = get_gtfs_df(webloc, agency, 'calendar')
    # calendar.txt header:
    # service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
    if day is not None:
        date = u.string_to_date(day)
        dow = date.strftime("%A").lower() # lowercase name not number

    for _, row in cal.iterrows():
        startstring, endstring = row['start_date'], row['end_date']
        startdate, enddate = u.string_to_date(startstring), u.string_to_date(endstring)

        if day is None or\
           (row[dow]==1 and startdate <= date <= enddate): #
            dicto[row['service_id']] = Service(int(row['monday']),
                                               int(row['tuesday']),
                                               int(row['wednesday']),
                                               int(row['thursday']),
                                               int(row['friday']),
                                               int(row['saturday']),
                                               int(row['sunday']),
                                               startstring,
                                               endstring)
    # Start accounting for exceptions:
    if day is not None and exp is True:
        exceptions = get_exceptions(agency, day)        
        for x in exceptions:
            thisserviceid = x.service_id
            if x.exception_type == 1:
                # Service ID is activated today
                if thisserviceid not in dicto: # service ID should be added and is not currently in dict
                    servicetup = get_services(agency)[thisserviceid] # Write tuple by recursively calling function
                    dicto[x.service_id] = servicetup
        
            elif x.exception_type == 2:
                if thisserviceid in dicto: # Service has been disabled for today
                    del dicto[x.service_id] # remove it from dict
        # print 'Returning ', len(dicto), 'service types active for today.'

    return dicto

def get_exceptions(agency, day = None):
    """
    Read through exception dates for given agency, and 
    return (a dict of) list(s) of tuples. As in:
        Dict, where:
        Key: date as expressed in a string yyyymmdd
        Value: List(Tuple), each tuple representing an exception for Key date
        List Item/Tuple: (Service_ID, ExceptionType)
        Exception Type: 1 for adding service, 2 for removing service.
    
        List, of 'Value'.
    
    If today is not None and is an int date, 
    return only those exceptions active for today's date in a list, not a dictionary.
    
    Example returned list, for Christmas:
        [('Green_Monday_Thursday', 2),
        ('Orange_Winter_Monday_Friday', 2), 
        ('Yellow_Monday_Thursday', 2), 
        ('Blue_Winter_Monday_Friday', 2),
        ('Red_Winter_Monday_Friday', 2), 
        ('Turquoise_Monday_Thursday', 2)]
    
    :param agency: a str identifying the transit agency. Assumes a agency.zip GTFS archive.
    :param today: an int representing date, in the format yyyymmdd.
    :returns a list or a dict. If today is a date, then returns a list of exceptions active today.
             If today is None then returns a dict of *all* exceptions information in agency GTFS.
    """
    
    cexp = get_gtfs_df(webloc, agency, 'calendar_dates')
    dicto = {}
    l = [] # will be returned if today is passed
    
    for _, row in cexp.iterrows():
        
        t = CalException(row['service_id'], row['exception_type'])
        dateint = row['date']

        if day is None:
            if dicto.get(dateint) is None:
                    dicto[dateint] = [t]
            else:
                dicto[dateint].append(t)
            return dicto
        
        else:
            if day is not int: day = int(day)
            if dateint == day:
                l.append(t)
            return l

def get_trip_id(todaytripsdict, route_id, direction_id):
    """
    With a dict of the trips active today and through a series 
    of comparisons decide which trip the vehicle info belongs to.
    Returns trip_id matching GTFS file.
    
    :param todaytripsdict: a dict of today's trips for a given agency, in the format returned by get_day_trips(agency, today)
           This is used instead of generating it inside this so as to avoid generating for each vehicle.
    :param route_id: a str corresponding with the route_id field in GTFS/GTFS-RT feeds of this agency
    :param direction_id: an int corresponding with direction_id field in GTFS/GTFS-RT feeds of this agency
    :param datetime: experimental field, optional. Defaults to datetime obj now()
    :returns str trip_id, and corresponding tuple
    """
    for trip_id, tftup in todaytripsdict.iteritems():
        if route_id is not None and route_id == tftup.route_id:
            if direction_id is not None and direction_id == tftup.direction_id:
                start, end = u.string_to_dt(tftup.start_time), u.string_to_dt(tftup.end_time)
                if start <= datetime.datetime.now() <= end + datetime.timedelta(hours=1):
                    return str(trip_id), tftup

def get_nextbus_agency_vl(agency):
    """
    get list of vehicle from nextbus
    :param agency:
    :return list of vehicle in namedtuples
    """
    alist = []
    for route in agencydict.get(agency):
        rlist = get_nextbus_route_vl(agency,route_id_to_NB_tag.get(agency).get(route))
        alist.extend(rlist)
        
    return alist


def get_nextbus_route_vl(agency, route, t=0):
    """
    Return a list of named tuples of AVL data for given route given the 
    desired agency id, NB route tag, and last time info was obtained.
    
    :param agency: a str NextBus identifier of a transit agency
    :param route: a str NextBus identifier of a transit route for given agency
    :param t: Last time this function was called, in *msec Posix time*. Optional, default 0.
    :returns a list of VehiclePos named tuples.
    """
    url = nextbus_url + 'vehicleLocations&a=' + agency + '&r=' + route + '&t=' + str(t)

    try:
        vehiclelist =  u.get_json_from_url(url).get('vehicle')
    except ValueError:
        vehiclelist = []
        
    vlist = []
    if vehiclelist:
        for vehicle in vehiclelist:
            t = get_nextbus_vehicle_data(agency, vehicle)

            vlist.append(t)

    return vlist


def get_nextbus_vehicle_data(agency, vehicle):
    """
    For a given NextBus dict representing a single vehicle,
    return all values in types and units consumable by the GTFS-RT 
    Python Protobuf compiler. Return None if key not in dict.
    NextBus does not return odometer value but it is returned
    as None here for consistency.
    
    @attention: Recall that dict.get(key) returns None if key is not 
    found. You must test_tripudate for None, e.g.:
        if lat is not None: feedentity.vehicle.position.latitude = lat
        
    :param vehicle: a NB dict of vehicle location/info, can be obtained as get_nextbus_route_vl(agency, route, t).get('vehicles')
    :returns a VehiclePos named tuple of information ordered and converted in the formats accepted by GTFS-RT
    """
    
    vehicle_id = route_id = direction_id = dirtag = lat = lon = timestamp = bearing = speed = odometer = None
    if isinstance(vehicle, dict):
        vehicle_id = vehicle.get('id')

        route_id = NB_tag_to_route_id.get(vehicle.get('routeTag'))
        
        if 'dirTag' in vehicle:
            lookup = (route_id + vehicle.get('dirTag')[0]).lower() #route_id + first letter of NB dirTag (n,s,e,w), all lowercase
            direction_id = dir_id_dict.get(agency).get(lookup)
            dirtag = vehicle.get('dirTag')
            
        lat = float(vehicle.get('lat'))
        
        lon = float(vehicle.get('lon'))
        
        if 'secsSinceReport' in vehicle:
            timestamp = get_nextbus_timestamp(vehicle.get('secsSinceReport'))
        
        h = int(vehicle.get('heading'))
        bearing = h if h >=0 else None
        
        if vehicle.get('speedKmHr'): 
            speed = float(vehicle.get('speedKmHr'))*(5/18) # return speed strictly in m/s

    return VehiclePos(vehicle_id, route_id, direction_id, dirtag, lat, lon, timestamp, bearing, speed, odometer)


def get_nextbus_timestamp(secsSinceReport):
    """
    Convert the secsSinceReport value into epoch timestamp.
    :param secsSinceReport: The number of secs value since report
    :returns The timestamp field converted into Epoch time in seconds
    """
    return int(time.time())-int(secsSinceReport)


def current_stop_sequence2(agency, trip_id, lat, lon):
    """
    input agency and trip info and current location of the vehicle
    output the sequence of the closest stop to the vehicle
    """
    dic_of_stop_loc = stopdic_loc[agency]
    trip_id = int(trip_id)
    list_id_seq = get_seq_dict(agency).get(trip_id)

    return get_closest_stop(dic_of_stop_loc, list_id_seq, lat, lon)

def get_seq_dict(agency):
    """
    get dictionary whose key as agency and trip id, value is a list of stop id order by sequence as the stop_times.txt
    :param agency:a str NextBus identifier of a transit agency
    :return dictionary of key as route, value as stop_id ordered by sequence
    """
    stopseq = get_gtfs_df(webloc, agency, 'stop_times')
    dict_seq = {}
    for trip in set(stopseq['trip_id']):
        dict_seq[trip] = stopseq.loc[stopseq['trip_id']==trip,'stop_id'].tolist()
    return dict_seq


def start_time(a,r,dirTag,block,tftup, time_to_start, seq):  
    """
    get the start time of each vehicle trip using block from next bus
    :param a: agency
    :param r: route
    :param dirTag: direction tag
    :param block: block number
    :param tftup: trip information
    :param time_to_start unix time stamp
    :return start time of the vehicle trip
    """    
    idx = datetime.datetime.strptime(str(u.get_service_day()),'%Y%m%d').weekday()
    SC = serviceClass.get(a)[idx]
    start_time_list_per_block = schedule.get(a).get(r).get(SC).get(dirTag).get(block)
    #print start_time_list_per_block

    if start_time_list_per_block:#in case start_time_list_per_block is none
        start_time = binary_search(start_time_list_per_block,time_to_start,seq)
        if a=='dc-streetcar' and dirTag=='west':
            start_time = start_time_special_streetcar(start_time)
        else:
            start_time = get_start_time_block(start_time, tftup)
        if int(start_time.split(':')[0]) < 5:
            start_time = str(int(start_time.split(':')[0])+24)+':'+start_time.split(':')[1]+':'+start_time.split(':')[2]
        return start_time, start_time_list_per_block
    else:
        return 0,start_time_list_per_block

def binary_search(time_list,time_to_start, seq):           
    start=0
    last=len(time_list)-1
    found=False 
    time_to_start_dt = datetime.datetime.fromtimestamp(time_to_start)
    while start<last and not found:
        mid=(start+last)//2
        if time_to_start_dt == get_datetime_blocktime(time_list[mid]):
            start = mid
            found = True
        elif time_to_start_dt < get_datetime_blocktime(time_list[mid]):
            last = mid
            if last-start==1:
                found=True
        else:
            start = mid
            if last-start==1:
                found=True
    if seq!=0:
        return time_list[start]
    else:
        if start == (len(time_list)-1):
            return time_list[start]
        #elif abs(u.string_to_dt(time_list[start])-time_to_start_dt) > abs(u.string_to_dt(time_list[start+1])-time_to_start_dt):
        elif abs(get_datetime_blocktime(time_list[start])-time_to_start_dt) >  abs(get_datetime_blocktime(time_list[start+1])-time_to_start_dt):
            return time_list[start+1]
        else:
            return time_list[start]

def get_datetime_blocktime(time_mid):
    """
    time_list[mid]
    return unix time
    """
    date = u.get_service_day()
    if int(time_mid.split(":")[0])<5:
        date = date+1
    return datetime.datetime.strptime(str(date)+time_mid,"%Y%m%d%H:%M:%S")
   
def get_start_time_block(start_time, tftup):
    """
    get the start time for the vehicle just set off from stop 0, which is the start time of current trip
    :param tftup: current trip info
    :return estimate start time in format hh:mm:ss
    """
    delta = u.string_to_dt(start_time) - u.string_to_dt(tftup.start_time)
    num_of_interval = int(delta.total_seconds() // tftup.headway_secs)
    start_time_trip = u.string_to_dt(tftup.start_time) + datetime.timedelta(0, num_of_interval * tftup.headway_secs)
    dt = datetime.time(hour = int(start_time_trip.hour),\
                       minute = int(start_time_trip.minute),
                       second = int(start_time_trip.second)).isoformat()
    return dt

def start_time_special_streetcar(start_time):#10:47:00
    if int(start_time.split(':')[1])<20:#11,12
        mins='12'
        hours=str( int(start_time.split(':')[0]))
    elif int(start_time.split(':')[1])<30:#23,28
        mins='24'
        hours=str( int(start_time.split(':')[0]))
    elif int(start_time.split(':')[1])<40:#35
        mins='36'
        hours=str( int(start_time.split(':')[0]))
    elif int(start_time.split(':')[1])<50:#43,47
        mins='48'
        hours=str( int(start_time.split(':')[0]))
    else:
        mins='00'
        hours=str( int(start_time.split(':')[0])+1)
    start_time=hours+":"+mins+":"+start_time.split(':')[2]
    return start_time

def get_vehicle():
    """
    get vehicle list from bishop peak list
    :return list of vehicle operating
    """
    url_veh = apiloc + "vehicle" + "&action=" + "list" + agencyID
    veh_list = u.get_json_from_url(url_veh).get("vehicle")
    
    ve_clean_list=[]
    unique={}
    for i in veh_list:
        if i.get("routeID")>1000:
            if i.get("vehicleName") not in unique.keys():
                unique[i.get("vehicleName")]=i
            else:
                print i 
                if i.get("updated")>unique[i.get("vehicleName")].get("updated"):
                    print unique[i.get("vehicleName")]
                    unique[i.get("vehicleName")]=i
                    print("unique update", i)
    
    for ve in unique.values():
        ve_clean_list.append(clean_up(ve))
    return ve_clean_list

def clean_up(ve):
    """
    get information from vehicle json and output as nametuaple
    """
    if time.time()-ve.get("updated") < 1000:
        vehicle_id = ve.get("vehicleName")
        route_id = ve.get("routeID")
        lat,lon = ve.get("lat"), ve.get("lng")
        timestamp = ve.get("updated")
        minsLate = ve.get("minsLate")
        nextStopID = ve.get("nextStopID")
        tripID=ve.get("tripID")
        return VehicleNew(vehicle_id, route_id, lat, lon, timestamp, minsLate, nextStopID, tripID)


def main():
    print 'Run something else.'

if __name__ == '__main__':
    main()