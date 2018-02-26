#!/usr/bin/env python2.7.3

# -------------------------------------------------------------------------------    
# Name:        read json from bishop peak
# Author:      huixin rao
# Created:     2017/11/14
# -------------------------------------------------------------------------------

import utils as u
from collections import namedtuple
from ddot_realtime import get_trip_id, get_seq_dict,get_gtfs_df
import pandas as pd
import time


agency='dc-circulator'
StopInfo = namedtuple('stopinfo', 'stop_name, lat, lon')

apibase = "api url"
app_id="app"
key="key"
agencyID='agency'

webloc = apibase + "?app_id=" + app_id + "&key=" + key + "&controller="
controller1,controller2="gtfs","gtfsrt"

VehicleNew = namedtuple('VehicleData', 'vehicle_id, route_id, lat, lon, timestamp, minsLate, nextStopID, tripID')

direction_dict = {
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
            }

routes_dict2= {11318: "yellow",#u'GT-US',
              11319: "green",#u'WP-AM',
              11320: "blue",#u'US-NY',
              11321: "rosslyn",#u'RS-DP',
              11322: "potomac",#u'PS',
              11323: "mall"}#"u'NM'}

routes_dict= {11318: "Yellow",#u'GT-US',
              11319: "Green",#u'WP-AM',
              11320: "Blue",#u'US-NY',
              11321: "Turquoise",#u'RS-DP',
              11322: "Orange",#u'PS',
              11323: "Red"}#"u'NM'}

seq_dict = get_seq_dict(agency)

df = pd.read_csv("new_stop.csv")[['route','direction','gtfs_id','stopID']]

stop_id_dict={}
for index, row in df.iterrows():
    stop_id_dict[int(row["stopID"])]=[int(row["gtfs_id"]),row["direction"]]
print("stop_id_dict done!")


def get_eta(routeID,stopID):
    """
    get the estimate arrival time json
    :return dictionary of eta, key as routeID,stopID, value as list of the estimate arrival time
    :output_example: eta in epochtime format
    """
    url_eta = webloc + "eta" + "&action=" + "list" + agencyID + "&routeID=" + str(routeID) + "&stopID=" + str(stopID)
    #print(url_eta)
    for i in range(3):
        try:
            eta_list = u.get_json_from_url(url_eta).get("stop")
            #print("clear")
        except:
            eta_list = []
            print("eta error", url_eta)
            continue
        else:
            break
    if eta_list:     
        return eta_list[0].get("ETA1") 
    else: 
        return eta_list   


def get_vehicle():
    """
    get vehicle list from bishop peak list
    :return list of vehicle operating
    """
    url_veh = webloc + "vehicle" + "&action=" + "list" + agencyID
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

def get_trip_info(tripID,nextStopID,seqq=0):
    """
    get the start time of each trip from bishop peak
    :param tripID: trip id in bishop paek format in 6 digit 
    :param nextStopID: next stop id in the bishop peak format as 6 digit number from 113120 to 114181
    """
    if tripID!=0:
        url_trip = webloc+'gtfs'+'&action='+'stopTimes'+agencyID + "&tripID=" +str(tripID) 
        
        
        for i in range(3):
            try:
                stop_list = u.get_json_from_url(url_trip).get("stopTimes")
                #print("clear")
            except:
                print("error")
                print(url_trip)
                stop_list=[]
                continue
            else:
                break
        if stop_list:
            start_time = stop_list[0].get("departureTime")
            if int(start_time.split(':')[0]) >= 24:
                start_time = '0'+str(int(start_time.split(':')[0])-24)+':'+start_time.split(':')[1]+':'+start_time.split(':')[2]
            if seqq!=0:
                print(stop_list[0].get("stopID"), nextStopID, tripID)
            start_time2 = to_zero(start_time)
            return start_time,start_time2
        else:
            #in case the url cannot be reached
            return '0','0'

def to_zero(start_time):
    h, m, s = start_time.split(":") 
    minu = '{:0>2}'.format(str(int(m) // 10 * 10))
    start_time_to_0 = h + ":" + minu + ":" + s
    return start_time_to_0

def get_seq(StopID, trip_id, seq_dict=seq_dict):
    """
    get sequence of stop in each trip, corresponding to stop_time.txt
    :param stopID: stop id as regional id same as stops.txt
    :param trip_id: trip id  same as trips.txt
    :retrun integer starts from 0
    """
    
    if int(StopID) in seq_dict[int(trip_id)]:
        return seq_dict[int(trip_id)].index(int(StopID))  
    else:
        print(StopID,seq_dict[int(trip_id)])
        return 100
        
def get_gtfsid(nextStopID):
    """
    get regianl id from bishop peak stop id 
    :param nextStopID: next stop id in the bishop peak format as 6 digit number from 113120 to 114181
    :return string of regional id
    """
    if any(df.stopID == nextStopID):
        gtfs_id = df.loc[df["stopID"]==nextStopID]['gtfs_id'].item()
        return str(gtfs_id)
    else:
        print(nextStopID)
        return None

def get_direction(todaytripsdict, gtfs_id, routeid):
    """
    get direction
    :param gtfs_id:
    :param routeid:
    :return direction as n,s,e,w or 
            0 which indicates that the stop list does not contains this but may be found in stop_times.txt
    """
    print(df.loc[df["gtfs_id"]== gtfs_id])
    trip_id = direction_id = seq = None
    if df.loc[(df["gtfs_id"] == gtfs_id) & (df["route"]==routes_dict2[routeid])]['direction'].empty:
        print('empty', gtfs_id,routeid)
        trip_id, direction_id, seq = find_trip_direction(todaytripsdict, routes_dict[routeid], gtfs_id)
    else:
        direction = df.loc[(df["gtfs_id"] == gtfs_id) & (df["route"]==routes_dict2[routeid])]['direction'].item() 
        direction_id = direction_dict[routes_dict[routeid].lower()+direction]
        try:
            trip_id, _ = get_trip_id(todaytripsdict, routes_dict[routeid], direction_id)
        except TypeError:
            print('type error', routeid, direction, direction_id)
        if trip_id:    
            seq = get_seq(gtfs_id, trip_id, seq_dict=seq_dict)    
    return trip_id, direction_id, seq

def get_direction_ver1(gtfs_id, routeid):
    """
    get direction
    :param gtfs_id:
    :param routeid:
    :return direction as n,s,e,w or 
            0 which indicates that the stop list does not contains this but may be found in stop_times.txt
    """
    #print(df.loc[df["gtfs_id"]== gtfs_id])
    if df.loc[(df["gtfs_id"] == gtfs_id) & (df["route"]==routes_dict2[routeid])]['direction'].empty:
        direction = 0
    else:
        direction = df.loc[(df["gtfs_id"] == gtfs_id) & (df["route"]==routes_dict2[routeid])]['direction'].item() 
    return direction    
    
def get_trip_id2(todaytripsdict, route_id, direction):
    """
    get trip id and direction id 
    :param todaytripsdict: dictionary of today's trips, key as trip id
    :param route_id: 
    :param direction: n,w,s,e
    :return trip id and direction id corresponding to trips.txt
    """
    #print(route_id, direction)
    direction_id = direction_dict[route_id.lower()+direction]
    try:
        trip_id, _ = get_trip_id(todaytripsdict, route_id, direction_id)
    except TypeError:
        print(route_id, direction,direction_id)
    return trip_id,direction_id

def find_trip_direction(today_trip, route_id, stop_id):
    """
    find_trip_direction(today_trip, routes_dict[vehicle.route_id], int(stu.stop_id))
    get the trip and direction for those that are not available in the new_stop.csv
    :param today_trip: dictionary of today's trips, key as trip id
    :param route_id
    :param stop_id
    """
    for key, value in today_trip.iteritems():
        if value.route_id == route_id:
            if stop_id in seq_dict[key]:
                return str(key), value.direction_id, seq_dict[key].index(stop_id)
 
def get_stop_route_direction():
    webloc = 'web location'
    trip = get_gtfs_df(webloc,'dc-circulator','trips')
    seq_dict = get_seq_dict('dc-circulator')
    dirt_stop={}
    for key,value in seq_dict.iteritems():
        for i in value:
            if i in dirt_stop.keys():
                dirt_stop[i].update({key:[trip.loc[trip["trip_id"] == key]["direction_id"].item(), value.index(i)]})
            else:
                dirt_stop[i] = {key:[trip.loc[trip["trip_id"]==key]["direction_id"].item(), value.index(i)]}
    return dirt_stop

def main():
    print 'Run something else.'

if __name__ == '__main__':
    main()


