#!/usr/bin/env python2.7.3

# -------------------------------------------------------------------------------    
# Name:        import vehicle information to proto using tuple
# Author:      huixin rao
# Created:     2018/02/13
# -------------------------------------------------------------------------------

import os, sys, time, shutil,datetime,time
import google.transit.gtfs_realtime_pb2 as g
from get_info import get_vehicle, get_gtfsid, get_trip_info, get_eta
from utils import get_service_day
import logging
from logging.handlers import TimedRotatingFileHandler
import pandas as pd
from ddot_realtime import get_seq_dict, distance
from collections import namedtuple

scriptloc = 'file locaton'
ext = '.pb'#'.txt'#

agency = 'dc-circulator'
webloc = 'web location'

routes_dict= {11318: "Yellow",#u'GT-US',
              11319: "Green",#u'WP-AM',
              11320: "Blue",#u'US-NY',
              11321: "Turquoise",#u'RS-DP',
              11322: "Orange",#u'PS',
              11323: "Red"}#"u'NM'}

VehicleInfo = namedtuple('VehicleInfo', 'vehicle_id, route_id, direction_id, \
                                        trip_id, bishop_trip, \
                                        start_time,start_time2, start_date\
                                        lat, lon, schedule_relation\
                                        nextstop, seq, eta, timestamp')

####logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
path = os.getcwd()
#logging handler
fh = TimedRotatingFileHandler(path+'/temp/log.log', when = 'h',interval = 1)
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
######

df = pd.read_csv("stop_geo_info.csv")[['route','direction','gtfs_id','stopID']]
seq_dict = get_seq_dict(agency)

tuplelist={}


def recording(today_trip,previous):
    #returnlist={}
    trip_start_time={}
    vehicle_list = get_vehicle()
    count=0
    filename = 'dc-circulator-'+'tripupdates'+ext
    fm = g.FeedMessage()
    fm.header.gtfs_realtime_version = '2.0'
    fm.header.incrementality = g._FEEDHEADER_INCREMENTALITY.values_by_name['FULL_DATASET'].number
    

    for ve in vehicle_list:
        schedule_relation = 0
        if ve.tripID==0:
            #skip the trip ID = 0 records
            continue
        
        route = routes_dict[ve.route_id]
        
        if get_gtfsid(ve.nextStopID):
            stop_ID = get_gtfsid(ve.nextStopID)
        else:
            #skip the next stop ID not in ddot list record
            continue

        eta = get_eta(ve.route_id,ve.nextStopID)
        if eta: eta2= time.strftime('%H:%M:%S', time.localtime(int(eta)))
        else:
            #skip no eta records
            continue
        
        if ve_to_garage_distance(ve.lat,ve.lon):
            #skip vehicles that are in/near garage
            continue
        
        start_time, start_time2 = get_trip_info(ve.tripID,ve.nextStopID) # start_time is the raw start time align wit optibus system, start_time2 is the one round to 10 mins
        if start_time2=='0':
            start_time = start_time2 = previous[ve.vehicle_id][-1]
        start_date = str(get_service_day())
        #get rid of the toooo old eta time # wait for bishop peak responds
        t = datetime.datetime.strptime(start_date+start_time, '%Y%m%d%H:%M:%S')
        if time.mktime(t.timetuple())>eta:
            continue
        
        #if it's terminal stops and not sure about the trip id, reocrd the trip id serve the route
        trip_list=[]
        for trip_id,tup in today_trip.iteritems():
            if tup.route_id == route:
                trip_list.append(trip_id)

        if int(stop_ID) in seq_dict[trip_list[0]] and int(stop_ID) in seq_dict[trip_list[1]]:
            trip_id_final=str(trip_list[0])+" "+str(trip_list[1])
            seq = str(seq_dict[trip_list[0]].index(int(stop_ID))) + " " + str(seq_dict[trip_list[1]].index(int(stop_ID)))
            
            #write to protobuf as start of next trip but do no return
            seq_to_proto='0'
            for i in trip_list:
                if seq_dict[i].index(int(stop_ID))==0:
                    trip_id_to_proto = str(i)
                    direction_proto = today_trip[i].direction_id
                    
            if trip_id_to_proto not in trip_start_time.keys():
                trip_start_time[trip_id_to_proto] = [start_time2]
            else:
                if start_time2 in trip_start_time[trip_id_to_proto]:#which is duplicate
                    schedule_relation=1
                else:
                    trip_start_time[trip_id_to_proto].append(start_time2)
                    
            tuplelist[ve.vehicle_id] = VehicleInfo(ve.vehicle_id, route, direction_proto, \
                                        trip_id_to_proto, ve.tripID, start_time,start_time2,  start_date,\
                                        ve.lat, ve.lon, schedule_relation,\
                                        stop_ID,  seq_to_proto, eta, ve.timestamp)
            
        elif int(stop_ID) in seq_dict[trip_list[0]]:

            trip_id_final=str(trip_list[0])
            direction = today_trip[int(trip_id_final)].direction_id
            seq = get_seq(trip_id_final, stop_ID)
            
            if ve.vehicle_id in previous.keys():
                if previous[ve.vehicle_id].trip_id == trip_id_final: 
                    start_time2 = previous[ve.vehicle_id].start_time2 
            
            if trip_id_final not in trip_start_time.keys():
                trip_start_time[trip_id_final] = [start_time2]
            else:
                if start_time2 in trip_start_time[trip_id_final]:#which is duplicate
                    schedule_relation=1
                else:
                    trip_start_time[trip_id_final].append(start_time2)
            
          tuplelist[ve.vehicle_id] = VehicleInfo(ve.vehicle_id, route, direction, \
                                        trip_id_final, ve.tripID, start_time, start_time2,  start_date,\
                                        ve.lat, ve.lon, schedule_relation, \
                                        stop_ID,  seq, eta, ve.timestamp)  
        
        elif int(stop_ID) in seq_dict[trip_list[1]]:

            trip_id_final=str(trip_list[1])
            direction = today_trip[int(trip_id_final)].direction_id
            seq = get_seq(trip_id_final, stop_ID)
            
            if ve.vehicle_id in previous.keys():
                if previous[ve.vehicle_id].trip_id == trip_id_final: 
                    start_time2 = previous[ve.vehicle_id].start_time2
       
            if trip_id_final not in trip_start_time.keys():
                trip_start_time[trip_id_final] = [start_time2]
            else:
                if start_time2 in trip_start_time[trip_id_final]:#which is duplicate
                    schedule_relation=1
                else:
                    trip_start_time[trip_id_final].append(start_time2)
                    

            tuplelist[ve.vehicle_id] = VehicleInfo(ve.vehicle_id, route, direction, \
                                        trip_id_final, ve.tripID, start_time,start_time2,  start_date,\
                                        ve.lat, ve.lon, schedule_relation,\
                                        stop_ID,  seq, eta, ve.timestamp) 

        #keep the start_time consistant through out the whole trip instead of updating based on input
        if ve.vehicle_id in previous.keys():
            if previous[ve.vehicle_id].trip_id!= trip_id_final and len(trip_id_final) >2:#9 to 9 10
                if previous[ve.vehicle_id].seq == 0:
                    
                    if trip_id_final not in trip_start_time.keys():
                        trip_start_time[trip_id_final] = [start_time2]
                    else:
                        if start_time2 in trip_start_time[trip_id_final]:#which is duplicate
                            schedule_relation=1
                        else:
                            trip_start_time[trip_id_final].append(start_time2)

                    tuplelist[ve.vehicle_id] = previous[ve.vehicle_id]._replace(bishop_trip = ve.tripID, start_time=start_time, start_time2=start_time2,lat=ve.lat,lon=ve.lon, eta=eta, timestamp=ve.timestamp, schedule_relation=schedule_relation)#relation??
                else:
                    if ve.tripID == previous[ve.vehicle_id].bishop_trip:
                        seq = get_seq(previous[ve.vehicle_id].trip_id, stop_ID)
                        tuplelist[ve.vehicle_id] = previous[ve.vehicle_id]._replace(lat=ve.lat,lon=ve.lon,seq=seq,eta=eta,timestamp=ve.timestamp)
                    else:
                        seq='0'
                        trip_list.remove(int(previous[ve.vehicle_id].trip_id)) # renew  the trip id # trip_list.remove(int(previous[ve.vehicle_id].trip_id_final))
                        trip_id_final = str(trip_list[0])
                        direction = today_trip[int(trip_id_final)].direction_id

                        if trip_id_final not in trip_start_time.keys():
                            trip_start_time[trip_id_final] = [start_time2]
                        else:
                            if start_time2 in trip_start_time[trip_id_final]:#which is duplicate
                                schedule_relation=1
                            else:
                                trip_start_time[trip_id_final].append(start_time2)

                        tuplelist[ve.vehicle_id] = VehicleInfo(ve.vehicle_id, route, direction, \
                                        trip_id_final, ve.tripID, start_time,start_time2,  start_date,\
                                        ve.lat, ve.lon, schedule_relation,\
                                        stop_ID,  seq, eta, ve.timestamp) 

        if ve.vehicle_id in tuplelist.keys():
            write_protobuf(tuplelist[ve.vehicle_id],fm)
            count+=1

        #st = str(ve.vehicle_id) + '{:>10}'.format(str(ve.tripID)) +'{:>12}'.format(route) +'{:>8}'.format(trip_id_final) + '{:>10}'.format(start_time) + '{:>10}'.format(start_time2) + '{:>10}'.format(stop_ID) + '{:>6}'.format(seq) + '{:>12}'.format(eta2) 


    fm.header.timestamp = int(time.time())
    
    print '    Wrote', count, 'vehicles trip update updated with complete AVL info of dc circulator.'
    st='    Wrote '+str(count)+' vehicles trip update updated with complete AVL info of '+str(agency)
    logger.info(st)
    logger.info(' ')
    
    f = open(scriptloc+'\\'+filename, "wb")
    f.write(fm.SerializeToString())
    f.close()

    newpath = webloc+filename
    shutil.copyfile(os.path.join(scriptloc, filename), newpath)

    return tuplelist


def get_seq(trip_id_final, stop_ID):
    
    if int(stop_ID)==0:
        seq = len(seq_dict[int(trip_id_final)])
    else:
        seq = str(seq_dict[int(trip_id_final)].index(int(stop_ID)))
    return str(seq)

def write_protobuf(veInfo,fm):
    """
    input as a nametuple

    """
    myentity = fm.entity.add()
    myentity.id = str(veInfo.vehicle_id) + "_" + veInfo.route_id + "_" + str(veInfo.bishop_trip)
        
    myentity.trip_update.timestamp =  veInfo.timestamp
    myentity.trip_update.vehicle.id = myentity.trip_update.vehicle.label = veInfo.vehicle_id
    myentity.trip_update.trip.route_id = veInfo.route_id
    myentity.trip_update.trip.direction_id = veInfo.direction_id
    myentity.trip_update.trip.trip_id = str(veInfo.trip_id)
    myentity.trip_update.trip.schedule_relationship = veInfo.schedule_relation
    myentity.trip_update.trip.start_time = veInfo.start_time2
    myentity.trip_update.trip.start_date = veInfo.start_date
        
    stu = myentity.trip_update.stop_time_update.add()
    stu.stop_id = veInfo.nextstop
    stu.stop_sequence = int(veInfo.seq)
    stu.arrival.time = veInfo.eta

    st = str(veInfo.vehicle_id) + '{:>10}'.format(str(veInfo.bishop_trip))  \
    +'{:>12}'.format(veInfo.route_id) +'{:>5}'.format(veInfo.trip_id) \
    + '{:>10}'.format(veInfo.start_date) + '{:>10}'.format(veInfo.start_time) + '{:>10}'.format(veInfo.start_time2)\
    +'{:>10}'.format(str(veInfo.lat))+'{:>10}'.format(str(veInfo.lon))\
    +'{:>10}'.format(veInfo.nextstop) + '{:>4}'.format(veInfo.schedule_relation)\
     + '{:>6}'.format(veInfo.seq) + '{:>12}'.format(veInfo.eta) 
    
    logger.info(st)
    print(st)

def ve_to_garage_distance(lat,lon):
    slat = 38.914968
    slon = -76.978436
    if distance(slat,slon,lat,lon)<1:
        return True
    else: return False

