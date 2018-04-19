#!/usr/bin/env python2.7.3
from __future__ import division
import os, sys, time, shutil
import google.transit.gtfs_realtime_pb2 as g  # @UnresolvedImport
from ddot_realtime_clean import get_seq_dict,start_time, get_multiplestop_pred, get_nextbus_agency_vl
import logging
from logging.handlers import TimedRotatingFileHandler
from utils import get_service_day
from collections import namedtuple
from stop_dict_generator import get_stop_dict
import pandas as pd
from operator import attrgetter

scriptloc = sys.path[0]+'\\RT Feeds'
ext = '.pb'#'.txt'#

webloc = 'file location'
agencies = ['dc-circulator','dc-streetcar']

nextbus_url = 'nextbux url'

direction_dict = {
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
 
NB_tag_to_route_id = {
    "yellow": "Yellow",
    "green": "Green",
    "blue": "Blue",
    "rosslyn":"Turquoise",
    "potomac": "Orange",
    "mall": "Red",
    "h_route": "Red"
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

VehicleInfo = namedtuple('VehicleInfo', 'vehicle_id, route_id, direction_id, \
                                        trip_id,  \
                                        timestamp, agency, route_NB, dirtag, block, tftup')
                                    
etaInfo = namedtuple('eta','nextstop, seq, eta')

####logging test
logger = logging.getLogger()
logger.setLevel(logging.INFO)
path = os.getcwd()
#logging handler
fh = TimedRotatingFileHandler(path+'/temp/log_test.log', when = 'midnight',interval = 1)
fh.suffix = "%Y-%m-%d"
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

######get dictionary that match stop tag to regional id
def tag_to_id(agency):
    """
    generate dictionary key as stop tag, value as gtfs id
    :param agency: agency of input, dc-circulator or dc-streetcar
    :return dictionary, key as stop tag, value as gtfs id
    """
    path = os.getcwd()
    if agency == 'dc-circulator':
        stopcsv=pd.read_csv(path+'/NB_stop2.csv')
    if agency == 'dc-streetcar':
        stopcsv=pd.read_csv(path+'/NB_stop4.csv')
    tag_to_id={}
    for i in range(len(stopcsv)):
        tag_to_id[stopcsv.loc[i,'tag']] = int(stopcsv.loc[i,'gtfs_id'])
    return tag_to_id

seq_dict={}
stopdic_tag={}
tag_id_dict={}
for agency in agencies:
    seq_dict[agency] = get_seq_dict(agency)
    _, stopdic_tag[agency] = get_stop_dict(agency)
    tag_id_dict[agency] = tag_to_id(agency)


def circulator_trip_update(tripsdict,agency):
    """
    this function is getting eta info for all stops, saving the eta records by vehicle_trip
    then write to protobuf
    :param tripsdict:dictionary of today's trips
    :param agency:  agency of input, dc-circulator or dc-streetcar
    :reutrn 
    """
    filename = agency+'-'+'tripupdates'+ext
    ve_tr={}
    
    for trip_id,tftup in tripsdict.iteritems():
        stop_list = seq_dict.get(agency).get(int(trip_id))
        route_NB = route_id_to_NB_tag.get(agency).get(tftup.route_id.lower())
        route_id = tftup.route_id.lower()
        
        for i in range(3):
            try:
                pre_stop_list = get_multiplestop_pred(agency, route_NB, stop_list).get('predictions')
            except:
                continue
            else:
                break

        for stop in pre_stop_list:#stop is a dictionary
            stop_id = tag_id_dict.get(agency).get(stop.get('stopTag'))
            idx = stop_list.index(int(stop_id))
            #when there's no prediction, attribute error occurs
            try:
                pred = stop.get('direction').get('prediction')
            except:
                continue
            
            if isinstance(pred,dict):
                pred=[pred]
           
            for i in pred: 
                vehicle = i.get('vehicle')   
                trip_id = str(trip_id)

                if vehicle+'_'+ str(trip_id) not in ve_tr.keys():
                    timestamp = int(time.time())
                    block = i.get("block")
                    dirtag = i.get('dirTag')
                    direction_id = direction_dict.get(agency).get(route_id.lower() + dirtag[0])
                    eta = i.get('epochTime')
                    #starttime = start_time(agency, route_NB, dirtag, block, tftup)
                    ve_tr[vehicle+'_'+trip_id]={'eta':[etaInfo(stop_id,idx,eta)],
                                                        'other':VehicleInfo(vehicle, route_id, direction_id, \
                                                        trip_id,timestamp, agency, route_NB, dirtag, block, tftup)}

                else:
                    eta = i.get('epochTime')
                    if any(a.seq==idx for a in ve_tr[vehicle+'_'+trip_id]['eta']):  
                    #same stop prediction, appears in streetcar
                        continue
                    ve_tr[vehicle+'_'+trip_id]['eta'].append(etaInfo(stop_id,idx,eta))

    fm,count = write_to_pb(ve_tr)

    print '    Wrote', count, 'trip update updated with complete AVL info of', agency.replace('-', ' ')+'.'
    st='    Wrote '+str(count)+' trip update updated with complete AVL info of '+str(agency)
    logger.info(st)
    logger.info(' ')
    f = open(scriptloc+'\\'+filename, "wb")
    f.write(fm.SerializeToString())
    f.close()
    #print 'Wrote .pb file to script location.'
    
    newpath = webloc+filename
    shutil.copyfile(os.path.join(scriptloc, filename), newpath)

def vehicle_position(agency):
    """
    getting vehicle position, prepared for vehicle position feed,
    not being used for now
    """
    vehiclelist = get_nextbus_agency_vl(agency)
    vehicle_dict={}
    for vehicle in vehiclelist:
        #vehicle_id, route_id, direction_id, dirtag, lat, lon, timestamp, bearing, speed, odometer
        vehicle_dict[vehicle.vehicle_id]=vehicle
    return vehicle_dict
    
def write_to_pb(ve_tr):
    """
    write trip update information into protobuf entity
    1.find the start time of trip
    :param ve_tr: dictionary of vehicle_trip, key is vehicle_trip,value is the dictionary, 
                    keys inside are eta and other,  value of eta is a list of namedtuple of eta
                    value of 'other' includes vehicle_id, route_id, direction_id, trip_id,
                                        timestamp, agency, route_NB, dirtag, block, tftup
    :param vehicle_dict: dictionary of vehicle info
    :return: feedmessage
    """
    fm = g.FeedMessage()
    
    ## Feed Header
    fm.header.gtfs_realtime_version = '2.0'
    fm.header.incrementality = g._FEEDHEADER_INCREMENTALITY.values_by_name['FULL_DATASET'].number #enum: full dataset
    fm.header.timestamp = int(time.time())
    check_multiple={}
    count=0
    for key, value in ve_tr.iteritems():
        
        sorted_eta = sorted(value['eta'],key=attrgetter('seq'))
        
        #get start time of current trip
        if sorted_eta[0].seq==0:
            time_to_start = int(int(sorted_eta[0].eta)/1000)        
        else:
            time_to_start = int(time.time()) 
        start_time_value, block = start_time(value['other'].agency, value['other'].route_NB, \
                                value['other'].dirtag, value['other'].block, \
                                value['other'].tftup, time_to_start, sorted_eta[0].seq)
        if sorted_eta[0].seq!=0:
            logger.info(block)
        if start_time_value==0:#start time cannot be identified, pass this record
            continue
        
        #entity
        myentity = fm.entity.add()
        myentity.id = key
        myentity.trip_update.timestamp = value['other'].timestamp
        myentity.trip_update.trip.trip_id=value['other'].trip_id
        myentity.trip_update.trip.route_id=value['other'].route_id
        myentity.trip_update.trip.direction_id=value['other'].direction_id
        myentity.trip_update.trip.start_time = start_time_value
        myentity.trip_update.trip.start_date = str(get_service_day())
        myentity.trip_update.vehicle.id=myentity.trip_update.vehicle.label=value['other'].vehicle_id
        count+=1

        st2 = myentity.id.replace("_"," ") +' ' + myentity.trip_update.trip.start_time 

        #write eta for each stop
        last=int(sorted_eta[0].eta)/1000
        for i in range(len(sorted_eta)):
            #filter eta eariler than previous stop, STOP_TIME_UPDATE_PREMATURE_ARRIVAL
            if i!=0 and int(sorted_eta[i].eta)/1000<last:
                continue
            #write eta for each stop
            i_eta = sorted_eta[i]
            stu = myentity.trip_update.stop_time_update.add()
            stu.stop_sequence=i_eta.seq
            stu.stop_id=str(i_eta.nextstop)
            last = stu.arrival.time=int(int(i_eta.eta)/1000)
            eta2 = time.strftime('%H:%M:%S', time.localtime(stu.arrival.time))   
            if i==0:
                st2 = st2+' '+str(stu.stop_sequence)+' '+ eta2

        logger.info(st2)

        #avoid the multiple entity per trip warning
        if value['other'].trip_id not in check_multiple.keys():
            check_multiple[value['other'].trip_id] = [myentity.trip_update.trip.start_time]
        elif myentity.trip_update.trip.start_time in check_multiple[value['other'].trip_id]:
            myentity.trip_update.trip.schedule_relationship = 1
        else:
            check_multiple[value['other'].trip_id].append(myentity.trip_update.trip.start_time)
        
        # vehicle position feed
        '''
        if myentity.trip_update.vehicle.id not in vehicle_dict.keys():
            continue
        myentity.vehicle.vehicle.id = myentity.vehicle.vehicle.label = myentity.trip_update.vehicle.id
        pos = vehicle_dict[myentity.vehicle.vehicle.id]
        myentity.vehicle.position.latitude = pos.lat
        myentity.vehicle.position.longitude = pos.lon
        myentity.vehicle.position.speed = pos.speed
        myentity.vehicle.timestamp = pos.timestamp
        myentity.vehicle.trip.trip_id = myentity.trip_update.trip.trip_id
        myentity.vehicle.trip.route_id = myentity.trip_update.trip.route_id
        myentity.vehicle.trip.direction_id = myentity.trip_update.trip.direction_id
        myentity.vehicle.trip.start_time = myentity.trip_update.trip.start_time
        myentity.vehicle.trip.start_date = myentity.trip_update.trip.start_date
        '''
    return fm,count