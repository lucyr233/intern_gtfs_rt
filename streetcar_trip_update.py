#!/usr/bin/env python2.7.3
from __future__ import division
import os, sys, time, shutil
import google.transit.gtfs_realtime_pb2 as g  # @UnresolvedImport
from ddot_realtime import get_nextbus_agency_vl, current_stop_sequence2, \
        get_trip_id
import logging
from logging.handlers import TimedRotatingFileHandler
from stop_dict_generator import get_stop_dict
from ddot_realtime import get_seq_dict,start_time#,get_in_feed
import utils as u
from utils import get_service_day

scriptloc = 'file location'
ext = '.pb'#'.txt'#

webloc = 'web location'
agency='dc-streetcar'
####logging test
logger = logging.getLogger()
logger.setLevel(logging.INFO)
path = os.getcwd()
#logging handler
fh = TimedRotatingFileHandler(path+'/temp2/steetcar_log.log', when = 'h',interval = 1)
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
######

route_id_to_NB_tag = { #Circulator route colours mapped onto NextBus Circulator route tags       
    "dc-streetcar":
        {
        "red": "h_route"
        }
    }

agencydict = {
    'dc-streetcar' : ['red']
    }

nextbus_url = 'streetcar api url' 

stopdic_loc = {}
stopdic_tag = {}

stopdic_loc[agency], stopdic_tag[agency] = get_stop_dict(agency)

def write_streetcar_trip_update(agency,tripsdict):
    """
    TODO: document
    # fix the no direction problem
    # fix the sequence number mismatch problem
    
     
    :param agency:
    :param tripsdict:
    return dictionary of myentity, key as enetity id ,value as the whole entity
    """
    filename = agency+'-'+'tripupdates'+ext

    vehiclelist = get_nextbus_agency_vl(agency) #now it should be vehicle info tuple

    # Feed Message
    fm = g.FeedMessage()
    
    ## Feed Header
    fm.header.gtfs_realtime_version = '2.0'
    fm.header.incrementality = g._FEEDHEADER_INCREMENTALITY.values_by_name['FULL_DATASET'].number #enum: full dataset
    count = 0

        for vehicle in vehiclelist: 
            #print "#",vehicle.vehicle_id
            if vehicle.route_id:
                r = route_id_to_NB_tag[agency][vehicle.route_id.lower()]
                #taglist = stopdic_tag.get(agency).get(r)
                if vehicle.direction_id is not None:
                    direction=vehicle.direction_id

                    if get_trip_id(tripsdict, vehicle.route_id, direction):
                        trip_id, tftup = get_trip_id(tripsdict, vehicle.route_id, direction)
                    else:
                        print "becuase of trip dict"
                        print tripsdict, vehicle.route_id, vehicle.direction_id
                        break
                                   
                    seq,current_stop_id,not_near_stop= current_stop_sequence2(agency, trip_id, vehicle.lat, vehicle.lon)
                        #print(vehicle,trip_id,seq,current_stop_id,not_near_stop)
                    if not_near_stop==0:
                            #print("route:",r, trip_id)
                        taglist = stopdic_tag.get(agency).get(r)
                            #
                        has=0
                        list_id_seq = get_seq_dict(agency).get(int(trip_id))
                        if int(current_stop_id) in list_id_seq:
                            idx = list_id_seq.index(int(current_stop_id))
                        else:
                            idx = 0   
                        count = get_predict(agency,has,idx,list_id_seq,taglist,r,vehicle,trip_id,tftup,fm,count,direction)
                        '''
                            while has==0 and idx<len(list_id_seq):
                                #print("idx",idx,len(list_id_seq))
                                stop_id = list_id_seq[idx]
                                #print(int(stop_id), idx, list_id_seq)
                                if int(stop_id) in taglist:
                                    tag = taglist.get(int(stop_id))            
                                    url = nextbus_url + 'predictions&a=' + agency +'&r=' + r + '&s=' + tag
                                    try:
                                        pre_dict = u.get_json_from_url(url).get('predictions')
                                    except:
                                        print "this stop no prediction"
                                        pre_dict=[]
                                        pass
                                    if 'direction' in pre_dict:
                                        p = pre_dict.get('direction').get('prediction')
                                        if isinstance(p,dict):
                                            if int(p.get('vehicle'))==int(vehicle.vehicle_id):
                                                #print("stopid:",stop_id)
                                                #print("route_id",r)
                                                #print("trip_id:",trip_id)
                                                #print p
                                                #print(start_time(agency,r,p.get('dirTag'),p.get('block'),tftup))
                                                get_in_feed(fm,trip_id,tftup,vehicle,agency,r,p,idx,stop_id)
                                                
                                                count+=1
                                                has=1
                                                break
                                            else:
                                                    idx+=1
                                        else:
                                            
                                            for p in [0:2]:
                                                #print p.get('vehicle')
                                                if int(p.get('vehicle'))==int(vehicle.vehicle_id):
                                                    #print("stopid:",stop_id)
                                                    #print("route_id",r)
                                                    #print("trip_id:",trip_id)
                                                    #print p
                                                    #print(start_time(agency,r,p.get('dirTag'),p.get('block'),tftup))
                                                    get_in_feed(fm,trip_id,tftup,vehicle,agency,r,p,idx,stop_id)
                                                    
                                                    count+=1
                                                    has=1
                                                    break
                                                else:
                                                    idx+=1
                                    else:
                                        print "no prediction"
                                else:
                                    idx+=1
                        '''       
                    else:
                        print "not_near_stop"
                        logger.info("not_near_stop")
                else:
                    print "!no direction",vehicle
                    logger.info("!no direction")
                        
                        #print(get_dir(agency,vehicle))
            else:
                print "no route id"
                logger.info("no route id" )                   
            
        
    ## Feed Header; updated here as opposed to above to be more recent
    fm.header.timestamp = int(time.time())

    print '    Wrote', count, 'vehicles pos and trip update updated with complete AVL info of', agency.replace('-', ' ')+'.'
    st='    Wrote '+str(count)+' vehicles pos and trip update updated with complete AVL info of '+str(agency)
    logger.info(st)
    logger.info(' ')
    f = open(scriptloc+'\\'+filename, "wb")
    f.write(fm.SerializeToString())
    f.close()
    #print 'Wrote .pb file to script location.'
    
    newpath = webloc+filename
    shutil.copyfile(os.path.join(scriptloc, filename), newpath)


def get_predict(agency,has,idx,list_id_seq,taglist,r,vehicle,trip_id,tftup,fm,count,direction):
    while has==0 and idx<len(list_id_seq):
        #print("idx",idx,len(list_id_seq))
        stop_id = list_id_seq[idx]
        #print(int(stop_id), idx, list_id_seq)
        if int(stop_id) in taglist:
            tag = taglist.get(int(stop_id))            
            p = get_predict_from_url(agency,r,tag)
            if p:
                if isinstance(p,dict):
                    if p.get('vehicle')==vehicle.vehicle_id:
                        get_in_feed(fm,trip_id,tftup,vehicle,agency,r,p,idx,stop_id,direction)          
                        #print(      entity.trip_update.stop_time_update.stop_sequence)
                        count+=1
                        has=1
                        break
                    else:
                        idx+=1
                else:                    
                    for p in p[0:2]:
                        if p.get('vehicle')==vehicle.vehicle_id:
                            get_in_feed(fm,trip_id,tftup,vehicle,agency,r,p,idx,stop_id,direction)                  
                            #print(      entity.trip_update.stop_time_update.stop_sequence)
                            count+=1
                            has=1
                            break
                        else:
                            idx+=1
            else:
                idx+=1
                
        else:
            idx+=1  
    #logger.info(entity.id,entity.trip_update.trip.trip_id,entity.trip_update.stop_time_update.stop_sequence,entity.trip_update.trip.start_time)              
    
    return count
    
def get_predict_from_url(agency,r,tag):
    url = nextbus_url + 'predictions&a=' + agency +'&r=' + r + '&s=' + tag
    try:
        pre_dict = u.get_json_from_url(url).get('predictions')
    except:
        print "this stop no prediction"
        logger.info("this stop no prediction")
        pass
        pre_dict=[]
    if 'direction' in pre_dict:
        p = pre_dict.get('direction').get('prediction')
    else:
        p=[]
        print "no prediction"
        logger.info("no prediction")
    return p

def get_in_feed(fm,trip_id,tftup,vehicle,agency,r,p,idx,stop_id,direction):
    myentity = fm.entity.add()
    myentity.id = vehicle.vehicle_id+'_'+vehicle.route_id+'_'+trip_id
    
    myentity.trip_update.timestamp = int(time.time())
    myentity.trip_update.trip.trip_id=trip_id
    myentity.trip_update.trip.route_id=vehicle.route_id
    myentity.trip_update.trip.direction_id=direction
    myentity.trip_update.trip.start_time=start_time(agency,r,p.get('dirTag'),p.get('block'),tftup)
    myentity.trip_update.trip.start_date=str(get_service_day())
    #myentity.trip_update.trip.schedule_relation
    myentity.trip_update.vehicle.id=myentity.trip_update.vehicle.label=vehicle.vehicle_id
    #myentity.trip_update.stop_time_update
    stu = myentity.trip_update.stop_time_update.add()
    stu.stop_sequence=idx
    stu.stop_id=str(stop_id)
    stu.arrival.time=int(int(p.get('epochTime'))/1000)
    #stu.schedule_relationship=
    print(myentity.id,stu.stop_sequence,myentity.trip_update.trip.start_time)
    st=myentity.id+' '+str(stu.stop_sequence)+' '+myentity.trip_update.trip.start_time
    logger.info(st)

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

def get_dir(agency,vehicle):
    
    rou = route_id_to_NB_tag[agency][vehicle.route_id.lower()]
    tag_list = stopdic_tag[agency][rou]
    for _,v in tag_list.iteritems():
        p=get_predict_from_url(agency,rou,v)
        if p:
            if isinstance(p,dict):
                if p.get('vehicle')==vehicle.vehicle_id:
                    dirtag=p.get('dirTag')
                    direction_id = dir_id_dict[agency][vehicle.route_id.lower()+dirtag[0]]
                    #trip_id, tftup = get_trip_id(tripsdict[agency], vehicle.route_id, vehicle.direction_id)
                    return direction_id
                    break
                else:
                    continue
            else:
                for p in p:
                    if p.get('vehicle')==vehicle.vehicle_id:
                        dirtag=p.get('dirTag')
                        print(vehicle.route_id.lower()+dirtag[0])
                        
                        route_tag=vehicle.route_id.lower()+dirtag[0]
                        
                        direction_id = dir_id_dict[agency][route_tag]

                        return direction_id
                        break
                    else:
                        continue
    