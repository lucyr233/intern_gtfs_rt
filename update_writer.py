#!/usr/bin/env python2.7.3

# -------------------------------------------------------------------------------    
# Name:        realtime gtfs for circulator from source bishop peak
# Author:      huixin rao
# Created:     2018/01/31
# -------------------------------------------------------------------------------

from __future__ import division
import os, sys, time, datetime
from ddot_realtime import get_day_trips
from vechicleinfo_to_proto import recording
import logging
from logging import FileHandler

scriptloc = 'file location'
agency = 'dc-circulator'
webloc = 'web location'

todaytripsdict ={}
previous={}

####logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
path = os.getcwd()
#logging handler
fh = FileHandler(path+'/temp/day_trip.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
######

def main():
    print scriptloc
    run_trip_update(30,minutes=0,) # if want to keep running, minutes=0, if certain time length, minutes= time length

def run_trip_update(period, minutes):          # writes both pos and alerts  # subsititute for ru_sa
    i = s = 0     
    
    con = True if not minutes else False
    while s < minutes*60 or con:
        
        global todaytripsdict
        global previous
        
        duration = 0
        print 'Iteration', str(i+1)+';', s, 'seconds since start.'
        inittime = time.time()
        
        if not todaytripsdict:
            todaytripsdict = get_day_trips(agency)
            print("run trips dictionary")
            logger.info(datetime.date.today().strftime('%Y%m%d') + ': ' + ', '.join(map(str, todaytripsdict.keys())))
            
        elif datetime.datetime.now().hour==5 and datetime.datetime.now().minute>58:#renew tripdict at five everyday
            todaytripsdict = get_day_trips(agency)
            logger.info(datetime.date.today().strftime('%Y%m%d') + ': ' + ', '.join(map(str, todaytripsdict.keys())))
            
            previous={}
            print("renew everyday")
           
        if todaytripsdict is not None: 
            
            try: 
                previous = recording(todaytripsdict,previous)
            except IOError:
                print("IOError")
                pass
        
        duration += (time.time() - inittime)
        print '        Time elapsed: '+'{0:3.3f}'.format(duration)+' seconds. Terminated at '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+"."
        print
        i+=1
        if duration<period:
            # script should sleep enough to reach period, not sleeping period blankly.
            sleeptime = period-duration
            s += int(sleeptime + duration)       # increment should be equal to period.
            time.sleep(sleeptime)
            
if __name__ == "__main__":
    main()