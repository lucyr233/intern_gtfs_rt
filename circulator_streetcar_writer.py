#!/usr/bin/env python2.7.3
from __future__ import division
import os, sys, time, shutil, datetime
#import google.transit.gtfs_realtime_pb2 as g  # @UnresolvedImport
from ddot_realtime_clean import get_day_trips
from circulator_streetcar_trip_update import circulator_trip_update

scriptloc = file_location

agencies = ['dc-circulator','dc-streetcar']

webloc = webloc

trip_start_time={}
tripsdict={}


def main():
    print scriptloc
    run_trip_update(30,minutes=0,) # if want to keep running, minutes=0, if certain time length, minutes= time length

def run_trip_update(period, minutes):          # writes both pos and alerts  # subsititute for ru_sa
    i = s = 0     
    
    con = True if not minutes else False
    while s < minutes*60 or con:

        global previous
        global tripsdict
        duration = 0
        print 'Iteration', str(i+1)+';', s, 'seconds since start.'
        inittime = time.time()
        
        for agency in agencies:
            tripsdict[agency]=[]
            if not tripsdict[agency]:
                tripsdict[agency] = get_day_trips(agency)
                        
            elif datetime.datetime.now().hour==5 and datetime.datetime.now().minute>58:#renew tripdict at five everyday
                tripsdict[agency] = get_day_trips(agency)
                print("renew everyday")
            
            if tripsdict[agency] is not None: 
                try: 
                    circulator_trip_update(tripsdict[agency],agency)
                except IOError:
                    print("IOError")
                    time.sleep(30)
            else:
                print (datetime.datetime.now(),tripsdict[agency])
            

        duration += (time.time() - inittime)
        print '        Time elapsed: '+'{0:3.3f}'.format(duration)+' seconds. Terminated at '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+"."
        print
        i+=1
        if duration<period:
            # script should sleep enough to reach period, not sleeping period blankly.
            sleeptime = period-duration
            s += int(sleeptime + duration)       # increment should be equal to period.
            time.sleep(sleeptime)
        # implied else is: in the horrifying case that duration is more than period, iterate immediately. Shame.

if __name__ == "__main__":
    main()