#!/usr/bin/env python2.7.3
from __future__ import division
import os, sys, time, shutil, datetime
import google.transit.gtfs_realtime_pb2 as g  # @UnresolvedImport
from ddot_realtime import get_day_trips
from streetcar_trip_update import write_streetcar_trip_update
scriptloc = 'file location'
ext = '.pb'#'.txt'#
agency = 'dc-streetcar'

webloc = "web location"

trip_start_time={}
tripsdict={}
previous={}

def main():
    print scriptloc
    run_streetcar_update(30,minutes=0,) # if want to keep running, minutes=0, if certain time length, minutes= time length

def run_streetcar_update(period, minutes):          # writes both pos and alerts  # subsititute for ru_sa
    i = s = 0     
    
    con = True if not minutes else False
    while s < minutes*60 or con:

        global previous
        global tripsdict
        duration = 0
        print 'Iteration', str(i+1)+';', s, 'seconds since start.'
        inittime = time.time()

        try: 
            if not tripsdict:
                tripsdict = get_day_trips(agency)
                    
            elif datetime.datetime.now().hour==5 and datetime.datetime.now().minute>58:#renew tripdict at five everyday
                tripsdict = get_day_trips(agency)

                print("renew everyday")

                previous=0

            if tripsdict is not None: 
                write_streetcar_trip_update(agency,tripsdict)
            else:
                print (datetime.datetime.now(),tripsdict)
        except IOError:
            print("IOError")
            time.sleep(30)

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