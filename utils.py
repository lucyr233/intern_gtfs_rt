
from __future__ import division
import time, datetime, operator, urllib, json, urllib2,requests

#### FUNCTIONS ####

### General ###
def get_json_from_url(url):
    """
    Read JSON at url and return a Python object based on JSON structure.
    e.g. JSON obj -> dict... JSON array -> list... etc.
    :param url: str of a URL where a JSON obj is located.
    :returns a Python obj representation of the JSON file. Usually a dict.
    """
    #u = urllib.urlopen(url)
    #u = urllib2.urlopen(url)
    #using urllib packages caused hanging for a really long time in some cases
    obj=requests.get(url).json()
    #obj = json.loads(u.read().decode())

    return obj


def get_service_day():
    """
    Returns today's service day.
    Assumes a 5am end/start to a service day.
    :param now: a datetime object. If not provided now() is used.
    :returns an 8-digit int representing today's service day.
    """
    now = datetime.datetime.now()
    date = date_to_basic(now)
    #date_to_basic(now)
    if now.hour < 5:
        return date-1
    return date_to_basic(now)
    
def get_unix_time(dt=datetime.datetime.now(), t='int'):
    """Takes datetime object and returns the 
    epoch time, i.e. seconds since Jan 1 1970"""
    f = time.mktime(dt.timetuple()) + dt.microsecond / 1E6
    return int(f) if t == 'int' else None

def string_to_date(num):
    """
    Take yyyymmdd string or int and convert it to
    a datetime.date object.
    
    :param num: an 8-digit int or string in the format yyyymmdd.
    :returns a datetime.date object.
    """
    if not isinstance(num, str) : num = str(num)
    
    assert len(num) == 8
    
    return datetime.date(int(num[0:4]), int(num[4:6]), int(num[6:8]))

def get_date(string):
    return datetime.datetime.strptime(string, '%m/%d/%Y %H:%M:%S')

def string_to_dt(timestring):
    """
    Return a datetime obj where the date component is
    given by datestring (int or str) in the format yyyymmdd
    and the time component is given by timestring
    in the 24H format HH:MM:SS.
    
    :param timestring: an 8 char string representation of a time, separated by ':'.
    :param datestring: optional. 8-digit in yyyymmdd format. If not supplied assumes today. Can be int or str.
    :returns a datetime object.
    """
    datestring = datetime.datetime.today().strftime("%Y%m%d")

    #if isinstance(datestring, datetime.date): datestring = date_to_basic(datestring)
    #if not isinstance(datestring, str): datestring = str(datestring)
    
    assert len(datestring) == 8
    assert len(timestring) == 8
    
    year = int(datestring[0:4])
    month = int(datestring[4:6])
    day = int(datestring[6:8])
    
    hours = int(timestring[0:2])
    minutes = int(timestring[3:5])
    seconds = int(timestring[6:8])
    
    if hours >= 24:             # Because GTFS 26:00:00 is 2 AM the following day, and 24:00:00 is midnight.
        #day += 1
        #hours = hours - 24 # this is not working for the end of the month or the end of the year
        newday = datetime.datetime(year,month,day)+ datetime.timedelta(days=1)
        year = newday.year
        month = newday.month
        day = newday.day
        hours = hours-24

    return datetime.datetime(
                      year,
                      month,
                      day,
                      
                      hours,
                      minutes,
                      seconds
                      )

def date_to_basic(t='int'):
    """
    Convert datetime obj to str or int.
    If no dateobj arg provided, returns today.
    Default returns int. If t='str' returns str.
    
    :param dateobj: a datetime.datetime or datetime.date object.
                    If not provided assumes today.
    :param t: Can be either 'int' or 'str'. int by default.
    :returns int or str, 8-char in the format yyyymmdd.
    """
    dateobj=datetime.date.today()
    string = dateobj.strftime('%Y%m%d')
    return string if t =='str' else int(string)

def lrange(num1, num2 = None, step = 1):
    """
    Custom version of xrange, for larger numbers. 
    Designed for use in very long iterations,
    such as those involving Posix time ranges with
    total steps of 50k seconds or more.

    Use as you would use xrange.

    @author: Ricardo Cardenes
    @see: https://stackoverflow.com/questions/2187135
    """
    op = operator.__lt__

    if num2 is None:
        num1, num2 = 0, num1
    if num2 < num1:
        if step > 0:
            num1 = num2
        op = operator.__gt__
    elif step < 0:
        num1 = num2

    while op(num1, num2):
        yield num1
        num1 += step

#----------------------------------------------------------------------------------------------------------------------------#

def main():
    print 'Run something else.'

if __name__ == '__main__':
    main()