Project Title

this project provides live AVL information for D.C. circulator and streetcar to Google map, enhances riders' experience by updating the real time vehicle information.

Prerequisites
python 2.7
protocol buffer 
google transit realtime gtfs 

Authors
Ehab Ebeid 
Huixin Rao

The program fetches AVL information from provider(nextbus) every 30s, modifies the information in order to match the static GTFS and input data in protocol buffer format, then push to Google transit. It handles data for about 40 vehicles, 80 trips and 200 stops every iteration and runs 24/7.

Note: this code cannot be run since the provider API information is not provided. 

--python2 circulator_streetcar_writer.py
