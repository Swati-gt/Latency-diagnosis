#!/usr/bin/env python

import pg as pgsql
import sys
import os
import datetime
import math
import numpy as np
#import pandas as pd
import time

#begin_time = raw_input("Enter the start date in Y-M-D H:M:S format")
#end_time = raw_input("Enter the end date in Y-M-D H:M:S format")
begin_time = '2014-02-01 00:00:00'
#end_time = '2013-10-4 23:59:59'

length_data=0.0
digital_array=[]
analog_array=[]

dict_of_server={'ITALY':'143.225.229.254','LAX':'38.98.51.12','AMS':'213.244.128.169',"JNB":'196.24.45.146','NBO':'197.136.0.108','HND':'203.178.130.210','SYD':'175.45.79.44','INDIA':'180.149.52.20', 'BRAZIL':'143.54.2.20',\
 'GOOG':'8.8.8.8'}

#latency_array=[]
def sqlconn():
        try:
                #print "try"
                conn = pgsql.connect(dbname='swati',user='',passwd='')
        except:
                print "Could not connect to sql server"
                sys.exit()
        return conn

def new_run_data_query(conn,routerid,server): # Single router and server list
        if conn == None:
                 conn = sqlconn()

        #command construction
        results=[]

        #s= dict_of_server[server]
        cmd = "select deviceid, srcip, dstip, hop, ip, rtt, eventstamp from traceroute \
               where deviceid='%s' and dstip='%s' and eventstamp >= '%s'  order by eventstamp,hop limit 25" \
               %(routerid,server,begin_time)
        print "\ncommand is\n", cmd

        try:
        	res=conn.query(cmd)
        except:
                print "Couldn't run %s\n"%(cmd)
                #continue
               #print "result",res.getresult()
        results.append(res.getresult()) # list of tuples
        return results



def run_data_query(conn,routerid,server): # Single router and server list
        if conn == None:
                 conn = sqlconn()
        
	#command construction
        results=[]
       
	for srvr in server:
	       s= dict_of_server[srvr]
               cmd = "select deviceid, srcip, dstip, hop, ip, rtt, eventstamp from traceroute \
               where deviceid='%s' and dstip='%s' and eventstamp >= '%s'  order by eventstamp,hop limit 25" \
               %(routerid,s,begin_time)
               print "\ncommand is\n", cmd

               try:
                       res=conn.query(cmd)
               except:
                       print "Couldn't run %s\n"%(cmd)
		       continue
               #print "result",res.getresult()
               results.append(res.getresult()) # list of tuples
        return results

