#!/usr/bin/env pytho

import sys
import time
import pgsql as sql
import pandas as pd
import csv
import itertools
import numpy
from pylab import plot,show
from numpy import vstack,array
from numpy.random import rand
from scipy.cluster.vq import kmeans,vq
import itertools
import numpy as np
import os
import errno

# Third party modules #
import numpy, scipy, matplotlib, pandas
import  matplotlib.pyplot as plt
#matplotlib.use('Agg')
#plt.ioff()
import scipy.cluster.hierarchy as sch
import scipy.spatial.distance as dist


conn = sql.sqlconn();
list_server = [ '143.225.229.254', '38.98.51.12','213.244.128.169', '196.24.45.146','197.136.0.108','203.178.130.210','175.45.79.44','180.149.52.20', '143.54.2.20', '8.8.8.8'];

#list_server = ['128.48.110.150'];
#list_server = ['143.225.229.254'];
#list_server = ['4.71.254.166']; # Most of the time no data to handle
#list_server = ['38.98.51.12'];

#list_server = ['213.244.128.169'];
#list_server = ['196.24.45.146'];
#list_server = ['197.136.0.108'];
#list_server = ['203.178.130.210'];

#list_server = ['175.45.79.44'];
#list_server = ['180.149.52.20'];

#list_server = ['143.54.2.20'];
#list_server = ['8.8.8.8'];

#Rtt arrays
data_array=[]
lm_array=[]

rtt_array=[]

#Last-mile arrays
lm_latency=[]


analog_array=[]
lm_analog_array=[]
digital_array=[]
lm_digital_array=[]

rtt_analog_array=[]
fread = open('testrouter.txt')


def last_mile( dict_digital_array, lm_digital_array,routerid):
	print "In last-mile function\n"
	lm_count=0
	array=[]
	lm_array=[]
	matrix = pd.DataFrame(index=['0'],columns=routerid)
	matrix=matrix.fillna(0)
	
	for key in sorted(dict_digital_array.iterkeys()):
	
                df = dict_digital_array[key]
                #print "Router",key
                server = df.columns
		lm_array=lm_digital_array[key]
                for idx in df.index:
                	if df.loc[idx,'ITALY'] ==1 and df.loc[idx,'LAX']==1 and df.loc[idx,'AMS']==1 and df.loc[idx,'JNB']==1 and df.loc[idx,'SYD']==1 \
			   and df.loc[idx,'NBO']==1 and df.loc[idx,'HND']==1 and df.loc[idx,'INDIA']==1 and df.loc[idx,'BRAZIL']==1 and df.loc[idx,'GOOG']==1:
				if lm_array[idx]==1:
					lm_count +=1
				 			
		matrix.loc['0',key]=lm_count			
		#print "Router",key, 'last-mile count',lm_count

	print "last-mile matrix"
	for index in matrix.index:
		print index, matrix.ix[index]
	print "End of last-mile function"
	#sys.exit()
	return matrix

def get_routerid(fread):
	routerid=[]
	for router in fread:
		router = router.strip()
		routerid.append(router)
	return routerid

def make_sure_path_exists(path):
	print 'Inside directory check'
	try:
		if not os.path.isdir(path):
			print "Creating directory",path
			os.makedirs(path)
	except OSError as exception:
		if exception.errno!= errno.EEXIST:
			print 'Different exception raisedi!'
	print 'End of directory check'
	#sys.exit()

def spike_event(routerid,dict_digital_array):
	print "In spike event"
	for key in sorted(dict_digital_array.iterkeys()):
		print "Router",key
		df = dict_digital_array[key]
		event={}
		noevent={}
		#print len(dict_digital_array.keys())
		#sys.exit()
	
		tmp1 = key.split(':')
                tmp2 = ''.join(tmp1)

		path ='./textfiles/'
		path1 = './textfiles/noevent/'

		make_sure_path_exists(path)
		make_sure_path_exists(path1)

		#filename='./textfiles/' + str(tmp2) +'_dict.txt'
		#filename='./textfiles/' + str(tmp2) +'_dict.txt'
		filename = path + str(tmp2) +'_dict.txt'
		filename1= path1 + str(tmp2) +'_dict.txt'

		f=open(filename,'w')
		f1=open(filename1,'w')
		
		for index in df.index:	
			s = df.ix[index] # s is a series with server names as index
			for idx in s.index:
				if s[idx]==1:
					#print index,idx
					if event.has_key(index):
						event[index].append(idx)
					else:
						event[index]=[]
						event[index].append(idx)
				else:
					if s[idx]==0:
						#print index,idx
                                        	if noevent.has_key(index):
                                                	noevent[index].append(idx)
                                        	else:
                                                	noevent[index]=[]
                                                	noevent[index].append(idx)			
		print "Key :",key
		for key in sorted(event.iterkeys()):
			print key,'->',event[key]

		f.write(str(event))
		f.close()

		f1.write(str(noevent))
		f1.close()

	print "End of spike event function"

def anomaly_detection(routerid,dict_digital_array):
	print "In anomaly detection"
	timeseries = sql.get_timeseries()
	total_original =0
	total_change =0
	for key in sorted(dict_digital_array.iterkeys()):
		df = dict_digital_array[key]
		#print df.index
		#print df
		#print df.columns
		#print df.index
		#sys.exit()
		print "Router",key
		server = df.columns
		#sys.exit()
		for col in df.columns:
		#for col in ['LAX']:
			for idx in df.index:
				#print "df",idx,df.loc[idx,col]
				if df.loc[idx,col]!=-999:
					total_original+=df.loc[idx,col]
				index= timeseries.index(idx)
				if index ==0:
					prev_value=df.loc[idx,col]
				else:
					prev_value=df[col].iget(index-1)
				if index == (len(timeseries)-1):
					next_value = df.loc[idx,col]
				else:
					next_value =df[col].iget(index+1)	
				if (next_value==1 or prev_value ==1) and df.loc[idx,col]!=-999 \
					and df.loc[idx,col]!=0:
					df.loc[idx,col]=1
				else:
					if df.loc[idx,col]==0:
						df.loc[idx,col]=0
					else:
						df.loc[idx,col]=-999
				#print "prev",prev_value,"next",next_value	
			'''	
				if df.loc[idx,col]!=-999:
					total_change+=df.loc[idx,col]
				print df.loc[idx,col]
			print total_original,total_change
			'''
	print "End of anomaly detection function"
	#spike_event(routerid,dict_digital_array)
	return dict_digital_array


def main():	
	routerid = get_routerid(fread)
	for rtr in routerid:
		print "Router in main function:%s"%(rtr)
		data_array.append(sql.run_data_query(conn,rtr,list_server))
		lm_array.append(sql.lm_data_query(conn,rtr))
		if not data_array:
			print "No rows returned";
			continue
		#analog_array,len_data_array= sql.fill_zeros(data_array)
		timeseries = sql.get_timeseries()
	
	print "Data array length",len(data_array)
	analog_array,len_data_array= sql.fill_zeros(data_array)
	lm_analog_array,len_lm_array= sql.fill_zeros(lm_array)
	
	# Find routers having data
	routerid=[]
	index=0
	prev_router=data_array[0][0][0][0].upper()
	print "previous router", prev_router
	#sys.exit()
	routerid.append(prev_router)
	for data in data_array:
		print "Number of routers having data", len(data)
		for dt_data in data:
			if len(dt_data)==0:
				print "inside break"
				continue
			print "Length",len(dt_data)
			#print "dt_data value index",dt_data
			print "Router id",dt_data[0][0].upper()
			if dt_data[0][0].upper() != prev_router.upper():
				routerid.append(dt_data[0][0].upper())
				prev_router = dt_data[0][0].upper()
			#break
	#print routerid
	for i in range(len(routerid)):
		print routerid[i]

	print "New Routers having data", len(routerid)
	if len(routerid) == 0:
		print "No router has data"
		sys.exit()

	dict_rtt_array,dict_digital_array = sql.set_threshold(analog_array,len_data_array,routerid)

	'''
	print "row in analog lm array"
	for row in (lm_analog_array):
		print row
	'''
	lm_rtt_array,lm_digital_array = sql.set_lmthreshold(lm_analog_array,len_data_array,routerid)
	
	'''
	print "LM rtt array after threshold"
	for index in lm_rtt_array.index:
		print lm_rtt_array.ix[index]
	'''

	returned_dict = anomaly_detection(routerid,dict_digital_array)
	
	## LAST-MILE COMPUTATION
	last_mile_matrix = last_mile(dict_digital_array, lm_digital_array,routerid)
	
	'''if returned_dict ==dict_digital_array:
		print "yes"
	else:
		print "No"
	'''

	spike_event(routerid,dict_digital_array)
	#sys.exit()


if __name__=="__main__":
	main()
