#!/usr/bin/env python

import pg as pgsql
import sys
import os
import datetime
import math
import numpy as np
import pandas as pd
import time
#from bigfloat import *

#begin_time = raw_input("Enter the start date in Y-M-D H:M:S format")
#end_time = raw_input("Enter the end date in Y-M-D H:M:S format")
#begin_time = '2013-10-2 00:00:00'
#end_time = '2013-10-4 23:59:59'
begin_time = '2014-02-01 00:00:00'
end_time = '2014-02-28 23:59:59'
#begin_time = '2012-09-01 00:00:00'
#end_time = '2012-09-30 23:59:59'
length_data=0.0
digital_array=[]
analog_array=[]

list_server = [ '143.225.229.254', '38.98.51.12','213.244.128.169', '196.24.45.146','197.136.0.108','203.178.130.210','175.45.79.44','180.149.52.20', '143.54.2.20', '8.8.8.8'];
#list_server = [ '38.98.51.12'];

dict_of_server={'143.225.229.254':'ITALY','38.98.51.12':'LAX','213.244.128.169':'AMS','196.24.45.146':'JNB','197.136.0.108':'NBO','203.178.130.210':'HND','175.45.79.44':'SYD','180.149.52.20':'INDIA', '143.54.2.20':'BRAZIL',\
 '8.8.8.8':'GOOG'}

server_names=['ITALY','LAX','AMS','JNB','NBO','HND','SYD','INDIA','BRAZIL','GOOG']

#latency_array=[]

def sqlconn():
	try:
		#print "try"
		conn = pgsql.connect(dbname='bismark_openwrt_live_v0_1',user='',passwd='')
	except:
		print "Could not connect to sql server"
		sys.exit()
	return conn

def get_timezoneinfo(routerid,):
	print "In get_timezoneinfo\n";

def run_data_query(conn,routerid,server):
	if conn == None:
		 conn = sqlconn()
	#command construction
	toolid = 'FPING'
	results=[]
	for srvr in server:
		cmd = "select deviceid,average, eventstamp,dstip from m_rtt \
	       where deviceid='%s' and dstip='%s' and toolid='%s' and eventstamp >='%s' and eventstamp <= '%s' order by eventstamp asc" \
	       %(routerid,srvr,toolid,begin_time,end_time)
		print "\ncommand is\n", cmd
		try:
			res=conn.query(cmd)
		except:
			print "Couldn't run %s\n"%(cmd)
		#print "result",res.getresult()
		results.append(res.getresult()) # list of tuples
	return results

def lm_data_query(conn,routerid):
	if conn == None:
		 conn = sqlconn()
	#command construction
	toolid = 'FPING'
	results= [] 
	cmd = "select deviceid,average, eventstamp, dstip from m_lmrtt \
	       where deviceid='%s'  and eventstamp >='%s' and eventstamp <= '%s' order by eventstamp asc" \
	       %(routerid,begin_time,end_time)
	print "\ncommand is\n", cmd
	try:
		print "Last-mile data query results execution"
		res=conn.query(cmd)
	except:
		print "Couldn't run %s\n"%(cmd)
	
	results.append(res.getresult()) # list of tuples
	return results

def get_timeseries():
	timeseries=[]
	str_timeseries=[]
	theday = datetime.datetime.strptime(begin_time, '%Y-%m-%d %H:%M:%S')
	begin= datetime.datetime.strptime(begin_time, '%Y-%m-%d %H:%M:%S')
	end= datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
	samples = abs((end - begin).days)
	samples = (samples + 1 ) * 24 * 6
	print "sample", samples
	for i in range(samples):
               	start_date = theday + i * datetime.timedelta(seconds=600)
		timeseries.append(start_date)
		str_timeseries.append(start_date.strftime('%Y-%m-%d %H:%M:%S'))
		#sys.exit()		
	#print str_timeseries
	#sys.exit()
	return str_timeseries


def fill_zeros(data_array):
	print "Fill zeros Function"
	#print "Below insertion function", data_array
	rtti=0
	len_data_array=[]
	theday = datetime.datetime.strptime(begin_time, '%Y-%m-%d %H:%M:%S')
        latency_array = []
	digital_array = []
	begin= datetime.datetime.strptime(begin_time, '%Y-%m-%d %H:%M:%S')
	end= datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
	samples = abs((end - begin).days)
	samples = (samples + 1 ) * 24 * 6
	#samples = samples +1;
	#samples = (samples + 1 ) * 6
	print "sample", samples
	for data in data_array:
		print "length data", len(data)
		#sys.exit()
		for dt_data in data:
			print "length of dt_data",len(dt_data)
			#print "dt_data",(dt_data[0])
			if len(dt_data)==0: #Means no rows were returned
				print "continue"
				continue
			len_data_array.append(len(dt_data))
			#print "dt_data",(dt_data)
			#print "dt_data Server and rtti",(dt_data[0][3]),rtti
			print "dt_data  MAC and server",(dt_data[rtti][0]), dt_data[rtti][3]
			#sys.exit()
			#print "dt_data",(dt_data)
			print "Length array",len_data_array,len(len_data_array)
		
	        	for i in range(samples):
				print "Value of i and rtti",i,rtti
        	        	start_date = theday + i * datetime.timedelta(seconds=600)
                		end_date = start_date + datetime.timedelta(seconds=600)
                		print "start_date, end_date",start_date,end_date;
                		if rtti < len(dt_data):
					
					#print "dt_data",(dt_data[rtti])
					duration = datetime.datetime.strptime(dt_data[rtti][2], '%Y-%m-%d %H:%M:%S')
					print 'Duration inside rtti <len(dt_data) and start_date and  end_date',duration,start_date, end_date
                        		while rtti < len(dt_data) and duration< start_date: # Within same time period consider first and ignore  rest 
                        			print rtti,"inwhile"
                                		#print data_array[rtti][1]
                               			rtti += 1
						#print "dt_data", dt_data
                        			if rtti == len(dt_data):
                                			#break
							continue
						#print "Duration", duration,start_date
                                		#print 'rtti',rtti, dt_data[rtti],len(dt_data)
                                		#print dt_array[rtti][2]
						#sys.exit()
						duration= datetime.datetime.strptime(dt_data[rtti][2], '%Y-%m-%d %H:%M:%S')
				
                        		if rtti == len(dt_data):
                                		#break
						continue
                        		#if start_date <= data_array[rtti][2] <= end_date:
                        		if start_date <= duration < end_date:
						print "Inside if start_date <=dration and <end_date",duration
						if (dt_data[rtti][1] >3000):
							print " Outlier > 3000"
							tup = (dt_data[0][0],0.0, dt_data[rtti][2],dt_data[rtti][3])
							latency_array.append(tup)
							#tup = (dt_data[0][0],0, dt_data[rtti][2],dt_data[rtti][3])
							#digital_array.append(tup)
							rtti += 1
							duration= datetime.datetime.strptime(dt_data[rtti][2], '%Y-%m-%d %H:%M:%S')
						else:
                                			#latency_array.append(data_array[rtti][0])
                                			latency_array.append(dt_data[rtti])
							#tup = (dt_data[0][0],1, dt_data[rtti][2],dt_data[rtti][3])
						#	digital_array.append(tup)
							
                        	        		rtti += 1
                        		elif duration > end_date:
                        			print rtti,"elsif",end_date,duration
                                		#tup=(dt_data[rtti][0],0.0,start_date,dt_data[rtti][3]);
                                		tup=(dt_data[rtti][0],-999,start_date,dt_data[rtti][3]);
                                		latency_array.append(tup);
						#tup = (dt_data[0][0],0,end_date)
						#digital_array.append(tup)
                		else:
                        		#print router,rtti,"else",start_date,end_date
                        		#sys.exit(0) # Data finsihed but not the time duration
					#print "Length of data_array",len(data_array)
                        		#tup =(dt_data[0][0],0.0,start_date,dt_data[0][3])
                        		tup =(dt_data[0][0],-999,start_date,dt_data[0][3])
					print tup
					#sys.exit()
                        		latency_array.append(tup)
					#tup = (dt_data[0][0],0, start_date,dt_data[0][3])
					#digital_array.append(tup)
			print "dt_data Server and rtti",(dt_data[0][3]),rtti
			#sys.exit()
			print "check point data and samples required and latency_array length", len(dt_data), samples, len(latency_array), latency_array[-1]
			rtti=0
			counter = len(dt_data)
        		#while(counter!= samples):
        			#print "in while len"
        			#sys.exit(0);
			#print "Latency array", latency_array
			print " length of latency array", len(latency_array)
			print " End date", end_date
			#sys.exit()
			
			start = end_date
			print "check more # samples required", ( samples - len(dt_data))
			#sys.exit()
			
			#print (samples-len(latency_array))	
			for j in range (samples - len(latency_array)):
				print " for loop"
                		bg_date = start_date + j * datetime.timedelta(seconds=600)
	                	#latency_array.append((dt_data[rtti][0],0.0,bg_date,dt_data[0][3]))
	                	latency_array.append((dt_data[rtti][0],-999,bg_date,dt_data[0][3]))
				tup = (dt_data[0][0],0,bg_date)
				#digital_array.append(tup)
				
			print " length of latency array", len(latency_array)
        		print(len(latency_array))
	
	data_array=[];
	#print len(latency_array),len(len_data_array)
	for i in range(len(latency_array)):
		print latency_array[i]
	#sys.exit()
	return latency_array,len_data_array

def get_average(analog_array,len_data_array,routerid):
	total=0.0;
	print "Length of array in average function", len(analog_array)
	if len(analog_array) == 0:
		print "No Data to handle in get_average function"
		sys.exit()
	print "Analog array in Average function\n"
	for row in analog_array:
		print row
	#sys.exit()
	print "Length of data in  average function",(len_data_array[0])
	array=[]
	avg_array=[]
	idx=0
	print "length",len_data_array[idx]
	flag=0
	flag_data=0
	## Print Statement
	#print " Analog Array", analog_array
	print "len_data_array", len_data_array,len(len_data_array)
	print "routerid length",len(routerid)
	for router in routerid:
		print "Router",router.upper()
		#sys.exit()i
		for server in list_server:
			print "Server", server
			for latency in analog_array:
				#print "latency", latency[0]
				if server == latency[-1] and router.upper() == latency[0].upper():
					if latency[1]!=0 and latency[1]!=-999: # 0 means an outlier and -999 means missing measurement
						flag_data =1
						array.append(latency[1])
				else:
					#flag=1
					#break
					continue
				
			if(flag_data == 1):
				flag_data=0
				#print "Test value in array"
				for value in array:
					#print value,","
					total+=value
				print "idx server number in psql",idx	
				print "total value #ofdatapoints routerid", total, len_data_array[idx],router.upper()
				print "Length of array computed", len(array)
				#avg_value=float(total)/len_data_array[idx]
			avg_value =0.0
			if len(array)!=0:
				avg_value=float(total)/len(array)
		
			#####
			variance =0.0
			for i in range(len(array)):
				variance += (array[i] -avg_value) **2
			####
			#print "Variance",variance

			# Computation of STD
			variance_t = map(lambda x: (x - avg_value)**2, array)
			variance_total=0.0
			
			for value in variance_t:
				variance_total+=value
			print "variance_t",variance_total

			if len(array)!=0:
				variance_total=variance_total/len(array)
				std_t = math.sqrt(variance_total)
			#std = math.sqrt(variance/1411)
			print "Std", std_t
			print "Average",avg_value
			#sys.exit()
			idx+=1
			tup = (router,server,avg_value,std_t)
			avg_array.append(tup)
			array=[]
			total=0
			#print "Average array", avg_array
			#sys.exit()
	print "Average array", avg_array
	#sys.exit()
	return avg_array

def get_lmaverage(analog_array,len_data_array,routerid):
	total=0.0;
	print "Length of array in LM average function", len(analog_array)
	if len(analog_array) == 0:
		print "No Data to handle in get_average function"
		sys.exit()
	print "Analog array in Average function\n"
	for row in analog_array:
		print row
	#sys.exit()
	print "Length of data in  average function",(len_data_array[0])
	array=[]
	avg_array=[]
	idx=0
	print "length",len_data_array[idx]
	flag=0
	flag_data=0
	## Print Statement
	#print " Analog Array", analog_array
	print "len_data_array", len_data_array,len(len_data_array)
	print "routerid length",len(routerid)
	for router in routerid:
		print "Router",router.upper()
		#sys.exit()i
		for latency in analog_array:
			#print "latency", latency[0]
			if router.upper() == latency[0].upper():
				if latency[1]!=0 and latency[1]!=-999: # -999 means missing measurement and 0 means outlier
					flag_data =1
					array.append(latency[1])
			else:
				#flag=1
				#break
				continue
				
		if(flag_data == 1):
			#print "Test value in array"
			flag_data=0
			for value in array:
				#print value,","
				total+=value
			print "idx server number in psql",idx	
			print "total value #ofdatapoints routerid", total, len_data_array[idx],router.upper()
			print "Length of array computed", len(array)
			#avg_value=float(total)/len_data_array[idx]
		avg_value =0.0
		if len(array)!=0:
			avg_value=float(total)/len(array)
		
		# Computation of STD
		variance_t = map(lambda x: (x - avg_value)**2, array)
		variance_total=0.0
			
		for value in variance_t:
			variance_total+=value
		print "variance_t",variance_total

		if len(array)!=0:
			variance_total=variance_total/len(array)
			std_t = math.sqrt(variance_total)
		#std = math.sqrt(variance/1411)
		print "Std", std_t
		print "Average",avg_value
		#sys.exit()
		idx+=1
		tup = (router,avg_value,std_t)
		avg_array.append(tup)
		array=[]
		total=0
		#print "Average array", avg_array
		#sys.exit()
	print "Average array", avg_array
	#sys.exit()
	return avg_array





def set_lmthreshold (analog_array,len_data_array,routerid):
	print " Inside LM threshold function"	
	#print latency_array	
	timeseries= get_timeseries()
	
	avg_array = get_lmaverage(analog_array,len_data_array,routerid)

	router_avg_std_dic ={}
	for row in avg_array:
		router_avg_std_dic[row[0].upper()]=[float(row[1]),float(row[2])]
	
	for keys in sorted(router_avg_std_dic.keys()):
		print keys,'->',router_avg_std_dic[keys]
	#print router_avg_std_dic
	print "length of routerid",len(routerid), routerid
	digital_array=get_empty_matrix(timeseries,routerid)
	rtt_array = get_empty_matrix(timeseries,routerid)
	#sys.exit()
	flag_data=0
	for router in routerid:
		rtt=[]
		digital=[]	
		print "Router",router.upper()
		#sys.exit()
		for latency in analog_array:
			#print "latency analog array length", len(analog_array)
			#print latency
			#sys.exit()
			if router.upper() == latency[0].upper():
			#print "if statement"
				flag_data=1
				avg = router_avg_std_dic[router.upper()][0]
				std= router_avg_std_dic[router.upper()][1]
				#print avg, std 					
				#sys.exit()
				if (latency[1] > (avg +std)):
				#	print "Value Appened",latency[1]
					rtt.append(latency[1])
					digital.append(1)
				elif latency[1] == -999:
					rtt.append(-999)
					digital.append(-999)
				else:
					rtt.append(0.0)
					digital.append(0)
			else:
				continue
		if flag_data == 1 :
			flag_data=0
			print "checkpoint for analysis",router, (rtt)	
			print "rtt rray length",len(rtt), avg, std
			print "Flag router", router
			i=0
			for idx in rtt_array.index:
				if i <len(rtt):
					#print "rtt[i] value for matrix",rtt[i]
					rtt_array.loc[idx,router.upper()] = rtt[i]
					digital_array.loc[idx,router.upper()]=digital[i]
					i+=1
				else:
					break
			rtt=[]
			digital=[]
	print rtt_array,digital_array

	print "End of lm_threshold"
	#sys.exit()
	return rtt_array,digital_array





def get_empty_matrix(timeseries,routerid):
	uppercase=[]
	for router in routerid:
		#router_new.append(router.upper)
		print "Router", router
		uppercase.append(router.upper())
	print "case",uppercase
	#sys.exit()
	uppercase = list(set(uppercase))
	matrix = pd.DataFrame(index=timeseries, columns=uppercase)
	matrix = matrix.fillna(0)
	print matrix.columns
	#sys.exit()
	return matrix

		
def set_threshold (analog_array,len_data_array,routerid):
	#print " Latency array in threshold function"	
	#print latency_array	
	dict_of_df={}
	dict_of_digital_df={}
	timeseries= get_timeseries()
	#print "Length of rtt_array dataframe index", rtt_array.index
	#sys.exit()
	#print digital_array.columns
	'''rtt=[]
	digital=[]'''
	#print"Digital Array Data Frame",digital_array.index
	'''for index in  digital_array.index:
		print digital_array.loc[:]'''
	#sys.exit()
	avg_array = get_average(analog_array,len_data_array,routerid)

	router_avg_std_dic ={}
	for row in avg_array:
		router_avg_std_dic[(row[0].upper(),row[1])]=[float(row[2]),float(row[3])]
	
	for keys in sorted(router_avg_std_dic.keys()):
		print keys,'->',router_avg_std_dic[keys]
	#print router_avg_std_dic
	print "length of routerid",len(routerid), routerid
	#sys.exit()
	flag_data=0
	for router in routerid:
		rtt=[]
		digital=[]	
		print "Router",router.upper()
		digital_array=get_empty_matrix(timeseries,server_names)
		rtt_array = get_empty_matrix(timeseries,server_names)
		#sys.exit()
		for server in list_server:
			print "Server",server
			for latency in analog_array:
				#print "latency analog array length", len(analog_array)
				#print latency
				#sys.exit()
				if router.upper() == latency[0].upper() and server==latency[-1]:
				#print "if statement"
					flag_data=1
					avg = router_avg_std_dic[(router.upper(),server)][0]
					std= router_avg_std_dic[(router.upper(),server)][1]
				#print avg, std 					
				#sys.exit()
					if (latency[1] > (avg +std)):
					#	print "Value Appened",latency[1]
						rtt.append(latency[1])
						digital.append(1)
					elif latency[1] == -999:
						rtt.append(-999)
						digital.append(-999)
					else:
						rtt.append(0.0)
						digital.append(0)
				else:
					continue
			if flag_data == 1 :
				flag_data=0
				print "checkpoint for analysis",router, (rtt)	
				print "rtt rray length",len(rtt), avg, std
				print "Flag router", router
				i=0
				for idx in rtt_array.index:
					if i <len(rtt):
						#print "rtt[i] value for matrix",rtt[i]
						rtt_array.loc[idx,dict_of_server[server]] = rtt[i]
						digital_array.loc[idx,dict_of_server[server]]=digital[i]
						i+=1
					else:
						break
				rtt=[]
				digital=[]
		dict_of_df[router]=rtt_array	
		dict_of_digital_df[router]=digital_array
		#print rtt_array.columns,rtt_array.index,rtt_array
	for key in sorted(dict_of_digital_df.iterkeys()):
		print key,'->',dict_of_digital_df[key]

	#sys.exit()
	return dict_of_df,dict_of_digital_df
			
'''
def set_latency(analog_array,len_data_array,routerid):
	i=0;
	array=[]
	threshold  =  get_threshold(analog_array,len_data_array,routerid)
	
		
		
		


		
	
	#sys.exit()
	avg = average(latency_array)
	print "avg",avg     
	#print "add",add
	variance = map(lambda x: (x - avg)**2, add)
	#print variance
	std = math.sqrt(average(variance))
	print "Std", std
	del add[:]
	rtt_array = threshold(latency_array,avg,std)
	return rtt_array
'''
def sql_own():
	try:
		conn2 = pgsql.connect(dbname='swati',user='',passwd='')
	except:
		print "couldnot  connect to sql server"
		sys.exit()
	return conn2


def run_insert_query(rtt_array):
	conn2 = sql_own();
	print "Length in insert query", (len(rtt_array));
	try:
		for x in range(len(rtt_array)):
			cmd ="INSERT into single_server(deviceid,rtt,eventstamp,server) values ('%s','%s','%s', '%s')"%(rtt_array[x][0],rtt_array[x][1],rtt_array[x][2],rtt_array[x][3]);
			print "Insert command", cmd
			conn2.query(cmd)
		conn2.close()
		#	print x[0],x[1],x[2];
	except:
		print "Couldn't run %s\n"%(cmd)
		#print "Couldn't run Insert command"
	
def run_insert_lm_query(rtt_array):
	conn2 = sql_own();
	print "Length in insert query", (len(rtt_array));
	try:
		for x in range(len(rtt_array)):
			cmd ="INSERT into last_mile(deviceid,rtt,eventstamp,first_hop) values ('%s','%s','%s', '%s')"%(rtt_array[x][0],rtt_array[x][1],rtt_array[x][2],rtt_array[x][3]);
			print "Insert command", cmd
			conn2.query(cmd)
		conn2.close()
		#	print x[0],x[1],x[2];
	except:
		print "Couldn't run %s\n"%(cmd)

#if __name == '__main__':
#run_data_query(None,'2c:b0:5d:83:01:61','Ca','128.48.110.150');
