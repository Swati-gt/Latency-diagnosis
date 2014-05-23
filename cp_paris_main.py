#!/usr/bin/env pytho

import sys
import time
import paristraceroute as sql
#import pandas as pd
import csv
import os
import commands
import pandas as pd
conn = sql.sqlconn();

list_of_hops={}
nospikehops={}
spikehops={}

dict_of_asnames={}
dict_count_as={} # example dict_count[AMS]=20
dict_count_peer={}

dict_of_server={'ITALY':'143.225.229.254','LAX':'38.98.51.12','AMS':'213.244.128.169',"JNB":'196.24.45.146','NBO':'197.136.0.108','HND':'203.178.130.210','SYD':'175.45.79.44','INDIA':'180.149.52.20', 'BRAZIL':'143.54.2.20',\
 'GOOG':'8.8.8.8'}
fread = open('testrouter.txt')


def first_hop(list_of_hops,asnum):
	print "Inside first hop function\n"
	
	hop={}
	
	for key in sorted(asnum.iterkeys()):
		if hop.has_key(key[1]):
			hop[key[1]].append(asnum[key][0])
			print asnum[key]
		else:
			hop[key[1]]=[]
			hop[key[1]].append(asnum[key][0])
			
	for key in sorted(hop.iterkeys()):
		print key,"->",len(list(set(hop[key])))		
	print "End of first_hop function"
	#sys.exit()

def asnum_parse(output,ip): #output - is output of getstatusoutput
	print "\nIn Parse function"
	print "IP address in Parse function ", ip

	###print output
	###print output[-1]
	tmp = output.split(' ')
        tmp2 =" ".join(tmp)

        #print "program output:", output
        #print "join output: \n ",tmp2
    
	#print "Variable assessing"
        tmp3 =  tmp2.split()
        
	'''
	for idx in range(len(tmp3)):
                print tmp3[idx]
        print tmp3[10:]
	'''
	###print "tmp3",tmp3

	if cmp(tmp3, ['Timeout.'])==0:
		return ["No output","No output"]

	if cmp(tmp3[3], 'out')==0:
		return ["No output","No output"]
	#sys.exit()
        number =tmp3[6]
        name = tmp3[10]
        asname =''.join(name)
        ###print "AS Name and number\n ",asname, number
	#sys.exit()
	print "End of parse function"
	return asname, number


def get_asnum(data_array,server): # tup = routerid,ip,asnum dictionary {routerid :[ AS ] }
	print "In get_asnum function"
	asnum={}
	list_of_hops=[]
	#list_of_asnames=[]
	#asname_d={}
	print "checkpoint in get_asnum",data_array
	
	for data in data_array:
		for dt_data in data:
			if len(dt_data) == 0:
				print "No data available in get_asnum function"
				continue
			for value in dt_data: # value= ( MAC, own IP address, server, hop, hopip, rtt, date)
				#print "value dt_data"
				#print value
				#sys.exit()
				
				ip = value[4]
				#count = 1
				#if cmp(int(value[3]),1)==0:
				#	count+=1

				print "IP address to extract AS",value[0],ip,value[3]
		#sys.exit() 
				statement = "whois -h whois.cymru.com " + ip;
				print statement
				#as_details = os.system(statement)	
				status, details = commands.getstatusoutput(statement)
				#print status,"\n",details
				#sys.exit()
				#print "AS number",as_details , ip
				asname,number = asnum_parse(details,ip)
				#print "asname type:",type(asname),
				key =(value[0],value[2])
				print "key type",type(key),key
				print asnum, "checkpoint"
				if asnum.has_key(key):
					print number, ip
					if cmp(number,'NA')==0 or cmp(number,'No output')==0:
						print 'NA'
					else:
						#if cmp(number,'NA')!=0 or cmp(number,'No output')!=0:	
						asnum[key].append(number)
						dict_of_asnames[number]=asname
				else:
					asnum[key]= []
					#print "In esle"
					if cmp(number,'NA')==0 or cmp(number,'No output')==0:
						print 'NA'
					else:
						#if cmp(number,'NA')!=0 or cmp(number,'No output')!=0:
						asnum[key].append(number)
						dict_of_asnames[number]=asname

				if (cmp(ip,value[2]) == 0):
				#if (cmp(int(value[3]),1) == 0 and count>=2):
					print "break condition"
					break
	#		print "asnum",asnum
	#		sys.exit()
	for keys in sorted(asnum.keys()):
		print keys,'->',asnum[keys]
		print "length of key value' list ",len(asnum[keys])

	print "End of get_asnum function"
	#sys.exit()####recent checkpoint

	print "length of dictionary",len(asnum)
	list_of_hops.append(asnum)
	print "Length of list of router with ASes", len(list_of_hops)
	#return list_of_hops
	return asnum
	
def get_routerid(fread):
        routerid=[]
        for router in fread:
                router = router.strip()
		tmp = router.split(':')
		tmp2 = ''.join(tmp)
                routerid.append(tmp2)
        return routerid

def get_set(asnum,nospikehops):
	print "In get_set"
	print "ASNUM"
	for keys in sorted(asnum.keys()):
                print keys,'->',asnum[keys]
                #print "length of key value' list ",len(asnum[keys])

	print "No spike hops"
	for keys in sorted(nospikehops.keys()):
                print keys,'->',nospikehops[keys]

	link={}
        for key in sorted(asnum.iterkeys()):
                index=1
                link[key]=[]
                #print key,'->',asnum[key]
                for idx in range(len(asnum[key])):
                        if index < len(asnum[key]):
                                join = asnum[key][idx]+str('-')+asnum[key][index]
                                index+=1
                                link[key].append(join)

	nospikelink={}
        for key in sorted(nospikehops.iterkeys()):
                index=1
                nospikelink[key]=[]
                #print key,'->',asnum[key]
                for idx in range(len(nospikehops[key])):
                        if index < len(nospikehops[key]):
                                join = nospikehops[key][idx]+str('-')+nospikehops[key][index]
                                index+=1
                                nospikelink[key].append(join)



        print "AS path with links "
        for key in sorted(link.iterkeys()):
                print key,'->',link[key],"\n"

        print "AS path with links having no spikes"
        for key in sorted(nospikelink.iterkeys()):
                print key,'->',nospikelink[key],"\n"
	#sys.exit()
	#Find common set
	c_set =[]
	c_peer_set=[]
	#Finf common nospike set
	nospike_c_set=[]
	nospike_c_peer_set=[]
	'''
	for key in sorted(link.iterkeys()):
		c_set.append(asnum[key])
		c_peer_set.append(link[key])
	
		
	for key in sorted(nospikelink.iterkeys()):
		nospike_c_set.append(nospikehops[key])
		nospike_c_peer_set.append(nospikelink[key])
	'''
	
	for key in sorted(link.iterkeys()):
		for k in link[key]:
			c_peer_set.append(k)
	#print c_peer_set

	for key in sorted(asnum.iterkeys()):
		for k in asnum[key]:
			c_set.append(k)
	
	for key in sorted(nospikelink.iterkeys()):
		for k in nospikelink[key]:
			nospike_c_peer_set.append(k)
	#print c_peer_set

	for key in sorted(nospikehops.iterkeys()):
		for k in nospikehops[key]:
			nospike_c_set.append(k)

	print set(c_peer_set),"\n",set(nospike_c_peer_set)
	print "Hitting set",set(c_peer_set) - set(nospike_c_peer_set)
	#sys.exit()
	common_peer = set(c_peer_set) - set(nospike_c_peer_set)
	common_as = set(c_set) - set(nospike_c_set)
	
	print "Common AS",common_as
	print "Common Peer",common_peer

	for elem in common_as:
		print elem,'->',dict_of_asnames[elem]
	
	for elem in common_peer:
		(hop1,hop2) = elem.split('-')
		print elem,'->',dict_of_asnames[hop1],'-',dict_of_asnames[hop2]
	'''
	for key in sorted(nospikelink.iterkeys()):
		nospike_c_set.append(nospikehops[key])
		nospike_c_peer_set.append(nospikelink[key])

	#print c_set,c_peer_set
	common_as = list(set(c_set[0]))
	for elem in c_set:
		common_as = list(set(elem) & set(common_as))

	common_peer = list(set(c_peer_set[0]))	
	for elem in c_peer_set:
		common_peer = list(set(elem) & set(common_peer))

	print "Common AS",common_as
	print "Common Peer",common_peer

	for elem in common_as:
		print elem,'->',dict_of_asnames[elem]
	
	for elem in common_peer:
		(hop1,hop2) = elem.split('-')
		print elem,'->',dict_of_asnames[hop1],'-',dict_of_asnames[hop2]
	'''
	print "End of get_set function"
	#sys.exit()
	return common_as,common_peer



def merge_asnum(list_of_hops):
		print "In merge function"
		for key in sorted(list_of_hops.iterkeys()):
			prev = list_of_hops[key][0]
			tmp=[]
			tmp.append(prev)
			for i in range(1,len(list_of_hops[key])):
				if prev == list_of_hops[key][i]:
					continue
				else:
					prev = list_of_hops[key][i]
					tmp.append(prev)

			print "Original hops"
			print key,'->',list_of_hops[key]
			print "New hops"
			print key,'->',tmp
			list_of_hops[key]=tmp
		print "End of merge_asnum"
		return list_of_hops

def get_list(rtr):
	print "In get_list function"
	#sym =['ITALY']
	sym =['ITALY','LAX','AMS','JNB','NBO','INDIA','HND','SYD','BRAZIL','GOOG']
	for s in sym:
		server = dict_of_server[s]
		data_array=[]
		data_array.append(sql.new_run_data_query(conn,rtr,server))
		hops=get_asnum(data_array,server)
		list_of_hops.update(merge_asnum(hops))
	#print list_of_hops
	#sys.exit()
	return list_of_hops

def main():		
	print "In main function"
	routerid = get_routerid(fread)
	for rtr in routerid:
		list_of_hops = get_list(rtr)
		data_array=[]
		filename = './textfiles/' + str(rtr) + '_dict.txt'
		filename1 = './textfiles/noevent/' + str(rtr) + '_dict.txt'
		g = open(filename,'r')
		h = open(filename1,'r')
		event = eval(g.read())
		noevent = eval(h.read())
		#print event
		#sys.exit()
		check=0
		for date in sorted(event.iterkeys()):
			data_array=[]
			nospike_data_array=[]
			server = event[date] #date:[AMS,GOOG]
			print "Checkpoint"
			print date,server
			#sys.exit()
			if noevent.has_key(date):
				nospikeserver = noevent[date]
				print nospikeserver
			#sys.exit()
			if len(server) == 1:
				k = server[0]
				if dict_count_as.has_key(k):
					dict_count_as[k]+=1
				else:
					dict_count_as[k]=0
					dict_count_as[k]+=1

			else:
				print "Server",server,"no spike server",nospikeserver
				for srv in server:
					#spikehops={}
					k=dict_of_server[srv]
					key = (rtr,k)
					print "spike server",srv
					print "spike",key
					if list_of_hops.has_key(key):
						spikehops[key]=list_of_hops[key]
							
				for srv in nospikeserver:
					k=dict_of_server[srv]
					key = (rtr,k)
					#nospikehops={}
					print "no spike server",srv
					print "No spike",key
					if list_of_hops.has_key(key):
						nospikehops[key]=list_of_hops[key]
		
			
				'''	
        			data_array.append(sql.run_data_query(conn,rtr,server))
				list_of_hops = get_asnum(data_array,server)
				list_of_hops = merge_asnum(list_of_hops)

				#No spike server traceroute extraction
        			nospike_data_array.append(sql.run_data_query(conn,rtr,nospikeserver))
				nospikehops = get_asnum(nospike_data_array,nospikeserver)
				nospikehops = merge_asnum(nospikehops)
				'''
				print "Check dictionary in main function"
				print "list of hops",list_of_hops,"\n"
				print "Spike hops",spikehops,"\n"
				print "No Spike hops",nospikehops
				#sys.exit()
				common_as,common_peer = get_set(spikehops,nospikehops)
				
				#if check==2:
				#	print check
				#	sys.exit()
				#check+=1	
				for elem in common_as:
					if dict_count_as.has_key(elem):		
						dict_count_as[elem] +=1
					else:
						dict_count_as[elem]=0
						dict_count_as[elem]+=1
				for elem in common_peer:
					if dict_count_peer.has_key(elem):		
						dict_count_peer[elem] +=1
					else:
						dict_count_peer[elem]=0
						dict_count_peer[elem]+=1
				
		
		print "Check point in main"
		for key in sorted(dict_count_peer.iterkeys()):
			print key,'->',dict_count_peer[key]
		
		for key in sorted(dict_count_as.iterkeys()):
			print key,'->',dict_count_as[key]
		#sys.exit()

if __name__=="__main__":
	main()
