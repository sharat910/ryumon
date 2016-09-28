import requests
import ast
import json
import os
import csv
import time
import eventlet
from ruyplot import send_to_graphite

def write_header(fp,keys):
	with open(fp,"wb") as f:
		writer = csv.writer(f)
		writer.writerow(["timestamp"] + keys)

def write_flow(sid,so):
	x = "flow"	
	t = time.time()
	dic = json.loads(so.text)
	keys = dic[dic.keys()[0]][0].keys()
	fp = str(sid)+"/%s.csv"%x
	if not os.path.exists(fp): write_header(fp,keys)  
	flows = dic[dic.keys()[0]]
	with open(fp,"a") as f:
		writer = csv.writer(f)		
		for flow in flows:
			values = [flow[key] for key in flow.keys()]			
			writer.writerow([t] + values)	
	return sid

def write_aggr_flow(sid,so):
	x = "aggregateflow"	
	t = time.time()
	dic = json.loads(so.text)
	keys = dic[dic.keys()[0]][0].keys()
	fp = str(sid)+"/%s.csv"%x
	if not os.path.exists(fp): write_header(fp,keys)  
	aggr_flow = dic[dic.keys()[0]][0]
	send_to_graphite(sid,"aggregateflow",aggr_flow)
	values = [aggr_flow[key] for key in aggr_flow.keys()]
	with open(fp,"a") as f:
		writer = csv.writer(f)
		writer.writerow([t] + values)	
	return sid

def write_port_data(sid,so):	
	dic = json.loads(so.text)
	ports = dic[dic.keys()[0]]
	t = time.time()
	for port in ports:
		send_to_graphite(sid,"ports.%d"%port["port_no"],port)
		keys = port.keys()
		fp = str(sid)+"/%s.csv"%("port:"+str(port["port_no"]))
		if not os.path.exists(fp): write_header(fp,keys)
		values = [port[key] for key in port.keys()] 
		with open(fp,"a") as f:
			writer = csv.writer(f)						
			writer.writerow([t] + values)
	return sid

def fetch(url):
	_,var = url.split("stats/")
	typ,sid = var.split("/")
	sid = int(sid)
	so = sess.get(url)
	if typ == "flow":
		write_flow(sid,so)
	elif typ == "aggregateflow":
		write_aggr_flow(sid,so)
	elif typ == "port":
		write_port_data(sid,so)
	return sid

def url_gen(switch_id_list):	
	urls = []
	for sid in switch_id_list:
		urls.append(r + "flow/"+str(sid))
		urls.append(r + "aggregateflow/"+str(sid))
		urls.append(r + "port/"+str(sid))
	return urls

def initi(switch_id_list):
	for sid in switch_id_list:
		if not os.path.exists(str(sid)):
			os.mkdir(str(sid))

		so = sess.get(r + "desc/"+str(sid))
		with open(str(sid)+"/switchdesc.txt","w") as f:
			f.write((json.dumps(json.loads(so.text),indent=3)).encode('utf-8'))
			
		so = sess.get(r + "portdesc/"+str(sid))
		with open(str(sid)+"/portdesc.txt","w") as f:
			f.write((json.dumps(json.loads(so.text),indent=3)).encode('utf-8'))

def get_switches(sess):	
	a = sess.get(r + "switches")
	switch_id_list = ast.literal_eval(a.text)
	return switch_id_list


if __name__ == '__main__':
	sess = requests.Session()
	r = "http://192.168.1.36:8080/stats/"	
	switch_id_list = get_switches(sess)
	initi(switch_id_list)
	urls = url_gen(switch_id_list)
	i=0
	pool = eventlet.GreenPool()
	st = time.time()
	while(True):

		####PARALLELLLLLLL############

		# for sid in enumerate(pool.imap(write_flow, switch_id_list)):
		# 	pass
		# for sid in enumerate(pool.imap(write_aggr_flow, switch_id_list)):
		# 	pass
		# for sid in enumerate(pool.imap(write_port_data, switch_id_list)):
		# 	pass

		####SERIAL#############

		# for sid in switch_id_list:
		# 	write_flow(sid)
		# 	write_aggr_flow(sid)
		# 	write_port_data(sid)

		#######PARALLEL II #######
		for sid in pool.imap(fetch,urls):
			pass
		i+=1
		#print i
		time.sleep(0.5)
	print time.time() - st