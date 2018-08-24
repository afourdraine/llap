from __future__ import division
import math

#Check variable format and ask for it till the right format is type
#Initial variables
while True:
	try:
		N_LLAP_NODES = int(raw_input('Number of nodes dedicated to LLAP:'))
		break
	except ValueError:
		print("Oops!  That was no valid number.  Try again...")

while True:
	try:
		N_TOTAL_NODES = int(raw_input('Total number of datanodes in the cluster:'))
		break
	except ValueError:
		print("Oops!  That was no valid number.  Try again...")

while True:
	try:
		PARRALLEL_Q = int(raw_input('Total number of parralel queries:'))
		break
	except ValueError:
		print("Oops!  That was no valid number.  Try again...")

while True:
	try:
		QUEUE_NAME = str(raw_input('Name of the LLAP queue:')) 
		break
	except ValueError:
		print("Oops!  That was no valid string.  Try again...")

while True:
	try:
		IO_ENABLE = str(raw_input('Is there any BI query tool connected to LLAP ? (y/N):'))
		break
	except ValueError:
		print("Oops!  That was no valid number.  Try again...")

while True:
	try:
		SLIDER_AM = int(raw_input('Slider AM memory allocation mb[1024]:') or 1024)
		break
	except ValueError:
		print("Oops!  That was no valid number.  Try again...")

while True:
	try:
		YARN_RM_MB = int(raw_input('yarn.nodemanager.resource.memory-mb value:'))
		break
	except ValueError:
		print("Oops!  That was no valid number.  Try again...")

while True:
	try:
		TEZ_AM_MEMORY = int(raw_input('tez.am.resource.memory.mb value:'))
		break
	except ValueError:
		print("Oops!  That was no valid number.  Try again...")

while True:
	try:
		QUEUE_PRIORITY = int(raw_input('Queue priority:'))
		break
	except ValueError:
		print("Oops!  That was no valid number.  Try again...")

while True:
	try:
		N_EXECUTOR = int(raw_input('Number of LLAP executors (should be equal to the number of CPU cores per nodes):'))
		break
	except ValueError:
		print("Oops!  That was no valid number.  Try again...")

while True:
	try:
		EXECUTOR_MEMORY = int(raw_input('Executor memory allocation (default 4096mb):') or 4096)
		break
	except ValueError:
		print("Oops!  That was no valid number.  Try again...")


if IO_ENABLE == "y":
	IO_ENABLE = True
else:
	IO_ENABLE = False

#Calculation:
N_TEZ_AMs = PARRALLEL_Q + 1

XMX = N_EXECUTOR * EXECUTOR_MEMORY

HEADROOM = 0.06 * XMX
if HEADROOM > 6 * 6144:
	HEADROOM = 6144

IO_LAYER = 0.2 * N_EXECUTOR

CACHE_SIZE = math.ceil(YARN_RM_MB - XMX - HEADROOM) 
if IO_ENABLE is True and CACHE_SIZE < IO_LAYER:
	print "WARNING : There is not enought available memory to activate hive.llap.io.enable for BI queries"

TOTAL_MEMORY = math.ceil(CACHE_SIZE + XMX + HEADROOM)

QUEUE_SIZE = TEZ_AM_MEMORY * N_TEZ_AMs + SLIDER_AM + TOTAL_MEMORY * N_LLAP_NODES

QUEUE_CAPACITY = math.ceil(( QUEUE_SIZE / (YARN_RM_MB * N_TOTAL_NODES)) * 100)

#Yarn Config:
print ""
print ""
print "Yarn configuration related to LLAP"
print ""
print "yarn.resourcemanager.scheduler.monitor.enable = true"
print "yarn.scheduler.minimum-allocation-mb =",1024
print "yarn.scheduler.maximum-allocation-mb =",YARN_RM_MB
print "yarn.scheduler.capacity.root."+QUEUE_NAME+".user-limit-factor = 1"
print "yarn.scheduler.capacity.root."+QUEUE_NAME+".minimum-user-limit-percent = 100"
print "yarn.scheduler.capacity.root."+QUEUE_NAME+".priority =",QUEUE_PRIORITY
print "yarn.scheduler.capacity.root."+QUEUE_NAME+".capacity =",QUEUE_CAPACITY
print "yarn.scheduler.capacity.root."+QUEUE_NAME+".maximum-capacity =",QUEUE_CAPACITY


#Hive config:
print ""
print ""
print "HIVE configurartion related to LLAP"
print ""
print "hive.llap.io.enabled =",IO_ENABLE
print "num_llap_nodes =",N_LLAP_NODES
print "hive.server2.tez.sessions.per.default.queue =",PARRALLEL_Q
print "hive.llap.daemon.yarn.container.mb =",TOTAL_MEMORY
print "hive.llap.io.memory.size =",CACHE_SIZE
print "hive.llap.daemon.num.executors =",N_EXECUTOR
print "hive.llap.io.threadpool.size =",N_EXECUTOR