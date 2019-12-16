# -*- coding: utf-8 -*-

import sys
import numpy as np
sys.dont_write_bytecode = True
from order_priority import * 
from datetime import datetime
import itertools
import random
import time

#used for subtract a datetime
BENCHMARK_DATETIME = datetime.strptime('2010-01-01 12:00:00', "%Y-%m-%d %H:%M:%S")

def allStationFinishedOnType(mono_label, multi_label, otype):
	for s in mono_label:
		if mono_label[s][otype]<=1: return 0
	for s in multi_label:
		if multi_label[s][otype] <=1: return 0
	return 1

#determine whether outmost while should be terminated
def shouldTerminate(station_mono_labels,station_multi_labels,orders,ordersByType,count):
	if count > outmost_cycle_count_max: return 1
	if len(orders) == 0: return 1
	sT = 1
	for o in ordersByType:
		if len(ordersByType[o]) > 0 and not allStationFinishedOnType(station_mono_labels,station_multi_labels,o): return 0
	return 1

#return 1 if all station for all order type has label onhold or finished
def allStationAllTypeOnholdFinished(label):
	for s in label:
		for o in label[s]:
			if label[s][o]<1: return 0
	return 1

#only consider conditions of the station's mono slots, not ei3 label
def stationMonoFeasible(virSlots,monoSlots,staid = -1,otype = -1):
	temp = sum([virSlots[staid][slot]['zudan_status']<=1  for slot in monoSlots[staid][otype]])
	if temp > 0:
		return 1
	else:
		return 0

#only consider conditions of the station's multi slots, not ei3 label
#NOTE: take B a) into consideration
def stationMultiFeasible(virSlots,multiSlots,staid = -1, otype = -1):
	#a slot can be feasible if its zudan_status is not finished (necessary condition)
	allMultislotsFinished = 1
	allMultislotsNoSupport = 1
	can_do_multi_slots = [s for s in multiSlots[staid] if virSlots[staid][s]['zudan_status'] <= 1]
	#if len(can_do_multi_slots) > 0: allMultislotsFinished = 0
	#if len([s for s in can_do_multi_slots if otype in virSlots[staid][s]['des_support']])>0:
	#	allMultislotsNoSupport = 0
	#
	for slot in can_do_multi_slots:
		#if this slot has a supported order type (1) priority in front of otype (2) station haven't multi-label it as finished
		allMultislotsFinished = 0
		#if this slot support tyoe
		if otype in virSlots[staid][slot]['des_support']:
			if virSlots[staid][slot]['des_support'][virSlots[staid][slot]['des_indicator']] == otype:
				allMultislotsNoSupport = 0
			else:
				if len(virSlots[staid][slot]['has']) == 0:
					allMultislotsNoSupport = 0
		#Feasible = 1
		#print 'check des indicator: %s,%s,%s' %(staid,slot,otype)
		#print virSlots[staid][slot]['des_support'],virSlots[staid][slot]['des_indicator']

		if virSlots[staid][slot]['des_support'][virSlots[staid][slot]['des_indicator']] == otype:
			return 1, allMultislotsFinished + allMultislotsNoSupport
		#for supported_type in virSlots[staid][slot]['des_support']:
		#	if supported_type == otype: break #only consider those in front of 
		#	if label[staid][supported_type] <= 1: #this station have not finished a type of higher priority
		#		Feasible = 0
		#		break
		#if Feasible: return 1,allMultislotsFinished
	return 0,allMultislotsFinished + allMultislotsNoSupport


# a multi slot is finished when all its supported order types have finished label in the station 
# actually this function also applies to mono slot, but that would be redudent as station mono label alone
# can indicate its mono slots are finished.
def slotMultiFinished(virSlots,label,staid=-1,slotid=-1):
	for suported_type in virSlots[staid][slotid]['des_support']:
		if label[staid][supported_type] <=1:
			return 0
	return 1

	
def cal_r(o,s,stations=0,bills=0):
	temp = set(bills[o]['skus'].keys())
	return len(temp&stations[s].omega)/float(len(temp))


def calculateOverlap(combinations,exceptions=0,stations=0,bills=0):
	#remove exceptions from combinations
	cleaned_combinations = [(o,s) for o,s in combinations if (o,s) not in exceptions]
	R = {}
	StationsInR = set()
	StationsInRPositive = set()

	bill_spreaded_limit = {}
	# 1st round
	for o,s in itertools.product(bills, stations):
		if o not in bill_spreaded_limit.keys():
			bill_spreaded_limit[o] = 2
		if len(set(bills[o]['skus'].keys()) & stations[s].omega) > 0:
			bill_spreaded_limit[o] -= 1

	# 2nd round
	for o,s in cleaned_combinations:
		if bill_spreaded_limit[o] <= 0:	continue
		
		bill_spreaded_limit[o]-= 1
		temp = set(bills[o]['skus'].keys())
		R[(o,s)] = len(temp & stations[s].omega) / float(len(temp))
		StationsInR.add(s)
		if R[(o,s)] > 0:
			StationsInRPositive.add(s)
	return R,StationsInR,StationsInRPositive


def chooseSeed(S,ordersID,all_bills,threshold):
	filtered_orders_id = []
	sku_in_order_count = {}
	for o in ordersID:
		temp = set(all_bills[o]['skus'].keys())
		if len(temp & S) / float(len(temp)) <= threshold:
			filtered_orders_id.append(o)
	#if no orders below threshold, select all ordersID
	if len(filtered_orders_id) == 0:
		filtered_orders_id = ordersID
	for o in filtered_orders_id:
		temp = set(all_bills[o]['skus'].keys())
		for s in temp:
			if s not in sku_in_order_count: sku_in_order_count[s] = 0
			sku_in_order_count[s] += 1
	#choose seed SKU
	seed_sku = sorted(sku_in_order_count.keys(),key = lambda x: sku_in_order_count[x],reverse = 1)[0]
	#select orders with seed SKU
	seed_o = sorted([o for o in filtered_orders_id if seed_sku in all_bills[o]['skus']],key = lambda x: len(all_bills[x]['skus']),reverse = 1)[0]
	return seed_o


def zudanXPY(all_bills, pool=[], stations={}, racks={}, currentTime='', screen=0):

	#clear and initialize omega for all stations
	for s in stations:
		stations[s].omega = set()
		for sku in stations[s].skuRequested:
			stations[s].omega.add(sku)
		for r,f in stations[s].rackFacesIncoming:
			for sku in racks[r].sides[f]['inventory']:
				stations[s].omega.add(sku)

	#pre-screening slots that can be used for zudan (for each station)
	'''
	virSlots are all the available slots by station, it copies all the slot info and only used in zudan
	monoSlots store all the slots that support only one type of order(monoslot) by station and order type
	multiSlots store all the slots that support more than one type of order (multislot) by station 
	'''
	virSlots = {}
	monoSlots = {} # by station by order type
	multiSlots = {} # by station
	for sta in stations:
		screened_slots = []
		available_type1_slots = [s for s in stations[sta].type1_slots if stations[sta].slots[s]['status'] == 0]
		available_type2_slots = [s for s in stations[sta].type2_slots if stations[sta].slots[s]['status'] == 0]
		if len(available_type1_slots) >= THETA * len(stations[sta].type1_slots):
			screened_slots += available_type1_slots
		screened_slots += available_type2_slots
		#create virSlots for zudan
		virSlots[sta] = {}
		for s in screened_slots:
			virSlots[sta][s] = {'support':stations[sta].slots[s]['support'], \
				'des_support': stations[sta].slots[s]['des_support'] ,\
				'des_indicator':0,\
				'vol': stations[sta].slots[s]['vol'],\
				'remain_vol':stations[sta].slots[s]['vol'],\
				'zudan_status': 0, \
				'order_type_assigned':-1,\
				'has':[]} # zudan status: 0 for not zued,1 for zuing, 2 for finished
				#des_indicator initially point to the top priority type in des_support
		#create monoSlots and multiSlots for zudan
		monoSlots[sta] = {}
		for o in ORDER_PRI:
			monoSlots[sta][o] = []
		multiSlots[sta] = []
		for s in virSlots[sta]:
			if len(virSlots[sta][s]['des_support']) > 1:
				multiSlots[sta].append(s)
			else:
				monoSlots[sta][virSlots[sta][s]['des_support'][0]].append(s)
	#print monoSlots
	#print multiSlots
	#pre-screening orders in pool:
	#screenedOrders = []
	screenedOrders = set(pool)
	#print screenedOrders
	#order_considered_order
	ORDER_ORDER = sorted(ORDER_PRI.keys(),key = lambda x: ORDER_PRI[x],reverse = 1)
	screenedOrdersByType = {}
	for o in ORDER_ORDER:
		screenedOrdersByType[o]= set()
	for o in screenedOrders:
		screenedOrdersByType[all_bills[o]['process_type']].add(o)

	#store order volume
	ORDER_VOLUMES = {}
	for o in screenedOrders:
		ORDER_VOLUMES[o] = sum([racks[list(racks.keys())[0]].SKUS[sku]['volume'] * all_bills[o]['skus'][sku] for sku in all_bills[o]['skus']])
		#should upgrade to ORDER_VOLUMES[o] = all_bills[o]['volume'] 
	#clear and initialize all labels
	station_mono_labels = {} # to label MONO-support slots of a particular station INIT/ONHOLD/FINISHED choosing orders (BY TYPE)
	station_multi_labels = {} # to label MULTI-support slots of a particular station INIT/ONHOLD/FINISHED choosing orders (BY TYPE)
	#0 for init(can zu), 1 for onhold, 2 for finished
	for s in stations:
		station_mono_labels[s] = {}
		station_multi_labels[s] = {}
		for o in ORDER_ORDER:
			station_mono_labels[s][o] = 0
			station_multi_labels[s][o] = 0
	#ei3 label: order o can not be assigned to corresponding mono slot in station s
	ei3_cannot_assigned = set() #use set for performance, no need to assign value
	ei3_cannot_assigned_multi = set() 
	#
	OUTMOST_WHILE_COUNTER = 0
	#print virSlots
	################# OUTMOST WHILE ########################
	#
	#quit all station for all type (both mono and multi) finished or no more orders or reach counter limit
	#
	while not shouldTerminate(station_mono_labels,station_multi_labels,screenedOrders,screenedOrdersByType,OUTMOST_WHILE_COUNTER):
		OUTMOST_WHILE_COUNTER +=1
		#print 'OUTMOST_WHILE_COUNTER %s' %OUTMOST_WHILE_COUNTER
		################# A WHILE FOR MONO #####################
		#
		#quit: (all station for all order types, mono finished or mono onhold) or no more orders
		#
		#
		while (not allStationAllTypeOnholdFinished(station_mono_labels)) and len(screenedOrders)>0:
			#print 'cycle A'
			for o in ORDER_ORDER: #consider all types of orders based on the order type priority (descending) 
				#when such order do not exist: for all station, update station_mono_label of this type to FINISHED,
				#update all mono slots of this type to finished
				###a) calculate R
				if len(screenedOrdersByType[o]) ==0 :
					for sta in stations:
						station_mono_labels[sta][o] = 2 # finished
						for slot in monoSlots[sta][o]:
							virSlots[sta][slot]['zudan_status'] = 2 # finished
					continue
				#such type of order exists, proceed
				#need to choose supported station (CONDITIONS:1. )
				supported_stations = [] # chosen by condition of mono slots, need to 
				for sta in stations:
					if stationMonoFeasible(virSlots,monoSlots,staid = sta,otype = o):
						supported_stations.append(sta)
					else:
						station_mono_labels[sta][o] = 2 #finished
						#stationMonoFeasible already ensured all corresponding slots of sta has zudan_status 2
				#print supported_stations
				#return 0
				#calculate R
				PAIRS = itertools.product(screenedOrdersByType[o],supported_stations)
				R,StationsInR,StationsInRPositive = calculateOverlap(PAIRS,exceptions=ei3_cannot_assigned,stations=stations,bills=all_bills)
				##for stations in supported_stations - StationsInR, label finished
				for s in supported_stations:
					if s not in StationsInR:
						station_mono_labels[s][o] = 2
						for slot in monoSlots[s][o]:
							virSlots[s][slot]['zudan_status'] = 2 # finished
				###b) if R = 0, label supported stations onhold and proceed
				if sum(R.values()) <=0:
					for s in StationsInR:
						station_mono_labels[s][o] = 1
					continue # continue to next order priority
				###c) with some r in R > 0, remove corresponding onhold label for those stationsPositive
				#selected the best r
				for s in StationsInRPositive:
					if station_mono_labels[s][o] <= 1: #this seems to be redudent as support_stations cannot has station with satus 2, just double check
						station_mono_labels[s][o] = 0 #remove onhold label
				#choose the best o,s in the following orders:
				#1.largest R 2,when even, largest SKU count in o 3, when even, earliest deadline 4, when even, random
				MaxR = max(R.values())
				bestOS = sorted([k for k in R if R[k]>=MaxR],\
					key=lambda x: (len(all_bills[x[0]]['skus'].keys()),(BENCHMARK_DATETIME-all_bills[x[0]]['deadline_time']).total_seconds()),reverse=1)[0]
				###d) if best R < 1, add to omega 
				sta_id = bestOS[1]
				order_id = bestOS[0]
				if R[bestOS] < 1:
					#add best order sku to station omega
					temp_rack_key = racks.keys()[0]
					for sku in all_bills[order_id]['skus']:
						#add those in the best order
						stations[sta_id].omega.add(sku)
						#add those in the corresponding rack faces
						for face in racks[temp_rack_key].SKUS[sku]['on_rack_faces']:
							for s in racks[face[0]].sides[face[1]]['inventory']:
								stations[sta_id].omega.add(s)
				###e) add to slot
				#NOTE: for extreme large order (too large to fit in any slot):
				#this type of order if processed by order, ignore the volume, suit into any empty slot
				#if processed by batch, can only goes to empty slot, if no empty, label ei3
				#
				#indicator on whether this selected order is processed in the selected station in batch
				BATCHING_FOR_O_IN_S = int(stations[sta_id].order_production_type[o]) == 2
				#the total volume of this order
				orderVolume = ORDER_VOLUMES[order_id]
				#slot order count limit:
				orderCountLimit = 10 #this is only used when the order is processed in batch
				if o == SINGLE:
					orderCountLimit = stations[sta_id].single_batch_size
				if o == NO_MERGE:
					orderCountLimit = stations[sta_id].no_merge_batch_size
				#
				if BATCHING_FOR_O_IN_S: #process this type of order o by batching
					#
					CAN_PUT = 1
					zuing_slots_choice_set = [slot for slot in monoSlots[sta_id][o] \
						if (virSlots[sta_id][slot]['zudan_status'] == 1 and virSlots[sta_id][slot]['remain_vol'] >= orderVolume)]
					empty_slots_choice_set = [slot for slot in monoSlots[sta_id][o] if virSlots[sta_id][slot]['zudan_status'] == 0]
					if len(zuing_slots_choice_set) > 0:
						chosen_slot_id = random.choice(zuing_slots_choice_set)
					else:
						if len(empty_slots_choice_set) > 0:
							chosen_slot_id = random.choice(empty_slots_choice_set)
						else:
							CAN_PUT = 0
					#if CAN_PUT, update related info, if not, update ei3
					if CAN_PUT:
						#put the order in, remove from screened orders, change the slot  status to Finished (with condition)
						virSlots[sta_id][chosen_slot_id]['has'].append(order_id)
						virSlots[sta_id][chosen_slot_id]['order_type_assigned'] = o
						virSlots[sta_id][chosen_slot_id]['zudan_status'] = 1
						virSlots[sta_id][chosen_slot_id]['remain_vol'] -= orderVolume
						#if volume is below zero or reach batch limit, then label the chosen slot as finished
						if virSlots[sta_id][chosen_slot_id]['remain_vol'] <= 0 or len(virSlots[sta_id][chosen_slot_id]['has'])>=orderCountLimit:
							virSlots[sta_id][chosen_slot_id]['zudan_status'] = 2 
						screenedOrders.remove(order_id)
						screenedOrdersByType[o].remove(order_id)
					else:
						ei3_cannot_assigned.add((order_id,sta_id))

				else: #process this type of order o by oder
					#selected an empty slot (zudan_status = 0)
					#NOTE: the choice set cannot be zero as it is guaranteed by supported_stations 
					#and it is not possible to have slots with zudan_status 1 (zuing) when the order type is processed by order!
					slot_choice_set = [slot for slot in monoSlots[sta_id][o] if virSlots[sta_id][slot]['zudan_status'] == 0]
					chosen_slot_id = random.choice(slot_choice_set)
					#put the order in, remove from screened orders, change the slot  status to Finished
					virSlots[sta_id][chosen_slot_id]['has'].append(order_id)
					virSlots[sta_id][chosen_slot_id]['order_type_assigned'] = o
					virSlots[sta_id][chosen_slot_id]['zudan_status'] = 2
					virSlots[sta_id][chosen_slot_id]['remain_vol'] = 0 #just set to 0 as it has finished 
					screenedOrders.remove(order_id)
					screenedOrdersByType[o].remove(order_id)
				###f)for this selected station, after puting the order, update its status
				if not stationMonoFeasible(virSlots,monoSlots,staid = sta_id,otype = o):
						station_mono_labels[sta_id][o] = 2 #finished
			#print station_mono_labels
			#return 0
		################# B WHILE FOR MULTI #####################
		#
		#quit: (all station for all order types, multi finished or multi onhold) or no more orders
		#
		#
		while (not allStationAllTypeOnholdFinished(station_multi_labels)) and len(screenedOrders)>0:
			#print 'cycle B'
			#print len(screenedOrders),station_multi_labels
			for o in ORDER_ORDER: #consider all types of orders based on the order type priority (descending)
				#when such order do not exist: for all station, update station_multi_label of this type to FINISHED
				#NOTE: be careful when updatint multi slots status
				###a) calculate R
				if len(screenedOrdersByType[o]) == 0:
					for sta in stations:
						station_multi_labels[sta][o] = 2 #finished 
						#need to check if multi slot of sta can be set to finished (can be skip in mono)
						for slot in multiSlots[sta]:
							current = virSlots[sta][slot]['des_indicator']
							if virSlots[sta][slot]['des_support'][current] == o:
								# if this slot has been doing order type o:
								if len(virSlots[sta][slot]['has']) > 0:
									virSlots[sta][slot]['zudan_status'] = 2 # finish this slot
								else: # if this slot has current o, but empty slot yet
									if current < len(virSlots[sta][slot]['des_support']) - 1: #current is not last 
										virSlots[sta][slot]['des_indicator'] += 1 # this slot can proceed to processing next priority
									else: #current is last
										virSlots[sta][slot]['zudan_status'] = 2
							#if slotMultiFinished(virSlots,station_multi_labels,staid=sta,slotid=slot):
							#	virSlots[sta][slot]['zudan_status'] = 2 #finished
					continue
				#such type of order exists, proceed
				#need to choose supported station
				supported_stations = [] # chosen by condition of multi slots
				for sta in stations:
					if int(stations[sta].order_production_type[o]) == 0: #this station dont support 
						station_multi_labels[sta][o] = 2 #finished
					else:
						feas,fin = stationMultiFeasible(virSlots,multiSlots,staid = sta, otype = o)
						if feas:
							supported_stations.append(sta)
						else:#if not supported, there are three possibilities:
							#(0) no multislot ever support this type (need to finish the station for this type)
							#(1) all multislot are finished (need to finish the station for this type)
							#(2) there are slot not finished, but can process order of higher priority (do nothing)
							if fin>0:
								station_multi_labels[sta][o] = 2 #finished
								#if fin, it is already clear that all corresponding slots of sta has zudan_status 2
							#else: do nothing
				#print o,supported_stations
				#calculate R
				PAIRS = itertools.product(screenedOrdersByType[o],supported_stations)
				R,StationsInR,StationsInRPositive = calculateOverlap(PAIRS,exceptions=ei3_cannot_assigned_multi,stations=stations,bills=all_bills)
				#print o,R
				#print o,sum(R.values())
				#for stations in supported_stations - StationInR, label finished
				for s in [i for i in supported_stations if i not in StationsInR]:
					station_multi_labels[s][o] = 2
					for slot in multiSlots[s]:
						current = virSlots[s][slot]['des_indicator']
						if virSlots[s][slot]['des_support'][current] == o:
							if len(virSlots[s][slot]['has']) > 0:
								virSlots[s][slot]['zudan_status'] = 2 # finish this slot
							else:# if this slot has current o, but empty slot yet
								if current < len(virSlots[s][slot]['des_support']) - 1: #current is not last
									virSlots[s][slot]['des_indicator'] += 1 #this slot can proceed to processing next priority
								else: #current is last
									virSlots[s][slot]['zudan_status'] = 2 #finished
				###b) if R = 0, label supported stations onhold and proceed:
				if sum(R.values()) <= 0:
					#print 'check 0 for %s' %o
					#print StationsInR
					for s in StationsInR:
						#not only type o, all types of lower priority than o should be onhold
						for lower_o in ORDER_ORDER[ORDER_ORDER.index(o):]:
							station_multi_labels[s][lower_o] = 1
					continue # continue to next order priority
				###c) with some r in R > 0, remove corresponding onhold lavel for those stationsPositive
				#and select the best r
				for s in StationsInRPositive:
					if station_multi_labels[s][o] <= 1: #again, this seems to be redudent as support_stations cannot has station with status 2, just double check
						station_multi_labels[s][o] = 0 # remove onhold label
				#choose the best o,s in the following orders:
				#1.largest R; 2. when even, largest SKU count in o; 3. when even, earliest deadline; 4, when even, random
				MaxR = max(R.values())
				bestOS = sorted([k for k in R if R[k]>=MaxR],\
					key=lambda x: (len(all_bills[x[0]]['skus'].keys()),(BENCHMARK_DATETIME-all_bills[x[0]]['deadline_time']).total_seconds()),reverse=1)[0]
				###d) if best R < 1, add to omega
				sta_id = bestOS[1]
				order_id = bestOS[0]
				if R[bestOS] < 1:
					#add best order sku to station omega
					temp_rack_key = list(racks.keys())[0]
					for sku in all_bills[order_id]['skus']:
						#add those in the best order
						stations[sta_id].omega.add(sku)
						#add those in the corresponding race faces
						for face in racks[temp_rack_key].SKUS[sku]['on_rack_faces']:
							for s in racks[face[0]].sides[face[1]]['inventory']:
								stations[sta_id].omega.add(s)
				###e) add to slot
				#NOTE: for extreme large order (too large to fit in any slot):
				#this type of order if processed by order, ignore the volume, suit into any empty slot
				#if processed by batch, can only goes to empty slot, if not empty, label ei3_multi
				#
				#indicator on whether this selected order is processed in the selected station in batch
				BATCHING_FOR_O_IN_S = int(stations[sta_id].order_production_type[o]) == 2
				#the total volume of this order
				orderVolume = ORDER_VOLUMES[order_id]
				#slot order count limit:
				orderCountLimit = 10 #this is only used when the order is processed in batch
				if o == SINGLE:
					orderCountLimit = stations[sta_id].single_batch_size
				if o == NO_MERGE:
					orderCountLimit = stations[sta_id].no_merge_batch_size
				#
				if BATCHING_FOR_O_IN_S: #proceess this type of order o by batching
					#
					CAN_PUT = 1
					#
					zuing_slots_choice_set = [slot for slot in multiSlots[sta_id] \
					if (virSlots[sta_id][slot]['zudan_status'] == 1 and \
					virSlots[sta_id][slot]['des_support'][virSlots[sta_id][slot]['des_indicator']] == o and \
					virSlots[sta_id][slot]['remain_vol'] >= orderVolume)]
					#
					empty_slots_choice_set =  [slot for slot in multiSlots[sta_id] \
					if (virSlots[sta_id][slot]['zudan_status'] == 0 and \
					virSlots[sta_id][slot]['des_support'][virSlots[sta_id][slot]['des_indicator']] == o)]
					if len(zuing_slots_choice_set)>0:
						chosen_slot_id = random.choice(zuing_slots_choice_set)
					else:
						if len(empty_slots_choice_set) > 0:
							chosen_slot_id = random.choice(empty_slots_choice_set)
						else:
							CAN_PUT = 0
					#if CAN_PUT, update related info, if not, update ei3
					if CAN_PUT:
						#put the order in, remove from screened orders, change te slot status to Finished (with conditon)
						virSlots[sta_id][chosen_slot_id]['has'].append(order_id)
						virSlots[sta_id][chosen_slot_id]['order_type_assigned'] = o
						virSlots[sta_id][chosen_slot_id]['zudan_status'] = 1
						virSlots[sta_id][chosen_slot_id]['remain_vol'] -= orderVolume
						#if volume is below zero or reach batch limit, then label the chosen slot as finished
						if virSlots[sta_id][chosen_slot_id]['remain_vol'] <= 0 or len(virSlots[sta_id][chosen_slot_id]['has'])>=orderCountLimit:
							virSlots[sta_id][chosen_slot_id]['zudan_status'] = 2
						screenedOrders.remove(order_id)
						screenedOrdersByType[o].remove(order_id)
					else:
						ei3_cannot_assigned_multi.add((order_id,sta_id))
				else: #process this type of order o by oder
				#selected an empty slot (zudan_status = 0)
				#NOTE: the choice set cannot be zero as it is guaranteed by supported_stations 
				#and it is not possible to have slots with zudan_status 1 (zuing) when the order type is processed by order!
					slot_choice_set = [slot for slot in multiSlots[sta_id] if (virSlots[sta_id][slot]['zudan_status'] == 0 and \
					virSlots[sta_id][slot]['des_support'][virSlots[sta_id][slot]['des_indicator']] == o)]
					chosen_slot_id = random.choice(slot_choice_set)
					#put the order in, remove from screened orders, change the slot status to Finished
					virSlots[sta_id][chosen_slot_id]['has'].append(order_id)
					virSlots[sta_id][chosen_slot_id]['order_type_assigned'] = o
					virSlots[sta_id][chosen_slot_id]['zudan_status'] = 2
					virSlots[sta_id][chosen_slot_id]['remain_vol'] = 0 #just set to 0 as it has finished 
					screenedOrders.remove(order_id)
					screenedOrdersByType[o].remove(order_id)
				###f) for this selected station, after puting the order, update its status
				selected_feas,selected_fin = stationMultiFeasible(virSlots,multiSlots,staid = sta_id, otype = o)
				if selected_fin>0: 
					station_multi_labels[sta_id][o] = 2 #finished
					#print 'selected station %s finished %s' %(sta_id,o)
		################# 2) FOR ADD TO OMEGA #####################
		#	for each station with onhold labels, ONE seed order is placed 
		#	for a label with highest priority. 
		###
		S = set() # all stations share one common set
		stations_with_onhold = {}
		#find all onhold labels
		for s in stations:
			onhold_list = []
			for o in ORDER_ORDER:
				if station_mono_labels[s][o] == 1:  onhold_list.append((1,o)) #1 for mono
			for o in ORDER_ORDER:
				if station_multi_labels[s][o] == 1: onhold_list.append((2,o)) #2 for multi
			if len(onhold_list) > 0: stations_with_onhold[s] = onhold_list
		#print 'For cycle'
		#print stations_with_onhold
		#onhold_list has already been sorted, now sort stations_with_onhold by empty slot count
		for sta in sorted(stations_with_onhold.keys(),key = lambda x: sum([virSlots[x][slot]['zudan_status'] == 0 for slot in virSlots[x]]),reverse = 1):
			#
			#
			#
			#for each station with onhold, consider each label with priority
			for label in stations_with_onhold[sta]:
				#find the most empty slot of this station in respect of the label
				if label[0] == 1: # mono label
					choice_set = [slot for slot in monoSlots[sta][label[1]] if virSlots[sta][slot]['zudan_status'] <= 1]
				else: # multi label
					#need to consider CONDITION 1
					#for those empty slots with higher priority indicator, 
					for slot in multiSlots[sta]:
						if virSlots[sta][slot]['zudan_status'] == 0:
							label_type_index = virSlots[sta][slot]['des_support'].index(label[1])
							if virSlots[sta][slot]['des_indicator'] < label_type_index:
								virSlots[sta][slot]['des_indicator'] = label_type_index
					#
					choice_set = [slot for slot in multiSlots[sta] if (virSlots[sta][slot]['zudan_status'] <= 1 and \
					virSlots[sta][slot]['des_support'][virSlots[sta][slot]['des_indicator']] == label[1])]
				#print sta,label,choice_set
				if len(choice_set) == 0:
					if label[0] == 1: station_mono_labels[sta][label[1]] = 2 # onhold label removed to finished as choice set empty
					else: station_multi_labels[sta][label[1]] = 2 	
					#print 'empty choice set: ',sta,label
					#for a in virSlots[sta]:
					#	print virSlots[sta][a]
					continue # go to next label
				most_empty_slot = sorted(choice_set,key = lambda x: virSlots[sta][x]['remain_vol'],reverse=1)[0] # choice_set empty impossible
				#print 'chosen most empty slot %s of station %s' %(most_empty_slot,sta)
				if len(virSlots[sta][most_empty_slot]['has']) == 0:
					#most empty slot is umpty, do not need to consider volume
					fitOrders = [o for o in screenedOrdersByType[label[1]]]
				else:
					#need to consider volume
					fitOrders = [o for o in screenedOrdersByType[label[1]] if virSlots[sta][most_empty_slot]['remain_vol'] >= ORDER_VOLUMES[o]]
				#print fitOrders
				#set slot order count limit 
				orderCountLimit = 10
				if label[1] == SINGLE: orderCountLimit = stations[sta].single_batch_size
				if label[1] == NO_MERGE: orderCountLimit = stations[sta].no_merge_batch_size
				#
				if len(fitOrders) == 0: # for this station, for order type of label[0], no order can ever fit into slots supporting this type
					#print 'fitOrders Length 0'
					#set label to finished, update slot status, need to consider mono and multi seperately
					if label[0] == 1:
						station_mono_labels[sta][label[1]] = 2 # onhold label removed(changed to finished)
						for slot in monoSlots[sta][label[1]]:
							virSlots[sta][slot]['zudan_status'] = 2 # mono slot of this type finished(as no order can ever fit into any of them)
					else:
						station_multi_labels[sta][label[1]] = 2 # onhold label removed(changed to finished)
						#need to check if multi slot of sta can be set to finished 
						for slot in choice_set:
							current = virSlots[sta][slot]['des_indicator']
							#print 'please check',current,virSlots[sta][slot]['des_support']
							# if this slot has been doing order type o:
							if len(virSlots[sta][slot]['has']) > 0:
								virSlots[sta][slot]['zudan_status'] = 2 # finish this slot
							else:# if this slot has current o, but empty slot yet
								if current < len(virSlots[sta][slot]['des_support']) - 1: #current is not last
									virSlots[sta][slot]['des_indicator'] += 1 # this slot can proceed to proceessing next priority
									#CONDITION 1: the above line can triger a problem: the next indicator has no label!
								else: #current is last
									virSlots[sta][slot]['zudan_status'] = 2
					#continue to next label
					continue
				else:
					#choose seed based on S
					seed_order = chooseSeed(S,fitOrders,all_bills,thresholdS)
					#seeding the seed_order to most_empty_slot
					BATCHING_FOR_O_IN_S = int(stations[sta].order_production_type[label[1]]) == 2
					MONO = label[0] == 1
					#print 'seeding',seed_order,BATCHING_FOR_O_IN_S
					if BATCHING_FOR_O_IN_S:
						virSlots[sta][most_empty_slot]['has'].append(seed_order)
						virSlots[sta][most_empty_slot]['order_type_assigned'] = label[1]
						virSlots[sta][most_empty_slot]['zudan_status'] = 1
						virSlots[sta][most_empty_slot]['remain_vol'] -= ORDER_VOLUMES[seed_order]
						if virSlots[sta][most_empty_slot]['remain_vol'] <= 0 or len(virSlots[sta][most_empty_slot]['has']) >= orderCountLimit:
							virSlots[sta][most_empty_slot]['zudan_status'] = 2
						screenedOrders.remove(seed_order)
						screenedOrdersByType[label[1]].remove(seed_order)
					else:
						virSlots[sta][most_empty_slot]['has'].append(seed_order)
						virSlots[sta][most_empty_slot]['order_type_assigned'] = label[1]
						virSlots[sta][most_empty_slot]['zudan_status'] = 2 
						virSlots[sta][most_empty_slot]['remain_vol'] = 0 #just set to 0 as it has finished
						screenedOrders.remove(seed_order)
						screenedOrdersByType[label[1]].remove(seed_order)
					#update omega
					temp_rack_key = list(racks.keys())[0]
					for sku in all_bills[seed_order]['skus']:
						#add those in the best order
						stations[sta].omega.add(sku)
						#add those in the corresponding race faces
						for face in racks[temp_rack_key].SKUS[sku]['on_rack_faces']:
							for s in racks[face[0]].sides[face[1]]['inventory']:
								stations[sta].omega.add(s)
					#check if this station is finished
					if MONO:
						if not stationMonoFeasible(virSlots,monoSlots,staid = sta, otype = label[1]):
							station_mono_labels[sta][o] = 2 #finished					
					else:
						selected_feas,selected_fin = stationMultiFeasible(virSlots,multiSlots,staid = sta, otype = label[1])
						if selected_fin>0: station_multi_labels[sta][label[1]] = 2 #finished

					#remove all onhold labels of this station:
					for o in ORDER_ORDER:
						if station_mono_labels[sta][o] == 1:  station_mono_labels[sta][o] = 0
						if station_multi_labels[sta][o] == 1: station_multi_labels[sta][o] = 0
					#finish and break for choosing label of lower priority
					break
	#after quit outmost while, need to set all slots to finished
	for sta in virSlots:
		for slot in virSlots[sta]:
			virSlots[sta][slot]['zudan_status'] = 2
	#6.for all the batch orders, screen by volume
	ALL_SLOTS_READY = {}
	#for sta in virSlots:
	#	for slot in virSlots[sta]:
	#		print virSlots[sta][slot]
	#print station_mono_labels
	#print station_multi_labels
	#print screenedOrdersByType
	# if the zued slots need to be screened by volume utilization and order count
	if screen:
		for sta in virSlots:
			for slot in virSlots[sta]:
				#print virSlots[sta][slot]
				if len(virSlots[sta][slot]['has']) > 0 and virSlots[sta][slot]['zudan_status'] == 2:
					# batch
					if int(stations[sta].order_production_type[virSlots[sta][slot]['order_type_assigned']]) == 2:
						orderCountLimit = 10
						if virSlots[sta][slot]['order_type_assigned'] == SINGLE:
							orderCountLimit = stations[sta].single_batch_size
						if virSlots[sta][slot]['order_type_assigned'] == NO_MERGE:
							orderCountLimit = stations[sta].no_merge_batch_size
						#if the order meets volume lowerbound or reach order count limit, it can be dispatched
						if (float(virSlots[sta][slot]['remain_vol'])/virSlots[sta][slot]['vol'] <= volumeWasteRatioUpper) or \
						len(virSlots[sta][slot]['has'])>=orderCountLimit:
							ALL_SLOTS_READY[(sta,slot)] = virSlots[sta][slot]['has']
						else: # need to check time
							ready_flag = 0
							for order in virSlots[sta][slot]['has']:
								if (currentTime - all_bills[order]['last_fail_time']).total_seconds() > lastFailGapUpper:
									ALL_SLOTS_READY[(sta,slot)] = virSlots[sta][slot]['has']
									ready_flag = 1
									#print sta,slot,order
									break
							if ready_flag == 0:
								for order in virSlots[sta][slot]['has']:
									all_bills[order]['last_fail_time'] = currentTime
					# no batch
					else:
						ALL_SLOTS_READY[(sta,slot)] = virSlots[sta][slot]['has']
	else: # no need to screen
		for sta in virSlots:
			for slot in virSlots[sta]:
				if len(virSlots[sta][slot]['has']) > 0 and virSlots[sta][slot]['zudan_status'] == 2:
					ALL_SLOTS_READY[(sta,slot)] = virSlots[sta][slot]['has']
	return ALL_SLOTS_READY


def OrderAssign(all_bills, pool=[], stations={}, racks={}, currentTime='', screen=0):
    
    print('order assignment begin')

    # reinitialize the sku set [omega] for all stations
    for s in stations:
        # add the skus that have been requested to [omega]
        stations[s].omega = set(stations[s].skuRequested.keys())
        # add the sku in the incoming racks to [omega]
        for r,f in stations[s].rackFacesIncoming:
            stations[s].omega.update(set(racks[r].sides[f]['inventory']))
    print('initialize stations is done')
    

    dict_stat_slot = {}         # [dict]
    avl_slot_cnt = 0            # record the number of all the available slots
    for stat in stations:
        # find all available slots
        cur_avl_slots = [slot for slot in stations[stat].type1_slots + stations[stat].type2_slots if stations[stat].slots[slot]['status'] == 0]
        avl_slot_cnt += len(cur_avl_slots)
        
        # initialize all the available slots
        dict_stat_slot[stat] = {}
        for slot in cur_avl_slots:
            dict_stat_slot[stat][slot] = \
                    {'support':stations[stat].slots[slot]['support'], \
                    'des_support': stations[stat].slots[slot]['des_support'] ,\
                    'des_indicator':0,\
                    'vol': stations[stat].slots[slot]['vol'],\
                    'remain_vol':stations[stat].slots[slot]['vol'],\
                    'zudan_status': 0, \
                    'order_type_assigned':-1, \
                    'has':[]} # zudan status: 0 for not zued,1 for zuing, 2 for finished
                # des_indicator initially point to the top priority type in des_support
    print('find all avaiable slots is done')
    
    
    set_ord_ids = set(pool)          # all the orders in the pool to be processed 
    set_sku_ids = set(list(all_bills[o]['skus'].keys())[0] for o in set_ord_ids)
    
    # in this project, bands 'A' & 'B' are similar, and different from 'C'
    band_types = [['A','B'], ['C']]
    #band_types = set([all_bills[k]['band'] for k in set_ord_ids])
    
    # calculate the number of goods needed to be processed for each band and store the calculated info into [dict_band_sku]
    # NOTE, [dict_band_sku] can be regarded as a CLASS, and is defined with the following pattern
    # {band_label: 0-bands, 1-req_slots_cnt, 2-sreq_skus_cnt, 3-{sku: req_cnt, ords}}
    # note that: {'A': ['A','B'],...} & {'C': ['C'],...} 
    dict_band_sku = {b[0]: [b, 0, 0, {}] for b in band_types}
    for ord in set_ord_ids:
        is_assigned = False
        for sku in all_bills[ord]['skus'].keys():
            band_key = all_bills[ord]['bands'][sku]
            # transfer band 'B' to band 'A'
            if band_key not in dict_band_sku.keys():    band_key = band_types[0][0]

            if not dict_band_sku[band_key][3].__contains__(sku): dict_band_sku[band_key][3][sku] = [0,[]]
            dict_band_sku[band_key][3][sku][0] += all_bills[ord]['skus'][sku]
            # ensure the order requiring two diff. skus will only be assigned to one [dict_band_sku]
            if is_assigned == False:
                dict_band_sku[band_key][3][sku][1].append(ord)
                is_assigned = True

    for b in dict_band_sku.keys():
        # update the total required count of the sku belonging to [cur_band]
        dict_band_sku[b][2] = np.sum(dict_band_sku[b][3][sku][0] for sku in dict_band_sku[b][3].keys())
        # sort {sku:req_cnt, ords} in the nonincreasing order
        dict_band_sku[b][3] = dict(sorted(dict_band_sku[b][3].items(), key = lambda x:x[1][0], reverse = True))
        for sku in dict_band_sku[b][3].keys():
            # sort orders for each sku by the required count & process_type in the nonincreasing order
            dict_band_sku[b][3][sku][1]= sorted(dict_band_sku[b][3][sku][1], key = lambda x: (all_bills[x]['skus'][sku], all_bills[x]['process_type']), reverse = True)
        
    
    # calculate available slots for each band
    total_req_sku_cnt = np.sum(dict_band_sku[b][2] for b in dict_band_sku.keys())
    for b in dict_band_sku.keys():
        dict_band_sku[b][1] = int(0.5 + avl_slot_cnt * dict_band_sku[b][2] / total_req_sku_cnt)
    print('define [dict_band_sku] is done')
        

    # calculate order volume
    ord_volumes = {o : sum([racks[list(racks.keys())[0]].SKUS[sku]['volume'] * all_bills[o]['skus'][sku] for sku in all_bills[o]['skus']]) \
            for o in set_ord_ids}
    

    # for each slot at each station, assign orders into the slot
    for stat in stations:
 
        # find the skus spreaded on more than two stations
        set_spreaded_sku = set()   
        for sa in stations:
            for sb in stations:
                if sb<=sa:  continue
                omega_intersection = stations[sa].omega & stations[sb].omega
                if len(omega_intersection)>0:
                    set_spreaded_sku.update(omega_intersection)

       
        # [cur_slot_cnt] is the number of aviable slots at currrent station
        band_key = max(dict_band_sku, key = lambda x: dict_band_sku[x][1]>0)
        # [cur_band] is for searching for target goods with this band;
        cur_band = dict_band_sku[band_key][0]

        for slot in dict_stat_slot[stat]:
            
            # switch to the next band if the assigned slots for cur_band is use up
            if dict_band_sku[band_key][1] <= 0:
                band_key = max(dict_band_sku, key = lambda x: dict_band_sku[x][1]>0)
                cur_band = dict_band_sku[band_key][0]
            # update the avaiable slots for [cur_band]
            dict_band_sku[band_key][1] -= 1
           
            
            choosen_sku = ''
            set_inter_sku = set(dict_band_sku[band_key][3].keys()) & stations[stat].omega
            #++++++++++++++++++++++++++++
            # ROUND ONE - find all orders whose required skus are also in current omega
            if len(set_inter_sku) > 0:
                choosen_sku = list(set_inter_sku)[0]
            else:
                #++++++++++++++++++++++++++++
                # ROUND TWO - choose the first sku that (1) is still required by some orders, and (2) is not spread in more than two stations
                for sku in dict_band_sku[band_key][3].keys():
                    if dict_band_sku[band_key][3][sku][0]>0 and (sku not in set_spreaded_sku):
                        choosen_sku = sku
                        break
            

            # update the sku set [omega] at current station
            stations[stat].omega.add(choosen_sku)

            # print(slot, choosen_sku, dict_band_sku[band_key][3][choosen_sku][0], len(dict_band_sku[band_key][3][choosen_sku][1]))
            fill_ord_lmt = min(20, len(dict_band_sku[band_key][3][choosen_sku][1]))     # where 20 is the limit of a batch of mono-orders
            break_point = 0
            for ord in dict_band_sku[band_key][3][choosen_sku][1]:

                # update current slot
                dict_stat_slot[stat][slot]['has'].append(ord)
                #dict_stat_slot[stat][slot]['order_type_assigned'] = o
                dict_stat_slot[stat][slot]['zudan_status'] = 1
                dict_stat_slot[stat][slot]['remain_vol'] -= ord_volumes[ord]
                
                # update [dict_band_sku]
                dict_band_sku[band_key][3][choosen_sku][0] -= all_bills[ord]['skus'][choosen_sku]

                # break, 1) if current order is not the mono-order(requiring single sku); 2) if current order is a mono-order and reeach the predefined limit
                break_point += 1
                if (break_point >= fill_ord_lmt or all_bills[ord]['process_type'] == 1): break

            del dict_band_sku[band_key][3][choosen_sku][1][0:break_point]

            # remove the first element of dict_band_sku[band_key][3][0] if it is empty
            if dict_band_sku[band_key][3][choosen_sku][0] == 0 or len(dict_band_sku[band_key][3][choosen_sku][1]) == 0:
                del dict_band_sku[band_key][3][0]

    print('order assignment is done')


    # assigned slots that will be returned
    all_slots_ready = {}
    for stat in stations:
        for slot in dict_stat_slot[stat]:
            all_slots_ready[(stat,slot)] = dict_stat_slot[stat][slot]['has']

    return all_slots_ready


