# -*- coding: utf-8 -*-
'''
IO functions for inventory-related files

Purpose
- read various inventory-related files
- convert to python dict

Input
1. sku_info.csv >To> SKUS

- goods_no : SKU ID 
- volume : volume per item (in mm^3)
- weight : in kg
- clicks : clicks

2. sku_relevance.csv >To> SKUS_CORRELATION

- goods_no
- goods_no_rel
- relevance : 0-1

3. points.csv >To> STOCKS_POSITION

- id : stock position ID (in format: 10xx10yy)
- x : x for stock position
- y : y for stock position
- container_no: ID for rack on the stock position (-1 if none)

4. container_stock.csv >To> RACKS

- rack_id : rack ID
- side_id : side A (0) or side B(1)
- sku_id : SKU ID
- sku_quantity : number of items of the SKU
#- sku_volume: volume per items
- stock_position_id : ID for stock position having this rack

#- direction: direction of the side of the rack
#- band: rack band
#- remaining_volume: remaining volume of the side of the rack

5. outbound_bills.csv >To> OUTBOUND_BILLS

- bill_no : bill number
- goods_no : sku id
- qty : quantity
- process_type : order type (currently, 0 for single item order, 1 for multi item order)
- create_time : time when this order is placed
- deadline_time : time when this order is due

6. station_slot_setup.csv >To> STATIONS

- id : station id
- x and y : coordinates of the station
- order_production_type : a string to indicate production type of each kind of order
- slot_total : total number of slots of this station
- type1 to type10 : slot (supported_order_type%volume%count)


Author
siyu li
(solafishes@gmail.com)

'''

import csv
from datetime import datetime
import sys
sys.dont_write_bytecode = True

def readcsv(f):
	with open(f) as file:
		reader = csv.DictReader(file)
		contents = []
		for row in reader:
			contents.append(row)
		return contents


#def writecsv(f):


#return all SKUS as a dict
def getSKUS(f_dir):
	contents = readcsv(f_dir)
	SKUS = {}
	for c in contents:
		s = c['goods_no']
		SKUS[s] = {}
		SKUS[s]['volume'] = float(c['volume'])
		SKUS[s]['weight'] = float(c['weight'])
		SKUS[s]['clicks'] = float(c['clicks'])
		SKUS[s]['band'] = c['band']
	return SKUS

#return all SKU correlations as a dict
def getSKUS_CORRELATION(f_dir):
	contents = readcsv(f_dir)
	SKUS_CORRELATION = {}
	for c in contents:
		SKUS_CORRELATION[(c['goods_no'],c['goods_no_rel'])] = float(c['relevance'])
	return SKUS_CORRELATION

#return all stock position with initial rack id	
def getSTOCKS(f_dir):
	contents = readcsv(f_dir)
	STOCKS_POSITION = {}
	for c in contents:
		ID = int(c['id'])
		STOCKS_POSITION[ID] = {}
		STOCKS_POSITION[ID]['x'] = int(c['x'])
		STOCKS_POSITION[ID]['y'] = int(c['y'])
		STOCKS_POSITION[ID]['rack_id'] = int(c['container_no']) #-1 if the stock position has no rack on it.
	return STOCKS_POSITION 


#return all racks as a dict
'''
Rack
|___Side0_{SKU:QTY,}_direction_remaining_volume
|___Side1_{SKU:QTY,}_direction_remaining_volume
|___stock_position_id
|___band

'''	
def getRACKS(f_dir):
	contents = readcsv(f_dir)
	#looking for shelf_no in racks, if column not provided, place no shelf inventory!
	if 'shelf_no' in contents[0]:
		SHELF_INVENTORY_PROVIDED = 1
	else:
		SHELF_INVENTORY_PROVIDED = 0
	RACKS = {}
	for c in contents:
		ID = int(c['container_no'])
		SIDE = int(c['surface_no'])
		SKU = c['goods_no']
		QTY = int(float(c['qty']))
		if SHELF_INVENTORY_PROVIDED:
			SHELF_NO = int(c['shelf_no'])
		if ID not in RACKS:
			RACKS[ID] = {}
			RACKS[ID]['ID'] = ID
			RACKS[ID]['sides'] = {}
			#RACKS[ID]['position'] = int(c['stock_position_id'])
			RACKS[ID]['position'] = 0 # input version 2
			#RACKS[ID]['band'] = int(c['band'])
		if SIDE not in RACKS[ID]['sides']:
			RACKS[ID]['sides'][SIDE] = {}
			RACKS[ID]['sides'][SIDE]['direction'] = int(c['direction'])
			#RACKS[ID]['sides'][SIDE]['remaining_volume'] = float(c['remaining_volume'])
			RACKS[ID]['sides'][SIDE]['inventory'] = {}
			if SHELF_INVENTORY_PROVIDED:
				RACKS[ID]['sides'][SIDE]['shelf_inventory'] = {}
		if SKU != '-1': # otherwise this side is empty
			if SKU not in RACKS[ID]['sides'][SIDE]['inventory']:
				RACKS[ID]['sides'][SIDE]['inventory'][SKU] = 0
			RACKS[ID]['sides'][SIDE]['inventory'][SKU] += QTY
			if SHELF_INVENTORY_PROVIDED:
				if SHELF_NO not in RACKS[ID]['sides'][SIDE]['shelf_inventory']:
					RACKS[ID]['sides'][SIDE]['shelf_inventory'][SHELF_NO] = {}
				if SKU not in RACKS[ID]['sides'][SIDE]['shelf_inventory'][SHELF_NO]:
					RACKS[ID]['sides'][SIDE]['shelf_inventory'][SHELF_NO][SKU] = 0
				RACKS[ID]['sides'][SIDE]['shelf_inventory'][SHELF_NO][SKU] += QTY
	return RACKS

#return all orders
'''
outbound_bill
|___bill_no
|___process_type
|___place_time
|___deadline_time
|___{SKU:QTY,}

'''

def getOUTBOUND_BILLS(f_dir):
    contents = readcsv(f_dir)
    OUTBOUND_BILLS = {}
    for c in contents:
        BILL_ID = c['bill_no']
        SKU = c['goods_no']
        QTY = int(float(c['qty']))
        if BILL_ID not in OUTBOUND_BILLS:
            OUTBOUND_BILLS[BILL_ID] = {}
            OUTBOUND_BILLS[BILL_ID]['process_type'] = int(c['process_type'])
            OUTBOUND_BILLS[BILL_ID]['place_time'] = datetime.strptime(c['create_time'], "%Y/%m/%d %H:%M")
            OUTBOUND_BILLS[BILL_ID]['deadline_time'] = datetime.strptime(c['deadline_time'], "%Y/%m/%d %H:%M")
            OUTBOUND_BILLS[BILL_ID]['last_fail_time'] = datetime.strptime('2222-01-01 12:00:00', "%Y-%m-%d %H:%M:%S") #a super large time
            OUTBOUND_BILLS[BILL_ID]['status'] = -1
            OUTBOUND_BILLS[BILL_ID]['skus'] = {}
            OUTBOUND_BILLS[BILL_ID]['bands'] = {}
        OUTBOUND_BILLS[BILL_ID]['skus'][SKU] = QTY
        ### NOTE: this is a bad definition, since {SKU:QTY} & {SKU:BAND} can be combined in one dict., but that would bring some other unforcast errors
        OUTBOUND_BILLS[BILL_ID]['bands'][SKU] = c['band']
    return OUTBOUND_BILLS

#return all station-slot setup according to the provided files
#this function is optional when running the web service,
#if no setup files are provided, the default setep is used 

def getSTATIONS_SLOTS(f_dir='',sis = ''):
	#f_dir by default is '', if not provided, it will generate default station-slot setup.
	#if f_dir is provided, it will generate station-slot setup according to the file.
	#sis = 'id,x,y,slot_total,slot_vol,single_batch,no_merge_batch:id,x,y,slot_total,slot_vol,single_batch,no_merge_batch'
	STATIONS = {}
	#use default setup
	if f_dir == '':
		setup = [literal_eval(i) for i in sis.split(':')]
		for s in setup:
			STATIONS[s[0]] = {}
			STATIONS[s[0]]['ID'] = s[0]
			STATIONS[s[0]]['x'] = s[1]
			STATIONS[s[0]]['y'] = s[2]
			STATIONS[s[0]]['order_production_type'] = '2100000000'
			STATIONS[s[0]]['slot_total'] = s[3]
			STATIONS[s[0]]['single_batch_size'] = s[5]
			STATIONS[s[0]]['no_merge_batch_size'] = s[6]
			STATIONS[s[0]]['slots'] = {}
			for i in range(s[3]):
				STATIONS[s[0]]['slots'][i] = {'support':'11000000000','vol':float(s[4]),'status':0,'has':[],'deadline':-1}
	#use file-specific setup
	else:
		contents = readcsv(f_dir)
		for s in contents:
			ID = int(s['ID'])
			STATIONS[ID] = {}
			STATIONS[ID]['ID'] = ID
			STATIONS[ID]['x'] = int(s['x'])
			STATIONS[ID]['y'] = int(s['y'])
			STATIONS[ID]['order_production_type'] = s['order_production_type']
			STATIONS[ID]['slot_total'] = int(s['slot_total'])
			STATIONS[ID]['single_batch_size'] = int(s['single_batch_size'])
			STATIONS[ID]['no_merge_batch_size'] = int(s['no_merge_batch_size'])
			STATIONS[ID]['slots'] = {}
			c = 0
			for i in range(1,11):
				t = s['type%s'%i]
				if t == '': break
				slot_type = t.split('%')
				for k in range(int(slot_type[2])):
					#status 0 for available, 1 for occupied
					STATIONS[ID]['slots'][c] = {'support':slot_type[0],'vol':float(slot_type[1]),'status':0,'has':[],'deadline':-1}
					c += 1
	return STATIONS

