# -*- coding: utf-8 -*-
'''
Classes used in dilang projects

Author
siyu li
(solafishes@gmail.com)

order type:
0: single-item order
1: multi-item order (no merge needed)
2: all other order types (merge, special, backOwner, toSpare, bulk, move, internal, JZD, FOO)

batching type:
1: no-batching (single-order processing)
2: batching (batch-order processing)
0: not support this type 

for each station, the order_production_type needs to provide the batching type for all order type
for example: 21100000000 means for single-item order (0) , batching-order processing (2) is used,
for multiple-item order (1), single-order processing (1) is used, for the rest type of orders (2),
single-order processing (1) is used. noted that 11 digits represents 11 different order types, 
it support future expansions when more order types are used

for each slot, the support_order type need to provide the order types that the slot can process,
for example: 11000000000 means the slot can process both single- and multi-item orders, but not other types

order type priority: from 1 to 11, the larger the number, the higher the priority, this is specified in config_local.py
 
'''

import sys

sys.dont_write_bytecode = True
import zudan
#from pai_solve import solve
from order_priority import *
from datetime import datetime

# rack status
AT_STOCK = 0  # the rack in the stock position
RETURN_PENDING = 1  # the rack just finished picking, will either go to other station or return to stock
CHU_ENROUTE = 2  # the rack is on the way to station (right after scheduling, before finished picking)
RETURN_ENROUTE = 3  # the rack is on the way back to stock
RETURN_ENROUTE_NOCHU = 4  # not used anymore
RU_ENROUTE = 5  # the rack is on the way to station for intake tasks.
CHU_ENROUTE_NOADD = 6  # the rack is on the way to station but not able to add more

# order status
NOT_YET = -1  # not yet to be placed because time is not reached
PLACED = 0  # reach time, the order is placed
ZUED = 1  # the order has been zued.

ORDER_ORDER_DES = sorted(ORDER_PRI.keys(), key=lambda x: ORDER_PRI[x], reverse=1)

direction_lookup = {1: 3, 2: 4, 3: 1, 4: 2}


class Station:

    def __init__(self, S):
        # station_id
        self.ID = S['ID']
        self.x = S['x']
        self.y = S['y']
        self.slots = S['slots']
        self.order_production_type = S['order_production_type']
        self.single_batch_size = S['single_batch_size']
        self.no_merge_batch_size = S['no_merge_batch_size']
        self.rackFacesIncoming = []
        # for each SKU, sku requested record the qty required
        self.skuRequested = {}
        self.omega = set()
        # for each station, skupreFulfilled is sku qty that are already paichaned
        # (suggesting there are racks incoming with these sku qty loaded)
        self.skupreFulfilled = {}
        # create variables for quick pre-screening
        self.type1_slots = []
        self.type2_slots = []
        single_no_batch = int(self.order_production_type[SINGLE]) == 1
        nomerge_no_batch = int(self.order_production_type[NO_MERGE]) == 1
        for s in self.slots:
            slot_support_single = int(self.slots[s]['support'][SINGLE]) == 1
            slot_support_nomerge = int(self.slots[s]['support'][NO_MERGE]) == 1
            if (single_no_batch and slot_support_single) or (nomerge_no_batch and slot_support_nomerge):
                self.type1_slots.append(s)
            else:
                self.type2_slots.append(s)
            # add a list: all supported order types (higher priority in front)
            self.slots[s]['des_support'] = []
            for o in ORDER_ORDER_DES:
                if self.slots[s]['support'][o] == '1':
                    self.slots[s]['des_support'].append(o)

    def updatePreFulfilled(self, sku_qty={}, add=True):
        # when paichan is finished
        if add:
            for sku in sku_qty:
                if sku in self.skupreFulfilled:
                    self.skupreFulfilled[sku] += sku_qty[sku]
                else:
                    self.skupreFulfilled[sku] = sku_qty[sku]
        # when picking is finished
        else:
            for sku in sku_qty:
                self.skupreFulfilled[sku] -= sku_qty[sku]
                if self.skupreFulfilled[sku] <= 0:
                    del self.skupreFulfilled[sku]

    def releaseSlot(self, slot_id):
        self.slots[slot_id]['has'] = []  # when a slot is released, all bills in the slot are collected
        self.slots[slot_id]['status'] = 0  # available
        self.slots[slot_id]['deadline'] = -1

    def occupySlot(self, slot_id, allBills, bill_IDs=[]):
        self.slots[slot_id]['has'] = bill_IDs
        self.slots[slot_id]['status'] = 1  # not available
        # self.slots[slot_id]['deadline'] = sorted([allBills[b]['deadline_time'] for b in bills])[0]
        # add requested item qty by sku
        for b in bill_IDs:
            for item in allBills[b]['skus']:
                if item not in self.skuRequested:
                    self.skuRequested[item] = 0
                self.skuRequested[item] += allBills[b]['skus'][item]

    def addIncoming(self, rackFace):
        self.rackFacesIncoming.append(rackFace)

    def dedIncoming(self, rackFace, skuPlaced={}):
        self.rackFacesIncoming.remove(rackFace)
        # deduct requested item qty by sku
        for sku in skuPlaced:
            self.skuRequested[sku] -= skuPlaced[sku]
            if self.skuRequested[sku] <= 0:
                del self.skuRequested[sku]


class Rack:
    SKUS = {}  # skus as a static variable of class Rack
    for sku in SKUS:
        SKUS[sku]['on_rack_faces'] = []

    def __init__(self, R):
        self.ID = R['ID']
        self.position = R['position']  # in (x,y)
        self.sides = R['sides']
        # initialize sku look up table in Rack.SKUS
        for side in self.sides:
            for sku in self.sides[side]['inventory']:
                if 'on_rack_faces' not in self.SKUS[sku]:
                    self.SKUS[sku]['on_rack_faces'] = []
                if self.sides[side]['inventory'][sku] > 0:
                    self.SKUS[sku]['on_rack_faces'].append((self.ID, side))
        # self.band = R['band']
        self.status = 0  # this need to update with flexsim
        # pre-occupied inventory by paichan
        for side in self.sides:
            self.sides[side]['pre_occupied_inventory'] = {}
        ###NOTE###
        '''
        pre_occupied_inventory works at side level, it is updated when paichan finished (+) and when
        picking finished (-). 

        Shelf-based inventory is only used 
        (1) when picking, based on pre occupied inventory, select layer:qty, and calculate time accordingly
        (2) when finished picking, reduce layer-based inventory
        '''

    # this is used to update pre-occupied inventory of this rack
    def updatePreOccupied(self, face, skupreOccupied={}, add=True):
        # when paichan is finished, some inventory need to be pre-occupied
        if add:
            for sku in skupreOccupied:
                if sku in self.sides[face]['pre_occupied_inventory']:  # add up
                    self.sides[face]['pre_occupied_inventory'] += skupreOccupied[sku]
                else:
                    self.sides[face]['pre_occupied_inventory'][sku] = skupreOccupied[sku]
                if self.sides[face]['pre_occupied_inventory'][sku] >= self.sides[face]['inventory'][sku]:
                    # all inventory of this sku of this rack face are pre-occupied: remove from on_rack_face
                    self.SKUS[sku]['on_rack_faces'].remove((self.ID, face))
        # when rack picking finished
        else:
            for sku in skupreOccupied:
                # print len(self.sides[face]['pre_occupied_inventory'].keys())
                self.sides[face]['pre_occupied_inventory'][sku] -= skupreOccupied[sku]
                if self.sides[face]['pre_occupied_inventory'][sku] <= 0:
                    del self.sides[face]['pre_occupied_inventory'][sku]

    '''
    #used for shelfing and timing when picking
    def shelfing_timing(self,face):
        #based on pre_occupied_inventory, for each sku, calculate qty at each layer, 
        
        #calculate each sku each layer a time

        #return 
    
    #
    def _reduce_shelf(self,face,sku_taken_from_shelf={}):
    '''

    # to deduct the picked items from rack face.
    def reduce(self, face, skuTaken={}):
        for sku in skuTaken:
            qty = skuTaken[sku]
            self.sides[face]['inventory'][sku] -= qty
            # self.sides[face]['remaining_volume'] -= qty * self.SKUS[sku]['volume']
            if self.sides[face]['inventory'][sku] <= 0:
                del self.sides[face]['inventory'][sku]
        # self.SKUS[sku]['on_rack_faces'].remove((self.ID,face)) # wrong

    # to add items to rack face (used in ruku)
    # def add()

    # to update status of this rack (including status,position,direction)
    def updateStatus(self, status, pos, face_direction):
        self.status = status
        self.position = pos  # in (x,y)
        # face_direction only provide direction info for one
        self.sides[face_direction[0]]['direction'] = face_direction[1]
        for side in self.sides:
            if side != face_direction[0]:
                self.sides[side]['direction'] = direction_lookup[face_direction[1]]


# update position of rack and direction of faces
# def updatePos(self,xy):
#	self.position = xy

class Warehouse:
    Racks = {}  # ID as key, class instance as value
    Stations = {}  # ID as key, class instance as value

    def __init__(self, allBills):
        # self.maxTry = maxTry
        self.allBills = allBills
        self.zudanPool = set()  # only store bill id and try number in zudan

    def insertPool(self, list_of_bills):
        for b in list_of_bills:
            self.zudanPool.add(b)
            self.allBills[b]['status'] = PLACED

    # this is used when list_of_bills are all zued
    def removePool(self, list_of_bills):
        for b in list_of_bills:
            self.zudanPool.remove(b)
            self.allBills[b]['status'] = ZUED

    # zudan
    '''
    currentTime: current simulation time as datetime string
    currentJiedan: current boci jiedan time as datetime string
    nearTime: in seconds, from currentJiedan-nearTime to currentJiedan, defined as close jiedan time.

    whenever zudan is called, the pool is divided into several parts:
    Part 1: orders with jiedan time < currentJiedan
    Part 2: orders with jiedan time = currentJiedan
    Part 3: orders with jiedan time > currentTime
    
    Part 1 is zued first, if some orders are left, terminate zudan, else continue;
    if currentTime <currentJiedan - nearTime: put part 2 and 3 together, zudan
    else:
        Part 2 is zued, if some orders are left,terminate zudan, else continue to process Part 3
    
    for part 1 orders, part 2 orders (when currentTime >=currentJiedan-nearTime),
    zued orders will not be further screened by volume lowerbound and order limit.
    '''

    def _zu_cleanup(self, result):
        zued_orders = set()
        for sta_slot in result:
            # update order status
            for o in result[sta_slot]:
                zued_orders.add(o)
            # occupy slot
            self.Stations[sta_slot[0]].occupySlot(sta_slot[1], self.allBills, bill_IDs=result[sta_slot])
        self.removePool(zued_orders)  # remove these zued orders from zudan pool
        return zued_orders

    def zu(self, currentTime='', currentJiedan='', nearTime=0):
        all_result = {}  # this stores all the zued slots in this zu()
        currentTime = datetime.strptime(currentTime, "%Y-%m-%d %H:%M:%S")
        currentJiedan = datetime.strptime(currentJiedan, "%Y-%m-%d %H:%M:%S")
        # when zu is called self.insertPool() must be called to update zudan pool according to currentTime!
        orders_to_update = [o for o in self.allBills if
                            self.allBills[o]['place_time'] <= currentTime and self.allBills[o]['status'] == NOT_YET]
        self.insertPool(orders_to_update)
        # get part 1 orders: delayed_orders
        delayed_pool = set([o for o in self.zudanPool if self.allBills[o]['deadline_time'] < currentJiedan])
        # get part 2 orders: orders of this boci
        current_pool = set([o for o in self.zudanPool if self.allBills[o]['deadline_time'] == currentJiedan])
        # get part 3 orders: orders of future boci
        future_pool = set([o for o in self.zudanPool if self.allBills[o]['deadline_time'] > currentJiedan])
        #
        if len(delayed_pool) > 0:
            result = zudan.OrderAssign(self.allBills, pool=delayed_pool, stations=self.Stations, racks=self.Racks, screen=0, currentTime=currentTime)
            #result = zudan.zudanXPY(self.allBills, pool=delayed_pool, stations=self.Stations, racks=self.Racks, screen=0, currentTime=currentTime)

            # cleanup
            zued_orders = self._zu_cleanup(result)
            # update delayed pool
            delayed_pool -= zued_orders
            # add to all zued batches
            all_result.update(result)
        # if delayed pool has orders left, terminate zudan
        if len(delayed_pool) > 0:
            return all_result
        else:
            # check if need to mix boci
            MIX = (currentJiedan - currentTime).total_seconds() > nearTime
            if MIX:
                second_pool = current_pool | future_pool  # pool used for second time zudan
                SCREEN = 1  # if MIX, need to screen when dispatching slot
            else:
                second_pool = current_pool
                SCREEN = 0
        #
        if len(second_pool) > 0:
            result = zudan.OrderAssign(self.allBills, pool=delayed_pool, stations=self.Stations, racks=self.Racks, screen=0, currentTime=currentTime)
            #result = zudan.zudanXPY(self.allBills, pool=second_pool, stations=self.Stations, racks=self.Racks, screen=SCREEN, currentTime=currentTime)

            # cleanup
            zued_orders = self._zu_cleanup(result)
            # update second pool
            second_pool -= zued_orders
            # add to all zued batches
            all_result.update(result)
            if MIX:
                return all_result
            else:
                if len(second_pool) > 0:
                    return all_result
                else:
                    result = zudan.OrderAssign(self.allBills, pool=delayed_pool, stations=self.Stations, racks=self.Racks, screen=0, currentTime=currentTime)
                    #result = zudan.zudanXPY(self.allBills, pool=future_pool, stations=self.Stations, racks=self.Racks, screen=1, currentTime=currentTime)

                    # clean_up
                    zued_orders = self._zu_cleanup(result)
                    # update future_pool (skip,this zu is terminated regardless)
                    # add to all zued batches
                    all_result.update(result)
                    return all_result
        else:
            return all_result

    def _get_addup_qty(self, rf, sku, qty):
        if sku not in self.Racks[rf[0]].sides[rf[1]]['inventory']: return 0
        if sku not in self.Racks[rf[0]].sides[rf[1]]['pre_occupied_inventory']:
            inv = self.Racks[rf[0]].sides[rf[1]]['inventory'][sku]
        else:
            inv = self.Racks[rf[0]].sides[rf[1]]['inventory'][sku] - self.Racks[rf[0]].sides[rf[1]][
                'pre_occupied_inventory']
        return min(inv, qty)

    # paichan
    '''
    (1) all demand (accumulated sku count for each station) has beed stored in self.Stations.skuRequested 
    (2) all racks info are stored in self.Racks:
        i.Only racks with status: AT_STOCK(0) CHU_ENROUTE(2) RETURN_ENROUTE(3) can be available for paichan
        ii.Location of racks with status CHU_ENROUTE(2) and RETURN_ENROUTE(3) are not accurate, however these location are 
            fed by agv info
        iii. agv info should be provided, id, loc, rackOn

    '''

    def pai(self, agvs, rack_on_agv, buffers, distanceLookup):
        # addup_result[rack_id,face_id,sta] ={sku:qty,...}
        addup_result = {}  # store rack with status CHU_ENROUTE(2) used for add up
        # format data
        AGVS = [a for a in agvs if agvs[a]['avail'] == 1]
        Stations = [k for k in buffers if buffers[k] > 0]  # only consider those stations has at least 1 buffer
        demandStation2SKU = {}  # record the demand of sku for each station
        for sta in Stations:
            demandStation2SKU[sta] = {}
            possible_addup_rackface = [rf for rf in self.Stations[sta].rackFacesIncoming if
                                       self.Racks[rf[0]].status == CHU_ENROUTE]
            for sku in self.Stations[sta].skuRequested:
                qty = self.Stations[sta].skuRequested[sku]
                if sku in self.Stations[sta].skupreFulfilled:
                    qty -= self.Stations[sta].skupreFulfilled[
                        sku]  # requested - prefulfilled is the actual qty pending paichan
                ##search for CHU_ENROUTE(2) racks heading for this station that can be used for addup.
                if qty > 0:
                    for rf in possible_addup_rackface:
                        addup_qty = self._get_addup_qty(rf, sku, qty)
                        if addup_qty > 0:
                            # can addup, update addup_result
                            if (rf[0], rf[1], sta) not in addup_result: addup_result[(rf[0], rf[1], sta)] = {}
                            addup_result[(rf[0], rf[1], sta)][sku] = addup_qty
                            qty -= addup_qty
                            if qty <= 0: break
                if qty > 0:
                    demandStation2SKU[sta][sku] = qty
        # remove station with no need
        Stations_to_solve = []
        for sta in demandStation2SKU:
            if len(demandStation2SKU[
                       sta].keys()) == 0:  # the station has no need or the need has ALL been fultilled by addup
                del demandStation2SKU[sta]
            else:
                Stations_to_solve.append(sta)
        # get all SKUS involved
        SKUs = set()
        for sta in demandStation2SKU:
            for sku in demandStation2SKU[sta]:
                SKUs.add(sku)
        # formalize demandStation2SKU
        for sta in Stations:
            for sku in SKUs:
                if sku not in demandStation2SKU[sta]: demandStation2SKU[sta][sku] = 0
        # get inventory, racks, and 2 cost dicts
        Inventory = {}
        possible_rfs = set()
        for sku in SKUs:
            # for each sku, if a rf is on rack faces, it is ensured that its available inventory for this SKU >0
            for rf in self.Racks[self.Racks.keys()[0]].SKUS[sku]['on_rack_faces']:
                if self.Racks[rf[0]].status in [AT_STOCK, RETURN_ENROUTE]:
                    possible_rfs.add(rf)
        for rf in possible_rfs:
            for sku in SKUs:
                if sku not in self.Racks[rf[0]].sides[rf[1]]['inventory']:
                    qty = 0
                else:
                    if sku not in self.Racks[rf[0]].sides[rf[1]]['pre_occupied_inventory']:
                        qty = self.Racks[rf[0]].sides[rf[1]]['inventory'][sku]
                    else:
                        qty = self.Racks[rf[0]].sides[rf[1]]['inventory'][sku] - \
                              self.Racks[rf[0]].sides[rf[1]]['pre_occupied_inventory'][sku]
                Inventory[(rf[0], rf[1], sku)] = qty  # qty is the available inventory on rack face
        # get rack list
        Racks = list(set([k[0] for k in Inventory]))
        # Faces =list(set([(key[0],key[1]) for key in Inventory]))
        Faces = list(possible_rfs)
        # generate rack to station cost
        costRack2Station = {}
        costAGV2Rack = {}
        for r in Racks:
            if self.Racks[r].status == RETURN_ENROUTE:
                pos_rack = agvs[rack_on_agv[r]][
                    'pos']  # for racks with status RETURN_ENROUTE, its pos is determined by its agv
            else:
                pos_rack = self.Racks[r].position  # on stock position
            for sta in Stations_to_solve:
                pos_station = (self.Stations[sta].x, self.Stations[sta].y)
                costRack2Station[r, sta] = distanceLookup[pos_rack[0], pos_rack[1], pos_station[0], pos_station[1]]
            # generate car to rack cost
            for agv in agvs:
                costAGV2Rack[agv, r] = abs(pos_rack[0] - agvs[agv]['pos'][0]) + abs(pos_rack[1] - agvs[agv]['pos'][1])
        # flag whether a rack has status RETURN_ENROUTE (has agv below)
        RACK_RETURN_ENROUTE = {}
        for r in Racks:
            if self.Racks[r].status == RETURN_ENROUTE:
                RACK_RETURN_ENROUTE[r] = 1
            else:
                RACK_RETURN_ENROUTE[r] = 0
        # solve paichan
        new_result = solve(AGVs=AGVS, \
                           Racks=Racks, \
                           costAGV2Rack=costAGV2Rack, \
                           Stations=Stations_to_solve, \
                           costRack2Station=costRack2Station, \
                           SKUs=SKUs, \
                           demandStation2SKU=demandStation2SKU, \
                           inventory=Inventory, \
                           Faces=Faces, \
                           ALPHA=ALPHA, \
                           SLOT_WAITING=buffers, \
                           rackHasAGV=RACK_RETURN_ENROUTE)
        # now we get addup_result and new_result
        '''
        1. for addup_result: 
        (1) for each rack face, update pre-occupied
        (2) for the destinated station, update pre-fulfilled
        2. for new_result
        (1) for each rack face, update pre-occupied
        (2) for the destinated station, update pre-fulfilled
        (3) rack_link: addIncoming

        '''
        #
        for rfs in addup_result:
            # (1)
            self.Racks[rfs[0]].updatePreOccupied(rfs[1], skupreOccupied=addup_result[rfs], add=True)
            # (2)
            self.Stations[rfs[2]].updatePreFulfilled(sku_qty=addup_result[rfs], add=True)
        #
        for rf in new_result:
            # (1)
            self.Racks[rf[0]].updatePreOccupied(rf[1], skupreOccupied=new_result[rf]['skus_taken'], add=True)
            # (2)
            self.Stations[new_result[rf]['station']].updatePreFulfilled(sku_qty=new_result[rf]['skus_taken'], add=True)
            # (3)
            self.Stations[new_result[rf]['station']].addIncoming(rf)
        return addup_result, new_result
