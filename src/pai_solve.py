import itertools
from pyscipopt import Model,quicksum,LP
import pyscipopt

#every station has its available slots count
#MAX_SLOTS_WAITING=6

def crossproduct(A,B):
	return list(itertools.product(A,B))


def solveYZ(Racks={},Stations={},costRack2Station={},SKUs={},demandStation2SKU={},inventory={},Faces={},ALPHA={},AGV_qty=0,SLOT_WAITING={},rackHasAGV={}):
	#ALPHA
	ALPHA1 = ALPHA[1]
	ALPHA2 = ALPHA[2]
	ALPHA3 = ALPHA[3]
	print('Making Model...')
	#Initialte SCIP Model instance
	model = Model('Tri-partite-YZ')
	#create Y[j,k] for j in Faces, k in Stations ---- Assigning Faces[j] to Stations[k]\
	Y={}
	for (j,k) in crossproduct(Faces,Stations):
		Y[j,k] = model.addVar(vtype='B',name = 'To assign %s-%s to %s' %(j[0],j[1],k))
	#create Z[k,s] for k in Stations, s in SKUs ---- Compensation Variable for Station k and SKU s
	Z={}
	for (k,s) in crossproduct(Stations,SKUs):
	    Z[k,s] = model.addVar(vtype='C',name = 'Z(%s,%s)' % (k,s))
	#Adding Constraints (2) A Face can be assigned to at most 1 Station
	for j in Faces:
		model.addCons(quicksum(Y[j,k] for k in Stations)<=1, name = 'Cons (2) for %s-%s' % (j[0],j[1]))
	# Adding Constraints (7) for each SKU, the sum up re
	for s in SKUs:
		Js = [j for j in Faces if inventory[j[0],j[1],s]>0]
		for k in Stations:
			model.addCons(quicksum(Y[j,k]*inventory[j[0],j[1],s] for j in Js)>= (demandStation2SKU[k][s]-Z[k,s]),name = 'Cons (7) for %s-%s' % (k,s))
	# Constraints (8) availability of waiting slots for each station
	for k in Stations:
		model.addCons(quicksum(Y[j,k] for j in Faces)<=SLOT_WAITING[k], name= 'Cons (8) for %s' % k)
	#Constraints (12) For racks with A and B faces in Faces, only one can be matched.
	doubleRacks = [i for i in Racks if (i,1) in Faces and (i,2) in Faces]
	for t in doubleRacks:
		model.addCons(quicksum(Y[(t,1),k] + Y[(t,2),k] for k in Stations)<=1, name = 'Cons (12) for %s' % t)
	#Constraint AGV Resource
	model.addCons(quicksum(Y[j,k] * (1-rackHasAGV[j[0]]) for (j,k) in Y)<=AGV_qty,name = 'Constraint on AGV Qty')
	model.setObjective(
		ALPHA2 * quicksum(Y[j,k] * costRack2Station[j[0],k] for (j,k) in Y) + 
		ALPHA3 * quicksum(Z[k,s] for (k,s) in Z),"minimize")
	model.data =  Y,Z
	print('Optimizing...')
	model.optimize()
	faces_station = {}
	for (j,k) in Y:
	 	a=model.getVal(Y[j,k])
	 	if a>0.5: 
	 		print (j,k),a
	 		faces_station[j] = k
	z=0
	for (k,s) in Z:
		z+=model.getVal(Z[k,s])
		#if z>0: print (k,s),z
	print('Sum Z: %s' % z)
	return model,faces_station


def solveX(AGVs={},costAGV2Rack={},Faces={},ALPHA={}):
	#ALPHA
	ALPHA1 = ALPHA[1]
	ALPHA2 = ALPHA[2]
	ALPHA3 = ALPHA[3]
	model = Model('Tri-partite-X')
	X={}
	for (i,j) in crossproduct(AGVs,Faces):
		X[i,j] = model.addVar(vtype = 'B',name = 'To assign %s to %s - %s' %(i,j[0],j[1]))
	#Adding Constraints (1) An AGV can be assigned to at most 1 Face
	for i in AGVs:
		model.addCons(quicksum(X[i,j] for j in Faces)<=1,name='Cons (1) for %s' % i)
	#Adding Constraints (3) A Face can be assigned to at most 1 AGV
	for j in Faces:
		model.addCons(quicksum(X[i,j] for i in AGVs)==1, name = 'Cons (3) for %s-%s' % (j[0],j[1]))
	model.setObjective(
		ALPHA1 * quicksum(X[i,j] for (i,j) in X) + 
		ALPHA2 * quicksum(X[i,j]*costAGV2Rack[i,j[0]] for (i,j) in X) ,"minimize")
	model.data =  X
	model.optimize()
	faces_agv = {}
	for (i,j) in X:
		a=model.getVal(X[i,j])
		if a>0.5: 
			print (i,j),a
			faces_agv[j] = i
	return model,faces_agv



def solve(AGVs={},Racks={},costAGV2Rack={},Stations={}, costRack2Station={},
	SKUs={}, demandStation2SKU={},inventory={},Faces={},ALPHA={},AGV_qty=0,SLOT_WAITING={},rackHasAGV={}):
    
	#ALPHA
	#ALPHA1 = ALPHA[1]
	#ALPHA2 = ALPHA[2]
	#ALPHA3 = ALPHA[3]

	#print('Making Model...')
	#Initialte SCIP Model instance
	#model = Model('Tri-partite')

	#solve first stage
	model_solveYZ,face_station_chosen = solveYZ(Racks=Racks,Stations=Stations,costRack2Station=costRack2Station,
		SKUs=SKUs, demandStation2SKU=demandStation2SKU,inventory=inventory,Faces=Faces,ALPHA=ALPHA,AGV_qty=len(AGVs),SLOT_WAITING = SLOT_WAITING,rackHasAGV = rackHasAGV)
	
	#solve second stage
	stage2_faces = []
	for f in face_station_chosen:
		if not rackHasAGV[f[0]]:
			stage2_faces.append(f)

	model_solveX,face_agv_chosen =  solveX(AGVs=AGVs,costAGV2Rack=costAGV2Rack,Faces=stage2_faces,ALPHA=ALPHA)
	
	obj = model_solveX.getSolObjVal(model_solveX.getBestSol()) + model_solveYZ.getSolObjVal(model_solveYZ.getBestSol())
	solving_time = model_solveX.getSolvingTime() + model_solveYZ.getSolvingTime()	
	print("Two-Stage Solution: %s (in %s seconds)" % (obj,solving_time))
	#return [model_solveYZ,model_solveX]
	result = {}
	for f in face_station_chosen:
		result[f] = {}
		result[f]['station'] = face_station_chosen[f]
		if f in stage2_faces:
			result[f]['agv'] = face_agv_chosen[f]
		else:
			result[f]['agv'] = -1
		#each face has chosen a station, need to deterimine sku-qty taken for each face-station
		result[f]['skus_taken'] = {}
		for sku in demandStation2SKU[result[f]['station']]:
			# the chosen station need one SKU and the inventory of this rack has it 
			if demandStation2SKU[result[f]['station']][sku] > 0 and inventory[(f[0],f[1],sku)] > 0:
				#determine qty, record in result, update demand
				qty = min(demandStation2SKU[result[f]['station']][sku],inventory[(f[0],f[1],sku)])
				result[f]['skus_taken'][sku] = qty
				demandStation2SKU[result[f]['station']][sku] -= qty
	return result