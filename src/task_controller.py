from flask import Flask
from flask import request
import readfile
import entities
import pandas as pd


ScheduleApp = Flask(__name__)
PORT = 1111

@ScheduleApp.route('/test', methods=['GET', 'POST'])
def test():
    res = request.get_data()
    return res

@ScheduleApp.route('/init', methods=['GET', 'POST'])
def init():
    input_folder_path = '../input/'
    sku_path = input_folder_path + 'sku_info.csv'
    stock_path = input_folder_path + 'point.csv'
    rack_path = input_folder_path + 'container_stock.csv'
    order_path = input_folder_path + 'outbound_bills.csv'
    slot_path = input_folder_path + 'station_slot_setup.csv'
    _SKUS = readfile.getSKUS(sku_path)
    _RACKS = readfile.getRACKS(rack_path)
    _STOCKS = readfile.getSTOCKS(stock_path)
    _ORDERS = readfile.getOUTBOUND_BILLS(order_path)
    _STATIONS = readfile.getSTATIONS_SLOTS(slot_path)
    # init stations
    STATIONS = {}
    for s in _STATIONS:
        STATIONS[_STATIONS[s]['ID']] = entities.Station(_STATIONS[s])
    print(STATIONS)
    # init racks
    entities.Rack.SKUS = _SKUS
    RACKS = {}
    for r in _RACKS:
        RACKS[_RACKS[r]['ID']] = entities.Rack(_RACKS[r])
    for s in _STOCKS:
        if _STOCKS[s]['rack_id'] > 0:
            RACKS[_STOCKS[s]['rack_id']].position = (_STOCKS[s]['x'], _STOCKS[s]['y'])
    print(RACKS[40])

    global webWarehouse
    entities.Warehouse.Racks = RACKS
    entities.Warehouse.Stations = STATIONS
    webWarehouse = entities.Warehouse(_ORDERS)

    return 'initialization finished!'

@ScheduleApp.route('/zudan',methods=['GET','POST'])
def zudan():
    global webWarehouse
    request_body = request.get_data().decode()
    print(request_body)
    # request_body in format: 'currentTime,currentJiedanTime,nearTime'
    data = request_body.split(',')
    current = data[0]
    current_jiedan = data[1]
    near = int(data[2])
    result = webWarehouse.zu(currentTime=current, currentJiedan=current_jiedan, nearTime=near)
    return_string = []
    for sta_slot in result:
        return_string.append('%s,%s,%s' %(str(sta_slot[0]),str(sta_slot[1]),'%'.join(result[sta_slot])))
	#print(return_string
    return ':'.join(return_string)

if __name__ == '__main__':
    print('initialize...')
    init()
    ScheduleApp.run(host='127.0.0.1', port=PORT, debug=False, threaded=True)

