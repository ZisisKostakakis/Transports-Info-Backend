import os
USER_DIRECTORY = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir))
DATA_DIRECTORY = f'{USER_DIRECTORY}/generate_data/'
FLIGHTS = 'flights'
BUS = 'bus'
TRAIN = 'train'
transportation_type_list = [FLIGHTS, BUS, TRAIN]
