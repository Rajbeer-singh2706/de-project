import os 
from confluent_kafka import SerializingProducer
import simplejson as json 
from datetime import datetime, timedelta
import random
import uuid 


LONDON_COORDIATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDIATES = {"latitude": 52.4862, "longitude": -1.8904}

# Calculate movement increment
LATITUDE_INCREMENT = (BIRMINGHAM_COORDIATES['latitude'] - LONDON_COORDIATES['latitude']) / 100 
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDIATES['longitude'] - LONDON_COORDIATES['longitude']) / 100 

# EVriomenmt variabel for configration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SEERVERS', 'localhost:9092')
VECHILE_TOPIC = os.getenv('VECHILE_TOPIC','vechile_data')
GPS_TOPIC = os.getenv('GPS_TOPIC','gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC','traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC','weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC','emergency_data')

start_time = datetime.now()
start_location = LONDON_COORDIATES.copy()

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30,60)) # update freqquncey
    return start_time

def simulate_vechile_movement():
    global start_location
    # move towrads birmighgah 
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    ## add some randomness to simulate acutal road tarvel
    start_location['latitude'] +=  random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    return start_location

def generate_vechile_data(device_id):
    location = simulate_vechile_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10,40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid'
    }

def simulate_journery(producer, deivce_id):
    while True:
        vechile_data = generate_vechile_data(deivce_id)
        print(vechile_data)
        break 

if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kfaka error: {err}')
    }
    producer = SerializingProducer(producer_config)
    try:
        simulate_journery(producer, 'Vechile-CodeWithYu-123')
    except KeyboardInterrupt:
        print('Simulation Edndee by the user')
    except Exception as e:
       print(f'Unexepcted Error occured: {e}')