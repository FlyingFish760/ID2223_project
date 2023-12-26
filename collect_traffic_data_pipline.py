import modal
import pandas as pd

tomtom_api_key = '95GWSfDPOA0Nr9v0To26HEsAucy2yiR7'
LOCAL = False


if LOCAL==False:
    stub = modal.Stub("collect_traffic_data")
    image = modal.Image.debian_slim(python_version='3.9').pip_install(['hopsworks', 'requests'])

    @stub.function(image=image, schedule=modal.Period(minutes=1), secret=modal.Secret.from_name("HOPSWORKS_API_KEY"))
    def f():
        g()

def g():
    import hopsworks
    import requests
    import json
    from datetime import datetime

    '''Collect data'''
    # Traffic flow url (updates per minute)
    baseURL = 'api.tomtom.com'
    versionNumber = 4
    style = 'relative0'
    zoom = 10
    format = 'json'
    point = '59.34881%2C18.07302'
    unit = 'KMPH'
    boolean = 'false'

    traffic_flow_url = f'https://{baseURL}/traffic/services/{versionNumber}/flowSegmentData/{style}/{zoom}/{format}?key={tomtom_api_key}&point={point}&unit={unit}&openLr={boolean}'

    # Send HTTP Get request
    traffic_flow_response = requests.get(traffic_flow_url)

    # Check the status code of the request
    if traffic_flow_response.status_code == 200:
        traffic_flow_data = traffic_flow_response.json()
    else:
        print(f"Error: {traffic_flow_response.status_code}")
        print(traffic_flow_response.text)


    '''Save the data to hopsworks'''
    # log in to hopsworks
    project = hopsworks.login()
    fs = project.get_feature_store()

    # Change data format (to dataframe)
    traffic_flow_data = traffic_flow_data['flowSegmentData']
    del traffic_flow_data['@version']
    del traffic_flow_data['coordinates']
    traffic_flow_data['confidence'] = float(traffic_flow_data['confidence'])
    index = [0]
    traffic_flow_data_df = pd.DataFrame(traffic_flow_data, index=index)

    # Trim and modify data
    new_key_names = {'currentSpeed': 'current_speed', 
                     'freeFlowSpeed': 'free_flow_speed'}  # hopsworks can only use lowercase feature names
    traffic_flow_data_df=traffic_flow_data_df.rename(columns=new_key_names)
    traffic_flow_data_df=traffic_flow_data_df.drop(columns=['frc', 'currentTravelTime', 'freeFlowTravelTime', 'roadClosure'])

    current_datetime = datetime.now()
    print("hour:", current_datetime.hour)
    traffic_flow_data_df['weekend'] = [True] if current_datetime.weekday()>=5 else [False]
    traffic_flow_data_df['day'] = [current_datetime.day]
    traffic_flow_data_df['hour'] = [current_datetime.hour]
    traffic_flow_data_df['minute'] = [current_datetime.minute]
    print(traffic_flow_data_df)

    # push data to hopsworks
    keys = ['weekend', 'day', 'hour', 'minute']
    fg = fs.get_or_create_feature_group(
        name='traffic_flow_data',
        version=1,
        primary_key=keys,
        description='Traffic flow data'
    )
    fg.insert(traffic_flow_data_df)


if __name__=='__main__':
    if LOCAL==True:
        g()
    else:
        modal.runner.deploy_stub(stub)