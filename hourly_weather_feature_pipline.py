import modal
import pandas as pd

LOCAL = False


if LOCAL==False:
    stub = modal.Stub("collect_weather_data")
    image = modal.Image.debian_slim(python_version='3.9').pip_install(['hopsworks', 'requests'])

    @stub.function(image=image, schedule=modal.Period(hours=1), secret=modal.Secret.from_name("HOPSWORKS_API_KEY"))
    def f():
        g()

def g():
    import hopsworks
    import requests
    import json
    from datetime import datetime

    '''Collect weather data'''
    # Weather url (updates per minute)
    longitude = 18.07302
    latitude = 59.34881
    url = f'https://opendata-download-metanalys.smhi.se/api/category/mesan1g/version/2/geotype/point/lon/{longitude}/lat/{latitude}/data.json'

    # Send HTTP Get request
    response = requests.get(url)

    # Check the status code of the request
    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Error: {response.status_code}")
        print(response.text)


    '''Save the weather data to hopsworks'''
    # log in to hopsworks
    project = hopsworks.login()
    fs = project.get_feature_store()

    # Extract data from response
    valid_time = datetime.strptime(data["timeSeries"][0]['validTime'], "%Y-%m-%dT%H:%M:%SZ")
    day = valid_time.day
    hour = valid_time.hour

    for parameter in data["timeSeries"][0]["parameters"]:
        if parameter['name'] == 't':
            temp = parameter["values"] # Air temperature
        if parameter['name'] == 'wd':
            wd = parameter["values"] # Wind direction
        if parameter['name'] == 'ws':
            ws = parameter["values"] # Wind Speed
        if parameter['name'] == 'prec1h':
            prec1h = parameter["values"] # Precipation last hour
        if parameter['name'] == 'frsn1h':
            frsn1h = parameter["values"] # Snow precipation last hour
        if parameter['name'] == 'vis':
            vis = parameter["values"] # Horizontal visibility
    data_df = pd.DataFrame({
        'day': day,
        'hour': hour,
        'temp': temp,
        'wd': wd,
        'ws': ws,
        'prec1h': prec1h,
        'frsn1h': frsn1h,
        'vis': vis
    })
    print(data_df)

    # push data to hopsworks
    keys = data_df.keys()
    try:
        fg = fs.get_feature_group(
            name='hourly_weather_features',
            version=1
        )
    except:
        fg = fs.create_feature_group(
            name='hourly_weather_features',
            version=1,
            primary_key=keys,
            description='Hourly weather features'
        )
    fg.insert(data_df)

    # log out
    hopsworks.logout()




if __name__=='__main__':
    if LOCAL==True:
        g()
    else:
        modal.runner.deploy_stub(stub)