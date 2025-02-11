import requests
import json
import datetime
from airflow.models import Variable
import os

#load_dotenv('/.env')
#OPENWEATHER_APIKEY = os.environ.get('OPENWEATHER_APIKEY')
#URL = f'https://api.openweathermap.org/data/2.5/weather?q=Santiago,br&units=metric&APPID={OPENWEATHER_APIKEY}'

def call_weather_api():
    OPENWEATHER_APIKEY = Variable.get('OPENWEATHER_API_KEY')
    URL = f'https://api.openweathermap.org/data/2.5/weather?q=Santiago,br&units=metric&APPID={OPENWEATHER_APIKEY}'
    result = requests.get(URL)
    #raise Exception(f'{OPENWEATHER_APIKEY}')
    if result.status_code == 200:
        return result.json()
    else: raise Exception(f'{result.status_code}')


def save_call_result():
    resultado_json = call_weather_api()
    utf = -3.0
    timezone_info = datetime.timezone(datetime.timedelta(hours=utf))
    new_row = f"{resultado_json['coord']['lat']};{resultado_json['coord']['lon']};{resultado_json['weather'][0]['main']};{resultado_json['weather'][0]['description']};{resultado_json['base']};{resultado_json['main']['temp']};{resultado_json['main']['feels_like']};{resultado_json['main']['temp_min']};{resultado_json['main']['temp_max']};{resultado_json['main']['pressure']};{resultado_json['main']['humidity']};{resultado_json['main']['sea_level']};{resultado_json['main']['grnd_level']};{resultado_json['visibility']};{resultado_json['wind']['speed']};{resultado_json['wind']['deg']};{resultado_json['wind']['gust']};{resultado_json['clouds']['all']};{resultado_json['dt']};{resultado_json['sys']['country']};{resultado_json['sys']['sunrise']};{resultado_json['sys']['sunset']};{resultado_json['timezone']};{resultado_json['id']};{resultado_json['name']};{resultado_json['cod']};{datetime.datetime.now(timezone_info).strftime('%d/%m/%y %H:%M:%S')}"
    with open('./data_results/results.csv','a') as f:
        f.write(new_row)
        f.write('\n')