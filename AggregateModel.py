import json
from decimal import Decimal
from turtle import pu
import boto3
import time
import datetime
from boto3.dynamodb.conditions import Key, Attr

import pandas as pd

from dynamo_pandas import put_df,get_df,keys

from Database import DynamoDb


class Aggregation:
    def __init__(self,deviceid):
        self._dynamodb = DynamoDb('dynamodb','BSM_table')
        self._deviceid = deviceid
    def aggregate(self):
        response = self._dynamodb.table_intialization.query(KeyConditionExpression=Key('deviceid').eq(self._deviceid))
        df = pd.DataFrame(data=response['Items'])
        grouped = df.groupby('datatype')
        
        
        # Getting the grouped data
        heart_groped_data = grouped.get_group('HeartRate')
        spo2_grouped_data = grouped.get_group('SPO2')
        temperature_grouped_data = grouped.get_group('Temperature')
       
        # # resampling of heart data
        heart_groped_data.timestamp = pd.to_datetime(heart_groped_data.timestamp)
        heart_groped_data.set_index('timestamp',inplace=True)
        resampled_heartrate = heart_groped_data.resample('T').agg(['min','max','mean'])
        heartrate_data_frame = pd.DataFrame(data=resampled_heartrate['value'])
        heartrate_data_frame['deviceid'] = self._deviceid
        heartrate_data_frame['datatype'] = 'HeartRate'
        heartrate_data_frame['timestamp'] = heartrate_data_frame.index.strftime('%m-%d-%Y %H:%M:%S')
        heartrate_data_frame.reset_index(drop=True,inplace=True)
        
       
       

        # #Resampling of spo2 data
        spo2_grouped_data.timestamp = pd.to_datetime(spo2_grouped_data.timestamp)
        spo2_grouped_data.set_index('timestamp',inplace=True)
        resampled_spo2 = spo2_grouped_data.resample('T').agg(['min','max','mean'])
        spo2_dataframe = pd.DataFrame(data=resampled_spo2['value'])
        spo2_dataframe['deviceid'] = self._deviceid
        spo2_dataframe['datatype'] = 'SPO2'
        spo2_dataframe['timestamp'] = spo2_dataframe.index.strftime('%m-%d-%Y %H:%M:%S')
        spo2_dataframe.reset_index(drop=True,inplace=True)
        
       

        # #Resampling of temperature data
        temperature_grouped_data['timestamp'] = pd.to_datetime(temperature_grouped_data.timestamp)
        temperature_grouped_data .set_index('timestamp',inplace=True)
        resampled_temperature = temperature_grouped_data .resample('T').agg(['min','max','mean'])
        temp_dataframe = pd.DataFrame(data=resampled_temperature['value'])
        temp_dataframe['deviceid'] = self._deviceid
        temp_dataframe['datatype'] = 'Temperature'
        temp_dataframe['timestamp'] = temp_dataframe.index.strftime('%m-%d-%Y %H:%M:%S')
        temp_dataframe.reset_index(drop=True,inplace=True)
        
        

        # # Feeding data into corresponding table
        put_df(heartrate_data_frame,table='heart_table')
        put_df(spo2_dataframe,table='spo2_table')
        put_df(temp_dataframe,table='temperature_table')

        




