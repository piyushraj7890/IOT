import json
import pandas as pd
import boto3
import time
import datetime
from boto3.dynamodb.conditions import Key, Attr
from Database import DynamoDb
from decimal import Decimal


class Anamoly:
    def __init__(self,device_id,table_name) :
        self._device_id = device_id
        self._database = DynamoDb('dynamodb',table_name)

    def Anamoly_detected(self):
        response = self._database.table_intialization.query(KeyConditionExpression=Key('deviceid').eq(self._device_id))
        dataframe = pd.DataFrame(data=response['Items'])
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('Anamoly_table')
        json_data = dataframe.to_json(orient='records')
        data = json.loads(json_data)
        counter1 = 0
        counter2 = 0
        for i in data:
            if i['min'] == None or i['max'] == None:
                pass
            else:
                if i['min']<=55 or i['max']>=105:
                    print(f'Rule 1 breached at {i["timestamp"]} with breach type {"min" if i["min"]<=55 else "max"}')
                    counter1+=1
                if i['min']<=60 or i['max']>=110:
                    print(f'Rule 2 breached at {i["timestamp"] } with breach type {"min" if i["min"]<=60 else "max"}')
                    counter2 +=1
                if counter1>3 or counter2>3:
                    counter1=0
                    counter2=0
                    with table.batch_writer() as batch:
                        batch.put_item (
                            Item={
                                'deviceid':i['deviceid'],
                                'datatype':i['datatype'],
                                'timestamp':i['timestamp'],
                                'min':Decimal(str(i['min'])),
                                'max':Decimal(str(i['max'])),
                                'mean':Decimal(str(i['mean']))

                            }
                        )
                    
                    




