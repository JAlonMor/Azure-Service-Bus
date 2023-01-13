# Azure Service Bus
# Receive messages from subscriptions to the topic
# 01-12-2023

from azure.servicebus.aio import ServiceBusClient
import json
import pandas as pd
import os
import asyncio
import time
import re
from datetime import datetime

CONNECTION_STR = "YOUR_CONNECTION_STR"
TOPIC_NAME = "YOUR_TOPIC_NAME"
SUBSCRIPTION_NAME = "YOUR_SUBSCRIPTION_NAME"

async def main():

    path = os.getcwd()
    

    now = datetime.now()
    dt_string = now.strftime("%Y_%m_%d_%H_%M_%S")
    f_csv = str(path)+"/"+str(dt_string)+'_C3InterLocks.csv' # output csv file

    # we can also use a blobStorage path to export the CSV file (to do)
 
      
    while (True):

        servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR)

        async with servicebus_client:
            receiver = servicebus_client.get_subscription_receiver(
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME
            )
            async with receiver:
                received_msgs = await receiver.receive_messages(max_message_count=5, max_wait_time=1)
                print(len(received_msgs))

                if  len(received_msgs)==0:
                    print("There is nothing")
                    
                else:
                       
                    for msg in received_msgs:

                        # Create json from topic received
                        
                        js = str(msg)                
                        msgJSON = json.loads(js)

                        # Checking the data of interest     

                        if "c3-scplus-alarms" in msgJSON['subject']:


                            PCSN = re.findall(r"c3-scplus-alarms\/([A-Z]{0,3}[0-9]{0,6})",msgJSON['subject'])
                            PCSN = PCSN[0]

                            Date = re.findall(r"([0-9]+)(?=[.xml])",msgJSON['subject'])
                            Date = Date[0]
                            Date = Date[0:4]+str("-")+Date[4:6]+str("-")+Date[6:8]+str(" ")+Date[8:10]+str(":")+Date[10:12]+str(":")+Date[12:14]
                            Date = pd.to_datetime(Date, format='%Y-%m-%d %H:%M:%S' )

                            ILtrigger = msgJSON['subject'][msgJSON['subject'].find('(')+1:msgJSON['subject'].find(')')]

                                
                            # Avoiding file names with errors
                            if len(ILtrigger) >= 5:
                                break

                            print(Date)
                            print(PCSN)
                            print(ILtrigger)

                            # Create dataframe and export to csv file
                            df = pd.DataFrame(data=[[Date,PCSN,ILtrigger]])                                                
                            df.to_csv(f_csv, header=False, index=False, mode='a',sep=',')
                                
                            time.sleep(1)
                                
                                
                                
                                
                            print("return")
                                                 

                        await receiver.complete_message(msg)

asyncio.run(main())
