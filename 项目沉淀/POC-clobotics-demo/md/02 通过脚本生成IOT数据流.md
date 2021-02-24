# 2 通过脚本生成IoT数据流

我们可以通过脚本来实现IoT设备实时传输流数据，进行后续分析。

```python
import random
import time, datetime
from azure.iot.device import IoTHubDeviceClient, Message

CONNECTION_STRING = "Your Connection String"

//为了方便展示，这里的JSON结构并不复杂
MSG_TXT = '{{"storeid": "{storeid}","SKU": "{sku}","timestamp":"{timestamp}"}}'

def iothub_client_init():
    # Create an IoT Hub client
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    return client

def calcuNumber(number):
  if number <= 5:
    number = 20
  else:
    number = number - random.randint(1,5)
  return number

def iothub_client_telemetry_sample_run():

    try:
        client = iothub_client_init()
        number = 20
        print ( "IoT Hub device sending periodic messages, press Ctrl-C to exit" )

        while True:
            # Build the message with simulated telemetry values.
            dt = datetime.datetime.now()
            timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
            storeid = "store123"
            number = calcuNumber(number)
            #sku_facing_list = SKU_FORMATE.format(number=number)
            
            msg_txt_formatted = MSG_TXT.format(storeid=storeid, sku=number, timestamp=timestamp)
            message = Message(msg_txt_formatted)

            # Send the message.
            print( "Sending message: {}".format(message) )
            client.send_message(message)
            print ( "Message successfully sent" )
            time.sleep(10)

    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )


if __name__ == '__main__':
    print ( "IoT Hub Quickstart #1 - Simulated device" )
    print ( "Press Ctrl-C to exit" )
    iothub_client_telemetry_sample_run()

```