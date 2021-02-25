# 1 案例背景

## 客户介绍

客户根据传感器捕捉冰柜信息，关门触发，统计冰柜中物品数量进行分析。

## 测试案例

### Use case 1: 离线计算，对现有IOT设备进行数量汇总

每日凌晨统计客户IOT设备数量，可拓展需求至设备运行状态是否良好，投入设备是否在管理地区使用等等

![01](https://i.loli.net/2021/02/22/xwO9eYiA3TgpUq7.png)

通过Azure Data Factory进行批数据拉取，同步至Data Lake Gen 2进行数据存储与建模。Databricks进行ETL或数仓分层，将清洗后数据集市数据同步至Azure SQL进行Power BI数据展示。

![02](https://i.loli.net/2021/02/22/Ev2srDi9l85odpT.png)

### Use case 2: 实时计算，10分钟更新一次销售数据

实时接收IOT设备数据，以十分钟为一时间窗口进行统计，并及时展现在Power BI中：

![03](https://i.loli.net/2021/02/22/XbqvTAdy6ezPHsD.png)

通过Azure IoT Hub进行流数据收集，推送至Databricks进行流分析，并及时展示到Power BI。在流数据处理架构中也可以进行数据落盘保证数据存储与可追溯性。

![04](https://i.loli.net/2021/02/22/Ea9x6DPifVk8pn3.png)

根据实时、离线分析样例对Databricks进行综合考察，完成产品评估与性能判断。

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

# 3 流式数据传输到 Databricks Delta Lake

在大数据背景下，物联网设备数据算是企业数据资产中占据大量比例。对这些大量数据进行管理可以说式构建成功IOT平台的第一公里。
物联网数据平台可以在内部部署或在云上部署，作为云计算从业者，我更加喜欢基于云的解决方案，特别是PaaS产品。在本文中，我将展示如何使用Azure构建简易的流式数据处理链。

![05](https://i.loli.net/2021/02/22/ru28IyPHvFx5iNW.png)

## Databricks：统一分析平台和Delta Lake

Databricks提供了一个统一的数据工程、数据科学和业务逻辑平台。从根本上将，这是Azure提供的云计算商用Spark PaaS产品，可以帮助我们加快数据探索和准备工作。

## 那什么是Delta Lake呢？

Delta Lake是Databricks开源的一种存储层，可以将ACID事务引入大数据场景作为工作负载。在现有大数据生态存储机制中（例如Parquet），我们存储在HDFS中的文件是不可变的，当我们需要去更新Parquet文件中的记录时，只能通过重写整个文件解决。但是如果我们使用Delta Lake进行存储，我们就可以轻松地编写更新语句。
在IOT和实时计算场景下，Delta Lake作为重点的原因是：我们能够在数据到达时，对流数据进行查询，不用再等待数据分区更新（HDFS重写场景）。
在这个解决方案中，我们将看到如何对Databricks进行设置，并如何使用Spark Streaming收集Azure IOTHub的记录，将它们写入Delta表中。

## 设置Databricks

我们通过Azure门户单击“创建资源”->”分析“->”Azure Databricks“

![06](https://i.loli.net/2021/02/22/rKpuYL6McvTOgih.png)

## 参考资料

https://docs.microsoft.com/zh-cn/azure/databricks/scenarios/quickstart-create-databricks-workspace-portal?tabs=azure-portal#create-an-azure-databricks-workspace

## 收集IoT Hub的数据流

按照方案，我们需要在集群中安装“azure-eventhubs-spark_2.11:2.3.6”Maven库。我们只需要单击“Workspace”->“单击你的用户名”->“创建”->“Library”

![07](https://i.loli.net/2021/02/22/CpoWzbEJAFZisaQ.png)

选择Maven页签，在Coordinates框中输入包名，单击“Create”

提示：安装完成后，应重新启动，使其生效。

连接到IoT Hub，输出流：

```python
import org.apache.spark.eventhubs._
import  org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import  org.apache.spark.sql.functions.{ explode, split }

//我们需要指定IOTHub连接字符串与名称
val connectionString = ConnectionStringBuilder("--IOTHub连接字符串--")
  .setEventHubName("--IoTHub 名称--")
  .build
val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)
  
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()
```

上面的代码段可以构建一个指向我们之前创建的IoT Hub的连接字符串，只需要从Azure门户中获取到连接字符串就可以构建链接。

### 要查看传入数据如何运行

```
display(eventhubs)
```

### 提取设备数据并创建一个Spark SQL表

从上一步创建的DataFrame中提取body字段中的数据，并构建一个SQL内存表：

```python
val messages = eventhubs
  .withColumn("Time", $"enqueuedTime")
  .withColumn("Body", $"body".cast(StringType))
  .select("Time","Body")

val messagesDF = messages.select(get_json_object($"Body", "$.storeid").alias("storeid"),
                              get_json_object($"Body", "$.timestamp").alias("timestamp").cast(TimestampType),
                              get_json_object($"Body", "$.SKU").alias("SKU").cast(LongType))
```

将流数据写入至Delta表进行存储，或者可以在别的任务中使用处理好的内存表进行数据分析与调用：

```python
messagesDF.printSchema

messagesDF.createOrReplaceTempView("ods_iothub_data")

messagesDF.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
```

# 4 流式数据到Power BI的展示

## 创建流数据集

![08](https://i.loli.net/2021/02/22/EvWNOuCxjL9aynA.png)

## 配置流数据集的结构

数据结构要和dataframe里的列一致，列名和列类型一致。

![09](https://i.loli.net/2021/02/22/7ixZVLwsQAGaDFB.png)

## 获取流数据集的API

拷贝推送URL。

![10](https://i.loli.net/2021/02/22/K9LVGx5npDQ1JId.png)

## 下载databricks cli

下载链接：[databricks cli](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/cli/)
此处建议用python的pip来安装。

## 配置databricks

运行databricks configure --token，输入adb的host和token进行注册

## 创建加密url

**语法：**

```
databricks secrets create-scope --scope scope名称 --initial-manage-principal users
```

**例如：**

```
databricks secrets create-scope --scope powerbi --initial-manage-principal users
```

**语法：**

```
databricks secrets put --scope scope名称 --key key名称
```

**例如：**

```
databricks secrets put --scope powerbi --key skusensorstreamingapi
```

最后输入推送Url保存退出

## 调用

导入PysparkPowerBIStreaming.PowerBIStreaming的whl包：

![11](https://i.loli.net/2021/02/22/8lvbdAyKtsxRYZa.png)

## 在python notebook中引入包

```
from PysparkPowerBIStreaming.PowerBIStreaming import PowerBIStreaming
```

…

创建与powerbi流数据集一样格式的pyspark.sql.dataframe.DataFrame对象

…

**实例化powerbi对象：**

```
powerbi = PowerBIStreaming(dbutils, "scope名称", "key名称")
```

**例如：**

```
powerbi = PowerBIStreaming(dbutils, "powerbi", "skusensorstreamingapi")
```

**运行：**

```
powerbi.sendBatch(DataFrame对象)
```

**例如：**

```
powerbi.sendBatch(df) 
```



## 关键代码

![12](https://i.loli.net/2021/02/22/oeOTc8zHrSR9jkX.png)

![13](https://i.loli.net/2021/02/22/VoXUefYHkMJO8Nv.png)

**注意：此处需要注意的是对于2.3.15及更高版本，需要在配置中加密连接字符串：**

```
conf['eventhubs.connectionString'] =  sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
```

## 参考资料

https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md#event-hubs-configuration