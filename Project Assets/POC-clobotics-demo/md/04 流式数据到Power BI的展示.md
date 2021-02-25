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

databricks secrets create-scope --scope scope名称 --initial-manage-principal users

**例如：**

databricks secrets create-scope --scope powerbi --initial-manage-principal users

**语法：**

databricks secrets put --scope scope名称 --key key名称

**例如：**

databricks secrets put --scope powerbi --key skusensorstreamingapi

最后输入推送Url保存退出

## 调用

导入PysparkPowerBIStreaming.PowerBIStreaming的whl包：

![11](https://i.loli.net/2021/02/22/8lvbdAyKtsxRYZa.png)

## 在python notebook中引入包

from PysparkPowerBIStreaming.PowerBIStreaming import PowerBIStreaming

…

创建与powerbi流数据集一样格式的pyspark.sql.dataframe.DataFrame对象

…

**实例化powerbi对象：**

powerbi = PowerBIStreaming(dbutils, "scope名称", "key名称")

**例如：**

powerbi = PowerBIStreaming(dbutils, "powerbi", "skusensorstreamingapi")

**运行：**

powerbi.sendBatch(DataFrame对象)

**例如：**

powerbi.sendBatch(df) 

## 关键代码

![12](https://i.loli.net/2021/02/22/oeOTc8zHrSR9jkX.png)

![13](https://i.loli.net/2021/02/22/VoXUefYHkMJO8Nv.png)

**注意：此处需要注意的是对于2.3.15及更高版本，需要在配置中加密连接字符串：**

```
conf['eventhubs.connectionString'] =  sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
```
## 参考资料

https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md#event-hubs-configuration
