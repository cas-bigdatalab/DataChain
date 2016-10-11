**DataChain** 是大数据管理流水线，集数据采集，管理，分析于一体的通用数据分析平台。将上层应用从数据采集，管理，分析中解放出来！解决海量日志采集系统、实时推荐系统的共性问题。

它具有如下特性:

*  采集数据源多样化，包括巡天望远镜，天体运行，高能物理，爬虫，数据库，分布式文件系统，系统日志等.
*  数据解析多样化，包括JSON，CSV，RE等.
*  计算多样化，包括实时计算、离线批量计算、增量存储.
*  存储多样化，包括关系型，半结构化，非结构化，文档数据库
*  编程模型为SQL，同时支持MLlib算法库，用户自定义java，scala程序
*  Chain的各个环节高度可配置

![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/dataDesc.png)

Data Flow
=====================================
**DataChain**整个数据流经过四个步骤：
* **DataCollect**：源数据通过采集模块进行采集
* **DataTransform**：对数据格式进行分析、转换
* **DataCompute**：对数据进行计算
  * 实时计算
  * 增量存储
  * 批量计算
* **DataStore**：对数据进行存储

![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/dataFlow.png)

Data Collect
-------------------------------

支持多种数据源采集：
*  系统日志
*  应用日志
*  数据库数据
*  分布式文件系统Ceph
*  网络爬虫
*  巡天望远镜

![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/dataSource.png)

Data Transform
---------------------------------

支持多种数据解析方式：json、csv、RE等

![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/dataTransform.png)

Data Compute
---------------------------------
支持多种计算模型
* SQL
* MLlib
* Scala
* Java


Data Store
---------------------------------

支持多种数据存储：
* 关系型（Impala、MySQL）
* 半结构化（MongoDB、Memcached、HBase）
* 非结构化（HDFS）
* 文档数据库（Solr）

![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/dataStore.png)


Usage Example
=====================================
实时任务定义：采集csv格式的数据文件，存入到mysql数据库。

* 第一步：定义表

 * 表streaming_test，驱动定义为streaming，意为从采集端采集的数据映射成的表,table为消息队列topic
    ![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/streaming_table.png)
 * 表mysql_user，对应mysql中数据库spark下的user表
    ![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/mysql_table.png)
    
* 第二步：定义采集信息
  采用flume的定义规范，需确定source，channel，sink。该配置定义采集10.0.71.20上路径为/opt/flumeSpooleDir文件夹下的文件，并发到kafka
  ![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/agent.png)
  
* 第三步：定义Task
  需要定义Task名称，类型（realtime，offline，external），原表srcTable，转换mappingSpec（transformer），目标表destTable及对应表的操作SQL
  ![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/realtime_task.png)

