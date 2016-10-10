**DataChain** 是大数据管理流水线，数据采集，管理，分析于一体的通用数据分析平台。将上层应用从数据采集，管理，分析中解放出来！

它具有如下特性:

*  采集数据源多样化，包括巡天望远镜，天体运行，高能物理，爬虫，数据库，分布式文件系统，系统日志等.
*  数据解析多样化，包括JSON，CSV，RE等.
*  计算多样化，包括实时计算、离线批量计算、增量存储.
*  存储多样化，包括关系型，半结构化，非结构化，文档数据库
*  编程模型为SQL，同时支持java，scala

![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/dataDesc.png)

Data Flow
=====================================
**DataChain**整个数据流经过四个步骤：
* 源数据通过采集模块进行采集
* 对数据进行转换&计算
  * 实时计算
  * 增量存储
  * 批量计算
* 对数据进行存储
* 可视化展示
![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/dataFlow.png)

Data Source
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

Data Store
---------------------------------

支持多种数据存储：关系型（Impala、MySQL），半结构化（MongoDB、Memcached、HBase），非结构化（HDFS），文档数据库（Solr）

![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/dataStore.png)


Usage
=====================================
