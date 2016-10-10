
DataChain
===

**DataChain** is a full big data chain involving online data collection, real time & offline computation, data output

It provides:

*  采集数据源多样化，如巡天望远镜，天体运行，高能物理，爬虫，数据库，分布式文件系统，系统日志等）.
*  数据解析多样化，包括JSON，CSV，RE等.
*  计算多样化，包括实时计算和离线计算.
*  存储多样化，包括关系型（Impala、MySQL），半结构化（MongoDB、Memcached、HBase），非结构化（HDFS），文档数据库（Solr）
*  脚本支持java+scala

Data Flow
===
![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/dataFlow.png)

Data Source
===

支持多种数据源采集：
*  系统日志
*  应用日志
*  数据库数据
*  分布式文件系统Ceph
*  网络爬虫
*  巡天望远镜

![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/dataSource.png)

Data Transform
===

支持多种数据解析方式：json、csv、RE等

![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/dataTransform.png)

Data Store
===

支持多种数据存储：关系型（Impala、MySQL），半结构化（MongoDB、Memcached、HBase），非结构化（HDFS），文档数据库（Solr）

![](https://github.com/cas-bigdatalab/DataChain/blob/master/doc/dataStore.png)


