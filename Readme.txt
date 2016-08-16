1.zip -d /opt/datachain.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF

2.切换环境,记得修改hive-site.xml
javax.jdo.option.ConnectionUR
hive.metastore.uris

3.Collection Step
需要建立/opt/flume.out
同时监听的目录要存在,真对spoolDir

4.新建solr的collection-->financenews后，需要执行如下语句，用户在建索引的过程中能够查询到数据
curl -X POST http://localhost:8983/solr/financenews/config -d '{"set-property":{"updateHandler.autoSoftCommit.maxTime":"2000"}}'

5.支持Java、Scala任务
   
   1.提供SDK，现有如下两种方法，用户通过实现以下方法即可自定义数据的处理逻辑。

     process(schema: String, line: String)
     process(schema: String，rdd: RDD[String])

   2.提供通用数据处理逻辑

6. hive支持partition

   在集群环境hive-site.xml中增加如下配置：
   <property>
     <name>hive.exec.dynamic.partition</name>
     <value>true</value>
     <description>Whether or not to allow dynamic partitions in DML/DDL.</description>
   </property>
   <property>
     <name>hive.exec.dynamic.partition.mode</name>
     <value>nostrict</value>
     <description>
       In strict mode, the user must specify at least one static partition
       in case the user accidentally overwrites all partitions.
       In nonstrict mode all partitions are allowed to be dynamic.
     </description>
   </property>
   <property>
     <name>hive.enforce.bucketing</name>
     <value>true</value>
     <description>Whether bucketing is enforced. If true, while inserting into the table, bucketing is enforced.</description>
   </property> 