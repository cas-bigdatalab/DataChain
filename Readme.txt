1.zip -d /Project/DataChain/classes/artifacts/datachain_jar/datachain.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF

2.切换环境,记得修改hive-site.xml
javax.jdo.option.ConnectionUR
hive.metastore.uris

3.Collection Step
需要建立/opt/flume.out
同时监听的目录要存在,真对spoolDir