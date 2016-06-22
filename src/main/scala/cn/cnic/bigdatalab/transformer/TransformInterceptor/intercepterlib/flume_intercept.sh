INTERPRETOR_HOME=/root/intercepterlib
FLUME=/opt/apache-flume-1.6.0-bin
#echo ${INTERPRETOR_HOME}/parserlib

for jar in ${INTERPRETOR_HOME}/parserlib/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

CLASSPATH=$CLASSPATH:${INTERPRETOR_HOME}/myinterceptor.jar

echo $CLASSPATH

${FLUME}/bin/flume-ng agent -c ${FLUME}/conf -f ${FLUME}/conf/test1.conf -n agent -Dflume.root.logger=DEBUG,console --classpath $CLASSPATH 
