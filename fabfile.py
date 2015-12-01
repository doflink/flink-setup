#
# (C) Do Le Quoc, 11/2015
#

from fabric.api import *
import fabric.contrib.files
import time
import logging
import os
from fabric.contrib.files import append
import json

#Disable annoyting log messages.
logging.basicConfig(level=logging.ERROR)
#This makes the paramiko logger less verbose
para_log=logging.getLogger('paramiko.transport')
para_log.setLevel(logging.ERROR)
env.keepalive = True


### Input parameters ###
with open('nodes.json') as input_nodes:    
    env.roledefs = json.load(input_nodes)

with open('config.json') as input_parameters:
    parameters = json.load(input_parameters)

master_cluster =  env.roledefs['masters'][0]
env.user= parameters['cluster_user']
print env.user
env.key_filename = parameters['rsa_key']
print env.key_filename
user_home = parameters['user_home']
cluster_home = parameters['cluster_home']
flink_home = cluster_home + 'flink'
scala_home = cluster_home + 'scala'
spark_home = cluster_home + 'spark'

url_hadoop = parameters['url_hadoop']
url_flink = parameters['url_flink']
url_spark = parameters['url_spark']
url_scala = parameters['url_scala']
url_sbt = parameters['url_sbt'] 

flink_master_heap = parameters['flink_master_heap'] #the amount of available memory per TaskManager
flink_slave_heap = parameters['flink_slave_heap'] #the amount of available memory per slaver
flink_slave_slots = parameters['num_cores_per_node'] #the number of available CPUs per node
flink_parallelism_default = len(env.roledefs['slaves'])*int(flink_slave_slots) #Total number of cores in the cluster

num_maps = int(parameters['num_hadoop_maps'])
num_reduces = int(parameters['num_hadoop_reduces'])


#### Install Hadoop Flink Spark ####

#Install requirements
@roles('masters', 'slaves')
def installRequirement():
    sudo('sudo apt-get update')
    run('echo "Y"|sudo apt-get install openjdk-7-jdk')


#Download Hadoop, Flink, Spark
@roles('masters', 'slaves')
def downloads():
    if os.path.isdir(cluster_home):
	run('rm -rf '+ cluster_home + ' && mkdir ' + cluster_home)
    else:
	run('mkdir ' + cluster_home)

    #Hadoop
    run('cd ' + cluster_home + ' && wget ' + url_hadoop)
    tarhadoop = url_hadoop.split('/') [-1]
    hadoop = tarhadoop.split('.tar')[0]
    run('cd ' + cluster_home + ' && tar -xvzf ' + tarhadoop + ' && rm -rf hadoop && mv ' + hadoop + ' hadoop')
    #Flink
    run('cd ' + cluster_home + ' && wget ' + url_flink)
    tarflink = url_flink.split('/') [-1]
    flink = tarflink.split('-bin')[0]
    #flink_home = cluster_home + 'flink'
    run('cd ' + cluster_home + ' && tar -xvzf ' + tarflink + ' && rm -rf flink && mv ' + flink + ' flink')
    #Scala
    run('cd ' + cluster_home + ' && wget ' + url_scala)
    tarscala = url_scala.split('/') [-1]
    scala = tarscala.split('.tgz')[0]
    #scala_home = cluster_home + 'scala'
    run('cd ' + cluster_home + ' && tar -xvzf ' + tarscala + ' && rm -rf scala && mv ' + scala + ' scala')
    #Spark
    run('cd ' + cluster_home + ' && wget ' + url_spark)
    tarspark = url_spark.split('/') [-1]
    spark = tarspark.split('.tgz')[0]
    #spark_home = cluster_home + 'spark'
    run('cd ' + cluster_home + ' && tar -xvzf ' + tarspark + ' && rm -rf spark && mv ' + spark + ' spark')


### Cluster Configuration ###

#Hadoop Configuration
@roles('masters', 'slaves')
def changeMapRedSite(master=master_cluster, num_maps=str(num_maps), num_reduces=str(num_reduces)):
    filename = cluster_home + 'hadoop/etc/hadoop/mapred-site.xml.template'
    before = '<configuration>' #newfile is empty
    after = '<configuration>' + '\\n<property>\\n<name>mapred.job.tracker</name>\\n<value>' + master + ':9001</value>\\n</property>' + \
                                '\\n<property>\\n<name>mapred.map.tasks</name>\\n<value>' + num_maps + '</value>\\n</property>' + \
                                '\\n<property>\\n<name>mapred.reduce.tasks</name>\\n<value>' + num_reduces + '</value>\\n</property>' + \
                                '\\n<property>\\n<name>mapred.system.dir</name>\\n<value>' + cluster_home + 'hadoop/hdfs/mapreduce/system</value>\\n</property>' + \
                                '\\n<property>\\n<name>mapred.local.dir</name>\\n<value>' + cluster_home + 'hadoop/hdfs/mapreduce/local</value>\\n</property>' 
    fabric.contrib.files.sed(filename, before, after, limit='')
    run('cd ' + cluster_home + 'hadoop/etc/hadoop/' + '&& mv mapred-site.xml.template mapred-site.xml')

@roles('masters', 'slaves')
def changeCoreSite(master=master_cluster):
    filename = cluster_home + 'hadoop/etc/hadoop/core-site.xml'
    before = '<configuration>' #newfile is empty
    after = '<configuration>' + '\\n<property>\\n<name>hadoop.tmp.dir</name>\\n<value>' + cluster_home + 'hadoop/hdfs</value>\\n</property>' + \
                                '\\n<property>\\n<name>fs.default.name</name>\\n<value>hdfs://' + master + ':9000</value>\\n</property>' 
    fabric.contrib.files.sed(filename, before, after, limit='')

@roles('masters', 'slaves')
def changeHDFSSite(master=master_cluster, replica='1', xcieversmax='10096'):
    filename = cluster_home + 'hadoop/etc/hadoop/hdfs-site.xml'
    before = '<configuration>' #newfile is empty
    after = '<configuration>' + '\\n<property>\\n<name>dfs.name.dir</name>\\n<value>' + cluster_home + 'hadoop/hdfs/name</value>\\n</property>' + \
                                '\\n<property>\\n<name>dfs.data.dir</name>\\n<value>' + cluster_home + 'hadoop/hdfs/data</value>\\n</property>' + \
                                '\\n<property>\\n<name>dfs.replication</name>\\n<value>' + replica + '</value>\\n</property>' + \
                                '\\n<property>\\n<name>dfs.datanode.max.xcievers</name>\\n<value>' + xcieversmax + '</value>\\n</property>'
    fabric.contrib.files.sed(filename, before, after, limit='')

@roles('masters', 'slaves')
def changeMasters(master=master_cluster):
    filename = cluster_home + 'hadoop/etc/hadoop/masters'
    before = 'localhost'
    after = master
    fabric.contrib.files.sed(filename, before, after, limit='')

@roles('masters', 'slaves')
def changeSlaves():
    filename = cluster_home + 'hadoop/etc/hadoop/slaves'
    before = 'localhost'
    after = ''
    slaves = env.roledefs['slaves']
    for slave in slaves:
        after = after + slave + '\\n' 
    fabric.contrib.files.sed(filename, before, after, limit='')

@roles('slaves')
def changeYarnSiteSlave(master=master_cluster):
    filename = cluster_home + 'hadoop/etc/hadoop/yarn-site.xml'
    before = '<configuration>' #newfile is empty
    after ='<configuration>' + '\\n<property>\\n<name>yarn.resourcemanager.hostname</name>\\n<value>' + master + '</value>\\n' + \
           '<description>The hostname of the ResourceManager</description>\\n' + \
           '</property>\\n' + \
           '<property>\\n<name>yarn.nodemanager.aux-services</name>\\n' + \
           '<value>mapreduce_shuffle</value>\\n</property>\\n'
    fabric.contrib.files.sed(filename, before, after, limit='')
               
@roles('masters')
def changeYarnSiteMaster(master=master_cluster):
    filename = cluster_home + 'hadoop/etc/hadoop/yarn-site.xml'
    before = '<configuration>' #newfile is empty
    after ='<configuration>' + '\\n<property>\\n<name>yarn.resourcemanager.hostname</name>\\n<value>' + master + '</value>\\n' + \
           '<description>The hostname of the ResourceManager</description>\\n' + \
           '</property>\\n' + \
           '<property>\\n<name>yarn.nodemanager.aux-services</name>\\n' + \
           '<value>mapreduce_shuffle</value>\\n</property>\\n' + \
           '<property><name>yarn.nodemanager.resource.memory-mb</name>\\n' + \
           '<value>2048</value>\\n </property>\\n' + \
           '<property>\\n<name>yarn.scheduler.minimum-allocation-mb</name>\\n' + \
           '<value>1024</value>\\n</property>\\n' + \
           '<property><name>yarn.scheduler.maximum-allocation-mb</name>\\n' + \
           '<value>2048</value>\\n</property>\\n' + \
           '<property>\\n<name>yarn.app.mapreduce.am.resource.mb</name>\\n' + \
           '<value>1024</value>\\n</property>\\n' + \
           '<property>\\n<name>yarn.app.mapreduce.am.command-opts</name>\\n' + \
           '<value>-Xmx1024M</value>\\n</property>\\n' 

    fabric.contrib.files.sed(filename, before, after, limit='')

@serial
def configHadoop():
    execute(changeMapRedSite)
    execute(changeCoreSite)
    execute(changeHDFSSite)
    execute(changeYarnSiteMaster)
    execute(changeYarnSiteSlave)
    #execute(changeMasters)
    execute(changeSlaves)
    
# change limits.conf in the case work with heave load (readwrite many files)    
@roles('masters', 'slaves')
def changeLimitsUbuntu(): #require sudo permission
    cmd = 'echo "* hard nofile 128000" | sudo tee -a /etc/security/limits.conf && echo "* soft nofile 128000" | sudo tee -a /etc/security/limits.conf'
    sudo(cmd)


#Flink Configuration
@roles('masters', 'slaves')
def changeFlinkSlaves():
    filename = flink_home + '/conf/slaves'
    before = 'localhost'
    after = ''
    slaves = env.roledefs['slaves']
    for slave in slaves:
        after = after + slave + '\\n'
    fabric.contrib.files.sed(filename, before, after, limit='')

@roles('masters', 'slaves')
def changeFlinkConf(master=master_cluster, flink_parallelism_default=flink_parallelism_default, flink_master_heap=flink_master_heap, flink_slave_heap=flink_slave_heap):
    filename = flink_home + '/conf/flink-conf.yaml'
    before = 'jobmanager.rpc.address: localhost'
    after = 'jobmanager.rpc.address: ' + master
    fabric.contrib.files.sed(filename, before, after, limit='')
    before = 'jobmanager.heap.mb: 256'
    after = 'jobmanager.heap.mb: ' + str(flink_master_heap)
    fabric.contrib.files.sed(filename, before, after, limit='')
    before = 'taskmanager.heap.mb: 512'
    after = 'taskmanager.heap.mb: ' + str(flink_slave_heap)
    fabric.contrib.files.sed(filename, before, after, limit='')

@roles('masters', 'slaves')
def changeFlinkPara(flink_slave_slots=flink_slave_slots):
    filename = flink_home + '/conf/flink-conf.yaml'
    before = 'taskmanager.numberOfTaskSlots: 1'
    after = 'taskmanager.numberOfTaskSlots: ' + str(flink_slave_slots)
    fabric.contrib.files.sed(filename, before, after, limit='')
    before = 'parallelism.default: 1'
    after = 'parallelism.default: ' + str(flink_parallelism_default)
    fabric.contrib.files.sed(filename, before, after, limit='')

@serial
def configFlink():
    execute(changeFlinkSlaves)
    execute(changeFlinkConf)

@roles('masters')
def startFlink():
    run(cluster_home + 'hadoop/sbin/start-all.sh') #start Hadoop
    time.sleep(5)
    run(flink_home + '/bin/start-webclient.sh') #start Flink Web client

@roles('masters')
def stopFlink():
    run(cluster_home + 'hadoop/sbin/stop-all.sh') #stop Hadoop
    run(flink_home + '/bin/stop-webclient.sh') #stop Flink Web client


#Spark Configuration
@roles('masters', 'slaves')
def changeSparkSlaves():
    filename = spark_home + '/conf/slaves.template'
    before = 'localhost'
    after = ''
    slaves = env.roledefs['slaves']
    for slave in slaves:
        after = after + slave + '\\n'
    fabric.contrib.files.sed(filename, before, after, limit='')
    run('mv ' + filename + ' ' + spark_home +'/conf/slaves')

@roles('masters', 'slaves')
def compileSpark():
    cmd = 'cd ' + spark_home + '/build &&' + 'rm -rf *.jar && wget ' + url_sbt + ' && mv sbt-launch.jar sbt-launch-0.13.7.jar'
    run(cmd)
    hadoop_version = url_hadoop.split("hadoop-")[-1].split(".0.tar.gz")[0]
    cmd = 'cd ' + spark_home + ' && build/sbt -Pyarn -Phadoop-'+ hadoop_version + ' -DskipTests assembly'
    run(cmd)

@serial
def configSpark():
    execute(changeSparkSlaves)


### Cluster managements ###

#Clean
@roles('masters', 'slaves')
def cleanCluster():
    run('rm -rf ' + cluster_home + 'hadoop/logs/*')
    run('sudo rm -rf /tmp/*')
    run('rm -rf ' + cluster_home + 'hadoop/tmp/*')
    run('rm -rf ' + cluster_home + 'hadoop/hdfs/data/')
    run('rm -rf ' + cluster_home + 'hadoop/hdfs/name/')
    run('rm -rf ' + cluster_home + 'hadoop/hdfs/hadoop-unjar*')
    run('rm -rf ' + cluster_home + 'hadoop/hdfs/dfs/*')
    run('rm -rf ' + cluster_home + 'hadoop/hdfs/mapreduce/*')
    run('rm -rf ' + flink_home + '/logs/*')
    run('rm -rf ' + spark_home + '/logs/*')

#Remove old host key
@roles('masters', 'slaves')
def removeHostKey():
    for host in env.roledefs['masters']:
        run('ssh-keygen -R ' + host)
    for host in env.roledefs['slaves']:
        run('ssh-keygen -R ' + host)   

@roles('masters', 'slaves')
def listHadooplogs():
    run('ls ' + cluster_home + 'hadoop/logs/')

#Format Hadoop
@roles('masters')
def formatHadoop():
    with settings(warn_only=True):
        run('echo "Y\\n" |' + cluster_home + 'hadoop/bin/hadoop namenode -format')
        run(cluster_home + 'hadoop/bin/hadoop datanode -format')

#Refresh Cluster
def refreshCluster():
    execute(cleanCluster)
    execute(formatHadoop)

#Start Cluster
@roles('masters')
def startCluster():
    #Hadoop
    run(cluster_home + 'hadoop/sbin/start-all.sh', pty=False)
    #Flink web client
    run(flink_home + '/bin/start-webclient.sh', pty=False)
    #Spark
    #run(spark_home + '/sbin/start-all.sh', pty=False)

#Stop Cluster
@roles('masters', 'slaves')
def stopCluster():
    run('pkill -9 java', pty=True) #This is a ugly way to stop but it works well

#Change Batchrc environment
@roles('masters', 'slaves')
def changeBashrc():
    bash_home = user_home + ".bashrc"
    java_home = parameters['java_home']
    before = "#\ for\ examples" # a trick to add new content to bashrc
    after =   '#Flink-Spark-Hadoop\\n'+ 'export SCALA_HOME=' + scala_home + '\\n' + 'export PATH=$PATH:' + scala_home + '/bin\\n' + \
              'export FLINK_HOME=' + flink_home + '\\n' + \
              'export PATH=$PATH:' + flink_home +'/bin\\n' + \
              'export SPARK_HOME=' + spark_home + '\\n' + \
              'export PATH=$PATH:' + spark_home +'/bin\\n' + \
              'export YARN_CONF_DIR=' + cluster_home + 'hadoop/etc/hadoop\\n' + \
              'export HADOOP_CONF_DIR=' + cluster_home + 'hadoop/etc/hadoop\\n' + \
              'export HADOOP_HOME=' + cluster_home + 'hadoop\\n' + \
              'export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native\\n' + \
              'export HADOOP_OPTS = \"-Djava.library.path=$HADOOP_HOME/lib\"\\n' + \
              'export JAVA_HOME=' + java_home
    fabric.contrib.files.sed(bash_home, before, after, limit='')
    #fabric.contrib.files.append(bash_home, content, use_sudo=False, partial=False, escape=True, shell=False)


### Setup Cluster###
@serial
def setupCluster():
    #execute(installRequirement)
    execute(downloads)
    execute(configHadoop)
    execute(configFlink)
    execute(configSpark)
    execute(changeBashrc)
