# README #
SE Group, TU Desden, Germany

### What is this repository for? ###

* One-click deploy Flink, Spark and Hadoop

###How to start everything?###

* Step1: Install fabric:  
  $./setup.sh
* Step2: Update master node, slave nodes in nodes.json. Change config parameters in config.json.
* Step3: Setup Flink, Spark, and Hadoop cluster: 
  $fab setupCluster
* Step4: Start Flink, Hadoop: 
  $fab startFlink
* Step5: Enjoy Apache Flink

### Contact? ###
* Do Le Quoc: do@se.inf.tu-dresden.de 
