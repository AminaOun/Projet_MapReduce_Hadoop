# Projet_MapReduce_Hadoop

In this project we will create MapReduce programs, MapReduce is a programming framework of Hadoop that is used for parallel processing of data.

1) Mapper phase- It takes raw file as input and separate required output key and output value, in this project key is years and value is maximum and minimm temperature.
2) Reducer phase- The output from map phase will be input to reduce phase. It will group the data based on key and then aggregate all output key and output values.
The reducer output will be sent to HDFS (Hadoop Distributed File System).

To create this project you need to install Hadoop on your system.
