# HadoopMapReduce
map reduce and hadoop practice

To generate data:
javac Generator.java
java Generator

The following descriptions assume customers.csv and transactions been put onto the hdfs at the root directory

Query 1:
hadoop jar ./countrycode.jar hadoop3_1.CountryCode /customers.csv /output
