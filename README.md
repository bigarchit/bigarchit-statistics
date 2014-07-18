bigarchit-statistics
====================
###离线统计分析平台
[http://bigarchit.github.io/bigarchit-statistics/](http://bigarchit.github.io/bigarchit-statistics/)

统计系统是大数据技术常见的应用场景，从大量数据中提取有用的信息，经过复杂的运算，最终得到比较直观的统计数据.
####架构图
![架构图](http://bigarchit.github.io/bigarchit-statistics/images/statistics_1.png)

系统服务，应用等都会产生大量的日志信息，通过flume收容或者爬虫，获取到原始的日志信息。 日志经过ETL层的清洗后， 以一定的数据格式存入HDFS，支持日志收集完成。

利用已有的日志，经过一定的算法处理，或者通过hive等工具的个性化检索，就可以获得相应的信息。将这些得到的信息存入中间数据库，在数据库之上，就可以建立各种统计信息系统。

同时，也可以在底层加入消息队列，上层加入strom或者sparkstreaming等实时分析框架， 实收收容日志，实时处理， 搭建即时分析系统。

####工程架构

statistics 只是对架构中的离线数据统计的抽象化实现。

* dump
* service
* web

##### dump
dump 主要负责从hdfs等文件系统中导数据到关系型数据库，非关系型数据库或者其他存储， 方便上次应用程序使用。	

![dump类图](http://bigarchit.github.io/bigarchit-statistics/images/statistics_2.png)


##### service
service 负责为db层提供对外的接口，缓存处理等。

##### web
web 主要展现统计数据。
