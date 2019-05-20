# hbase
数据量10亿， 2亿

sqoop 不支持hbase2.* ，sqoop 1.4.7 从oracle 往hbase 中导数据经常不成功，所以自己写了一个工具，采用jdbc 导Oracle 数据到phoenix 中。
这次就简单的实现了这个功能，也没有做性能优化活着是引擎优化。
