场景： 使用阿里云实时计算平台，官方主推SQL类型作业，然而SQL作业对于有一定的局限性，使用DATASTREAM类型作业实现，消费Kafka数据，sink结果数据到阿里云hbase。主要遇到问题是：
1 Hbase网络和白名单问题。
2 alihbase-client  相关依赖冲突，部分依赖需要exclusion。
   com.google.protobuf 需要使用maven shade处理。
