/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.launch.bigdata.spark;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaOdpsOps;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction3;
import org.apache.spark.api.java.function.VoidFunction4;
import org.apache.spark.sql.Row;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.Random;
import java.util.UUID;


/**
 * 1. build aliyun-cupid-sdk
 * 2. properly set spark.defaults.conf
 * 3. bin/spark-submit --master yarn-cluster --class com.aliyun.odps.spark.examples.JavaOdpsTableReadWrite \
 * /path/to/aliyun-cupid-sdk/examples/spark-examples/target/spark-examples_2.11-1.0.0-SNAPSHOT-shaded.jar
 */
public class SparkOdpsTableWriteHbase {

    /**
     * This Demo represent the ODPS Table Read/Write Usage in following scenarios
     * This Demo will fail if you don't have such Table inside such Project
     * 1. read from normal table via rdd api
     * 2. read from single partition column table via rdd api
     * 3. read from multi partition column table via rdd api
     * 4. read with multi partitionSpec definition via rdd api
     * 5. save rdd into normal table
     * 6. save rdd into partition table with single partition spec
     * 7. dynamic save rdd into partition table with multiple partition spec
     */
	
	private static final String TABLE_NAME = "test7";
    private static final String CF_DEFAULT = "cf1";
    //private static final String zkAddress = "hb-proxy-pub-xxxx-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-xxxx-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-xxxx-003.hbase.rds.aliyuncs.com:2181";
    private static final String zkAddress = "hb-xxx-002.hbase.rds.aliyuncs.com:2181,hb-xxx-001.hbase.rds.aliyuncs.com:2181,hb-xxx-003.hbase.rds.aliyuncs.com:2181";
    transient static Configuration config;
    
    
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaOdpsTableReadWrite");
        sparkConf.setAppName("SparkOdpsTableWriteHbase");
        sparkConf
                //.set("spark.master", "local[1]") 
                .set("odps.project.name", "xxxxx_dw_dev")
                .set("odps.access.id", "xxxxxx")
                .set("odps.access.key", "xxxxx")
                .set("odps.end.point", "http://service.cn.maxcompute.aliyun.com/api")
                .set("spark.sql.catalogImplementation", "odps");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaOdpsOps javaOdpsOps = new JavaOdpsOps(ctx);
        String projectName = ctx.getConf().get("odps.project.name");

        config = HBaseConfiguration.create();
        System.out.println("11111111111="+zkAddress);
        config.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        tableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT));
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();
        boolean tableExists = admin.tableExists(TableName.valueOf(TABLE_NAME));
        if (tableExists){
            System.out.println("success");
            admin.disableTable(TableName.valueOf(TABLE_NAME));
            admin.deleteTable(TableName.valueOf(TABLE_NAME));
        }
        admin.createTable(tableDescriptor);
        System.out.print("Creating table. ");
        
        /**
         *  read from normal table via rdd api
         *  desc cupid_wordcount;
         *  +------------------------------------------------------------------------------------+
         *  | Field           | Type       | Label | Comment                                     |
         *  +------------------------------------------------------------------------------------+
         *  | id              | string     |       |                                             |
         *  | value           | string     |       |                                             |
         *  +------------------------------------------------------------------------------------+
         */
        JavaRDD<Tuple2<String, String>> rdd_0 = javaOdpsOps.readTable(
                projectName,
                "result_table",
                new Function2<Record, TableSchema, Tuple2<String, String>>() {
                    public Tuple2<String, String> call(Record v1, TableSchema v2) throws Exception {
                        return new Tuple2<String, String>(v1.getString(0), v1.getString(1));
                    }
                },
                0
        );

        System.out.println("rdd_0 count: " + rdd_0.count());

       /* rdd_0.foreach(new VoidFunction<Row>() {
            private static final long serialVersionUID = 1L;
            public void call(Row row) throws Exception {
                String id = row.getAs("id").toString();
                String name = row.getAs("name").toString();
                System.out.println("id:" + id + "    name:" + name);
                Put put = new Put(UUID.randomUUID().toString().getBytes());
                put.addColumn(CF_DEFAULT.getBytes(), "id".getBytes(), Bytes.toBytes(id));
                put.addColumn(CF_DEFAULT.getBytes(), "name".getBytes(), Bytes.toBytes(name));
                HTable table = new HTable(config, TABLE_NAME.getBytes());
                table.put(put);
            }
        });*/
        rdd_0.foreach(new VoidFunction<Tuple2<String, String>>() {
            private static final long serialVersionUID = 1L;
            public void call(Tuple2<String, String> tuple) throws Exception {
               
            	String id =tuple._1;
                String name = tuple._2;
                System.out.println("id:" + id + "    name:" + name);
                Put put = new Put(UUID.randomUUID().toString().getBytes());
                put.addColumn(CF_DEFAULT.getBytes(), "id".getBytes(), Bytes.toBytes(id));
                put.addColumn(CF_DEFAULT.getBytes(), "name".getBytes(), Bytes.toBytes(name));
                config = HBaseConfiguration.create();
                System.out.println("11111111111="+zkAddress);
                config.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
                Connection connection = ConnectionFactory.createConnection(config);
                Table table=connection.getTable(TableName.valueOf(TABLE_NAME));
                table.put(put);
                
            }
        });
        
    }
}
