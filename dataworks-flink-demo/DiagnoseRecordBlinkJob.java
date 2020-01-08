package com.launch.bigdata.blinkjob.diagnoserecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import com.launch.bigdata.blinkjob.common.connectors.cloudhbase.sink.CloudHBaseSinkFunction;
import com.launch.bigdata.blinkjob.common.connectors.cloudhbase.sink.CloudPhoenixSinkFunction;
import com.launch.bigdata.blinkjob.common.connectors.cloudhbase.sink.entity.CloudHBaseDataEntity;
import com.launch.bigdata.blinkjob.common.utils.FastJsonUtil;
import com.launch.bigdata.blinkjob.diagnoserecord.entity.DiagnoseRecordBean;

/**
 * 诊断报告实时数据分析作业： 消费kafka诊断报告数据，更新数据服务存储。 source：kafka
 * sink：mysql,Es,Hbase,Phinox,kafka ...
 * */

public class DiagnoseRecordBlinkJob {

	public static void main(String[] args) throws Exception {

		Map<String, String> paramsMap = new HashMap<String, String>();
		paramsMap.put("source_kafka_topic",
				Constants4DiagnoseRecordProd.SOURCE_KAFKA_TOPIC);
		paramsMap.put("bootstrap.servers",
				Constants4DiagnoseRecordProd.SOURCE_KAFKA_BOOTSTRAP_SERVERS);
		paramsMap.put("group.id",
				Constants4DiagnoseRecordProd.SOURCE_KAFKA_GROUP_ID);
		ParameterTool parameterTool = ParameterTool.fromMap(paramsMap);
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(2);

		DataStream<String> input = env.addSource(new FlinkKafkaConsumer010<>(
				parameterTool.getRequired("source_kafka_topic"),
				new SimpleStringSchema(), parameterTool.getProperties()));

		DataStream<String> printSourceDs = input
				.map(new MapFunction<String, String>() {
					private static final long serialVersionUID = 5451632324874329009L;

					@Override
					public String map(String value) throws Exception {
						// 解析出json, 获取诊断报告id,vin,经纬度
						return "printSourceDs22====" + value;
					}
				});

		printSourceDs.print();

		DataStream<String> fliterDs = input
				.flatMap(new FlatMapFunction<String, String>() {

					private static final long serialVersionUID = -3936621579768715527L;

					@Override
					public void flatMap(String value, Collector<String> out)
							throws Exception {

						if (FastJsonUtil.isJson(value) == false) {
							System.out.println("is not Json==" + value);
							return;
						}

						DiagnoseRecordBean diagnoseRecordBean = FastJsonUtil
								.toBean(value, DiagnoseRecordBean.class);
						if (diagnoseRecordBean == null) {

							return;
						}
						if (StringUtils.isEmpty(diagnoseRecordBean.getVin())) {

							return;
						}

						if (StringUtils.isEmpty(diagnoseRecordBean.getId())) {

							return;
						}

						out.collect(value);

					}
				});
		fliterDs.print();

		DataStream<String> DiagnoseRecordByVinDs = fliterDs
				.map(new MapFunction<String, String>() {
					private static final long serialVersionUID = 5451632324874329009L;

					@Override
					public String map(String value) throws Exception {
						// 解析出json, 获取诊断报告id,vin,经纬度
						DiagnoseRecordBean diagnoseRecordBean = FastJsonUtil
								.toBean(value, DiagnoseRecordBean.class);
						diagnoseRecordBean.setReportjsonStr(value);
						CloudHBaseDataEntity cloudHBaseDataEntity  =new CloudHBaseDataEntity();
						cloudHBaseDataEntity.setRowKey(diagnoseRecordBean.getVin());
						cloudHBaseDataEntity.setCf("cf1");
						cloudHBaseDataEntity.setColumnName(diagnoseRecordBean.getId());
						cloudHBaseDataEntity.setValue(diagnoseRecordBean.getReportjsonStr());
						String jsonStr = FastJsonUtil.parseToJSON(cloudHBaseDataEntity);

						return jsonStr;
					}
				});
		DiagnoseRecordByVinDs.print();

		/*
		 * sink 到kafka：
		 * 
		 * Properties producerConfig = new Properties();
		producerConfig.put("metadata.broker.list",
				Constants4DiagnoseRecordProd.SINK_KAFKA_BOOTSTRAP_SERVERS);
		producerConfig.put("bootstrap.servers",
				Constants4DiagnoseRecordProd.SINK_KAFKA_BOOTSTRAP_SERVERS);
		producerConfig.put("max.request.size", 41943040);
		 * 
		 * FlinkKafkaProducer010<String> kafkaProducer = new FlinkKafkaProducer010(
				Constants4DiagnoseRecordProd.SINK_KAFKA_TOPIC_TEST,
				new SimpleStringSchema(), producerConfig);

		DiagnoseRecordByVinDs.addSink(kafkaProducer);*/
		DiagnoseRecordByVinDs.addSink(new CloudHBaseSinkFunction<String>("xxxxx", "dataapi:table_test01"));
		DiagnoseRecordByVinDs.addSink(new CloudPhoenixSinkFunction<String>("xxxxx", "dataapi:table_test01"));

		env.execute("DiagnoseRecordBlinkJob");
	}

}
