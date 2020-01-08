场景：
使用dataworks搭建企业数据仓库，需要导出数据到阿里云hbase，数据集成目前不支持hbase动态列模式，而且如果对于复杂的导出业务，比如对数据或者rowkey做特殊处理。数据集成功能都是没办法的。
于是考虑的解决方案是，使用dataworks spark节点类型任务，读取odps表数据，写入到hbase。
注意点：
1 在本地测试可以使用hbase 外网地址，提交到dataworks需要使用内网地址。
2  提交任务需要配置spark.hadoop.odps.cupid.vpc.domain.list



3  当提交jar包大小大于50MB，需要使用maxcompute客户端上次jar，并配置spark.hadoop.odps.cupid.resources，格式launch_bigdata_dw_dev.sk_bigdata-dw-odpsspark-1.0.0-SNAPSHOT-jar-with-dependencies.jar

4  接下来就是很多依赖jar 冲突，exclusion处理，alihbase-client相关hadoop依赖和netty依赖都要exclusion掉。
	<dependencies>
		<dependency>
			<groupId>jdk.tools</groupId>
			<artifactId>jdk.tools</artifactId>
			<version>1.8</version>
			<scope>system</scope>
			<systemPath>${JAVA_HOME}/lib/tools.jar</systemPath>
		</dependency>
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-core_${scala.binary.version}</artifactId>
			<version>${spark.version}</version>
			<scope>provided</scope>
			<exclusions>
				<exclusion>
					<groupId>org.scala-lang</groupId>
					<artifactId>scala-library</artifactId>
				</exclusion>
				<exclusion>
					<groupId>org.scala-lang</groupId>
					<artifactId>scalap</artifactId>
				</exclusion>
				<exclusion>
					<groupId>io.netty</groupId>
					<artifactId>netty-all</artifactId>
				</exclusion>
			</exclusions>
		</dependency>
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-sql_${scala.binary.version}</artifactId>
			<version>${spark.version}</version>
			<scope>provided</scope>
		</dependency>


		<dependency>
			<groupId>com.aliyun.odps</groupId>
			<artifactId>cupid-sdk</artifactId>
			<version>${cupid.sdk.version}</version>
			<scope>provided</scope>
		</dependency>
		<dependency>
			<groupId>com.aliyun.odps</groupId>
			<artifactId>hadoop-fs-oss</artifactId>
			<version>${cupid.sdk.version}</version>
		</dependency>

		<dependency>
			<groupId>com.aliyun.odps</groupId>
			<artifactId>odps-spark-datasource_${scala.binary.version}</artifactId>
			<version>${cupid.sdk.version}</version>
		</dependency>
		<dependency>
			<groupId>org.scala-lang</groupId>
			<artifactId>scala-library</artifactId>
			<version>${scala.version}</version>
		</dependency>
		<dependency>
			<groupId>org.scala-lang</groupId>
			<artifactId>scala-actors</artifactId>
			<version>${scala.version}</version>
		</dependency>

		<!-- <dependency> <groupId>com.aliyun.odps</groupId> <artifactId>cupid-core_2.11</artifactId> 
			<version>1.0.0</version> <scope>provided</scope> </dependency> <dependency> 
			<groupId>com.aliyun.odps</groupId> <artifactId>cupid-datasource_2.11</artifactId> 
			<version>1.0.0</version> <scope>provided</scope> </dependency> <dependency> 
			<groupId>com.aliyun.odps</groupId> <artifactId>cupid-client_2.11</artifactId> 
			<version>1.0.0</version> <scope>provided</scope> </dependency> -->




		<!-- hbase 依赖 -->
		<dependency>
			<groupId>com.aliyun.hbase</groupId>
			<artifactId>alihbase-client</artifactId>
			<version>1.1.9</version>
			<exclusions>
				<exclusion>
					<groupId>org.apache.hadoop</groupId>
					<artifactId>hadoop-yarn-common</artifactId>
				</exclusion>
				<exclusion>
					<groupId>io.netty</groupId>
					<artifactId>netty-all</artifactId>
				</exclusion>
				<exclusion>
					<groupId>org.apache.hadoop</groupId>
					<artifactId>hadoop-common</artifactId>
				</exclusion>
				<exclusion>
					<groupId>org.apache.hadoop</groupId>
					<artifactId>hadoop-mapreduce-client-core</artifactId>
				</exclusion>
				<exclusion>
					<groupId>org.apache.hadoop</groupId>
					<artifactId>hadoop-auth</artifactId>
				</exclusion>
			</exclusions>
		</dependency>
	</dependencies>


参考文档：
https://github.com/aliyun/MaxCompute-Spark/wiki/03.-%E5%BA%94%E7%94%A8%E5%BC%80%E5%8F%91%E7%A4%BA%E4%BE%8B?spm=a2c4g.11186623.2.11.31867c9bZNvUWV#1

https://help.aliyun.com/document_detail/137513.html?spm=a2c4g.11174283.6.738.523b2b65ogru3P

https://help.aliyun.com/document_detail/27971.html?spm=a2c4g.11186623.6.941.61387956IZ3NBQ
