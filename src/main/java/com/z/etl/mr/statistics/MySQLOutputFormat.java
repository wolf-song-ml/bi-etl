package com.z.etl.mr.statistics;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.z.etl.collector.ICollector;
import com.z.etl.common.GlobalConstants;
import com.z.etl.common.KpiType;
import com.z.etl.converter.IDimensionConverter;
import com.z.etl.converter.impl.DimensionConverterImpl;
import com.z.etl.dimension.key.BaseDimension;
import com.z.etl.dimension.value.BaseStatsValueWritable;
import com.z.etl.util.JDBCManager;

public class MySQLOutputFormat extends OutputFormat<BaseDimension, BaseStatsValueWritable> {

	@Override
	public RecordWriter<BaseDimension, BaseStatsValueWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// 构建属于当前outputforamt的数据输出器
		Configuration conf = context.getConfiguration();
		// 1. 创建jdbc连接
		Connection conn = null;
		try {
			// 根据上下文中配置的信息获取数据库连接
			// 需要在hadoop的configuration中配置mysql的驱动连接信息
			conn = JDBCManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
			conn.setAutoCommit(false); // 关闭自动提交机制，方便我们进行批量提交
		} catch (SQLException e) {
			throw new IOException(e);
		}

		// 2. 构建对象并返回
		return new TransformerRecordWriter(conf, conn);
	}

	@Override
	public void checkOutputSpecs(JobContext context)
			throws IOException, InterruptedException {
		// 该方法的主要作用是检测输出空间的相关属性，比如是否存在之类的情况
		// 如果说job运行前提的必须条件不满足，直接抛出一个exception。
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// 使用的是FileOutputFormat中默认的方式
		String name = context.getConfiguration().get(FileOutputFormat.OUTDIR);
		Path output = name == null ? null : new Path(name);
		return new FileOutputCommitter(output, context);
	}

	/**
	 * 自定义的具体将reducer输出数据输出到mysql表的输出器
	 */
	static class TransformerRecordWriter extends RecordWriter<BaseDimension, BaseStatsValueWritable> {
		private Connection conn = null; // 数据库连接
		private Configuration conf = null; // 上下文保存成属性
		private Map<KpiType, PreparedStatement> pstmtMap = new HashMap<KpiType, PreparedStatement>();
		private int batchNumber = 0; // 批量提交数据大小
		private Map<KpiType, Integer> batch = new HashMap<KpiType, Integer>();
		private IDimensionConverter converter = null; // 维度转换对象

		
		/**
		 * 构造方法
		 * 
		 * @param conf
		 * @param conn
		 */
		public TransformerRecordWriter(Configuration conf, Connection conn) {
			this.conf = conf;
			this.conn = conn;
			this.batchNumber = Integer.valueOf(conf.get(GlobalConstants.JDBC_BATCH_NUMBER,
					GlobalConstants.DEFAULT_JDBC_BATCH_NUMBER));
			this.converter = new DimensionConverterImpl();
		}

		@Override
		public void write(BaseDimension key, BaseStatsValueWritable value)
				throws IOException, InterruptedException {
			try {
				// 每个分析的kpi值是不一样的，一样的kpi有一样的插入sql语句
				KpiType kpi = value.getKpi();

				// count表示当前PreparedStatement对象中需要提交的记录数量
				int count = 0; 

				// 从上下文中获取kpi对应的sql语句
				String sql = this.conf.get(kpi.name);
				
				// 1. 获取数据库的PreparedStatement对象
				PreparedStatement pstmt = null;
				// 判断当前kpi对应的preparedstatment对象是否存在
				if (this.pstmtMap.containsKey(kpi)) {
					// 获取对应的对象
					pstmt = this.pstmtMap.get(kpi); 
					if (batch.containsKey(kpi)) {
						count = batch.get(kpi);
					}
				} else {
					// 不存在， 第一次创建一个对象
					pstmt = conn.prepareStatement(sql);
					// 保存到map集合中
					this.pstmtMap.put(kpi, pstmt);
				}

				// 2. 获取collector类名称
				String collectorClassName = this.conf.get(GlobalConstants.OUTPUT_COLLECTOR_KEY_PREFIX + kpi.name);
				// 3. 创建class对象
				Class<?> clz = Class.forName(collectorClassName); // 获取类对象
				// 调用newInstance方法进行构成对象，要求具体的实现子类有默认无参构造方法
				ICollector collector = (ICollector) clz.newInstance();

				// 4. 设值参数
				collector.collect(conf, key, value, pstmt, converter);

				// 5. 处理完成后，进行累计操作
				count++;
				this.batch.put(kpi, count);

				// 6. 执行，采用批量提交的方式
				if (count > this.batchNumber) {
					pstmt.executeBatch(); // 批量提交
					conn.commit(); // 连接提交
					this.batch.put(kpi, 0);
				}
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		@Override
		public void close(TaskAttemptContext context)
				throws IOException, InterruptedException {
			// 关闭资源
			try {
				// 1. 进行jdbc提交操作

				for (Map.Entry<KpiType, PreparedStatement> entry : this.pstmtMap.entrySet()) {
					try {
						entry.getValue().executeBatch(); // 批量提交
					} catch (SQLException e) {
						// nothings
					}
				}

				this.conn.commit(); // 数据库提交
			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				// 2. 关闭资源
				for (Map.Entry<KpiType, PreparedStatement> entry : this.pstmtMap.entrySet()) {
					JDBCManager.closeConnection(null, entry.getValue(), null);
				}
				JDBCManager.closeConnection(conn, null, null);
			}
		}
	}
	
}
