package com.z.etl.mr.etl;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.z.etl.common.EventLogConstants;
import com.z.etl.common.GlobalConstants;
import com.z.etl.util.TimeUtil;
/**
 * 执行etl任务
 * 
 * @author wolf
 *
 */
public class AnalysisDataRunner implements Tool{
	private Configuration conf = null;
	
	public static void main(String[] args) {
		try {
			int resultCode = ToolRunner.run(new AnalysisDataRunner(), args);
			if(resultCode == 0){
				System.out.println("Success!");
			}else{
				System.out.println("Fail!");
			}
			System.exit(resultCode);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = HBaseConfiguration.create(conf);
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		//处理 时间参数，默认或不合法时间则直接使用昨天日期
		this.processArgs(conf, args);
		//开始创建Job
		Job job = Job.getInstance(conf, "Event-ETL");
		//设置Job参数
		job.setJarByClass(AnalysisDataRunner.class);
		//Mapper参数设置
		job.setMapperClass(AnalysisDataMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Put.class);
		//Reducer参数设置
		job.setNumReduceTasks(0);
		
		//配置数据输入
		this.initJobInputPath(job);
		
		//设置输出到HBase的信息
		initHBaseOutPutConfig(job);
		
//		job.setJar("target/transformer-0.0.1-SNAPSHOT.jar");
		
		//Job提交
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	/**
	 * 初始化Job数据输入目录
	 * @param job
	 * @throws IOException 
	 */
	private void initJobInputPath(Job job) throws IOException{
		Configuration conf = job.getConfiguration();
		//获取要执行ETL的数据是哪一天的数据
		String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
		//格式化文件路径
		String hdfsPath = TimeUtil.parseLong2String(TimeUtil.parseString2Long(date), "yyyy/MM/dd");
		if(GlobalConstants.HDFS_LOGS_PATH_PREFIX.endsWith("/")){
			hdfsPath = GlobalConstants.HDFS_LOGS_PATH_PREFIX + hdfsPath;
		}else{
			hdfsPath = GlobalConstants.HDFS_LOGS_PATH_PREFIX + File.separator + hdfsPath;
		}
		
		FileSystem fs = FileSystem.get(conf);
		Path inPath = new Path(hdfsPath);
		if(fs.exists(inPath)){
			FileInputFormat.addInputPath(job, inPath);
		}else{
			throw new RuntimeException("HDFS中该文件目录不存在：" + hdfsPath);
		}
	}
	
	
	/**
	 * 设置输出到HBase的一些操作选项
	 * @throws IOException 
	 */
	private void initHBaseOutPutConfig(Job job) throws IOException{
		Configuration conf = job.getConfiguration();
		//获取要ETL的数据是哪一天
		String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
		//格式化HBase后缀名
		String tableNameSuffix = TimeUtil.parseLong2String(TimeUtil.parseString2Long(date), TimeUtil.HBASE_TABLE_NAME_SUFFIX_FORMAT);
		//构建表名
		String tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + tableNameSuffix;
		
		//指定输出
		TableMapReduceUtil.initTableReducerJob(tableName, null, job);
		
		HBaseAdmin admin = null;
		admin = new HBaseAdmin(conf);
		
		TableName tn = TableName.valueOf(tableName);
		HTableDescriptor htd = new HTableDescriptor(tn);
		//设置列族
		htd.addFamily(new HColumnDescriptor(EventLogConstants.EVENT_LOGS_FAMILY_NAME));
		//判断表是否存在
		if(admin.tableExists(tn)){
			//存在，则删除
			if(admin.isTableEnabled(tn)){
				//将表设置为不可用
				admin.disableTable(tn);
			}
			//删除表
			admin.deleteTable(tn);
		}
		
		//创建表，在创建的过程中可以考虑预分区操作
		//预分区 3个分区
//		byte[][] keySplits = new byte[3][];
		//(-∞, 1]
//		keySplits[0] = Bytes.toBytes("1");
		//(1, 2]
//		keySplits[1] = Bytes.toBytes("2");
		//(2, ∞]
//		keySplits[2] = Bytes.toBytes("3");
//		admin.createTable(htd, keySplits);
		
		admin.createTable(htd);
		admin.close();
	}
	
	//处理时间参数，如果没有传递参数的话，默认清洗前一天的。
	/**
	 * Job脚本如下： bin/yarn jar ETL.jar com.z.transformer.mr.etl.AnalysisDataRunner -date 20170814
	 * @param args
	 */
	private void processArgs(Configuration conf, String[] args){
		String date = null;
		for(int i = 0; i < args.length; i++){
			if("-date".equals(args[i])){
				date = args[i+1];
				break;
			}
		}
		
		if(StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)){
			//默认清洗昨天的数据到HBase
			date = TimeUtil.getYesterday();
		}
		//将要清洗的目标时间字符串保存到conf对象中
		conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
	}
	

}
