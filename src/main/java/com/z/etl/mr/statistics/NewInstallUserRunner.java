package com.z.etl.mr.statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.z.etl.common.EventLogConstants;
import com.z.etl.common.GlobalConstants;
import com.z.etl.common.EventLogConstants.EventEnum;
import com.z.etl.dimension.key.stats.StatsUserDimension;
import com.z.etl.dimension.value.MapWritableValue;
import com.z.etl.util.TimeUtil;
/**
 * 执行数据统计任务：重点自定义写入到mysql
 * 
 * @author wolf
 *
 */
public class NewInstallUserRunner implements Tool {
	private Configuration conf = null;

	public static void main(String[] args) {
		try {
			int status = ToolRunner.run(new NewInstallUserRunner(), args);
			System.exit(status);
			if (status == 0) {
				System.out.println("运行成功");
			} else {
				System.out.println("运行失败");
			}
		} catch (Exception e) {
			System.out.println("运行失败");
			e.printStackTrace();
		}
	}

	@Override
	public void setConf(Configuration conf) {
		// 添加自己开发环境所有需要的其他资源属性文件
		conf.addResource("etl-env.xml");
		conf.addResource("output-collector.xml");
		conf.addResource("query-mapping.xml");

		this.conf = HBaseConfiguration.create(conf);
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		// 1、得到conf对象
		Configuration conf = this.getConf();
		this.processArgs(conf, args);
		// 2、创建Job
		Job job = Job.getInstance(conf, "new_install_users");

		job.setJarByClass(NewInstallUserRunner.class);

		// 3、Mapper设置HBase的Mapper,InputFormat
		this.setHBaseInputConfig(job);

		// 4、设置Reducer
		job.setReducerClass(NewInstallUserReducer.class);
		job.setOutputKeyClass(StatsUserDimension.class);
		job.setOutputValueClass(MapWritableValue.class);

		// 5、设置job的输出
		job.setOutputFormatClass(MySQLOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * 初始化HBase Mapper
	 */

	private void setHBaseInputConfig(Job job) {
		Configuration conf = job.getConfiguration();

		String dateStr = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);

		List<Scan> scans = new ArrayList<>();
		// 1、构建Filter
		FilterList filterList = new FilterList();

		// 2、构建过滤：只需要返回HBase表中Launch事件所代表的数据 en = e_l
		filterList.addFilter(new SingleColumnValueFilter(EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME,
				Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareOp.EQUAL,
				Bytes.toBytes(EventEnum.LAUNCH.alias)));

		// 3、构建其他过滤规则，一会需要添加到Filter中
		String[] columns = new String[] { EventLogConstants.LOG_COLUMN_NAME_PLATFORM, // 平台名称
				EventLogConstants.LOG_COLUMN_NAME_VERSION, // 平台版本
				EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, // 浏览器名称
				EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, // 浏览器版本
				EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, // 服务器时间
				EventLogConstants.LOG_COLUMN_NAME_UUID, // uuid 访客唯一标识符
				EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME,// 事件名称
		};

		filterList.addFilter(this.getColumnFilter(columns));

		// 4、访问HBase数据
		long startDate, endDate;// scan表区间范围
		// 传入参数的时间毫秒数：当天起始时间
		long date = TimeUtil.parseString2Long(dateStr);
		// 传如参数当天的最后时刻：当天结束时间
		long endOfDate = date + GlobalConstants.DAY_OF_MILLISECONDS;

		long firstDayOfWeek = TimeUtil.getFirstDayOfThisWeek(date);
		long lastDayOfWeek = TimeUtil.getFirstDayOfNextWeek(date);
		long firstDayOfMonth = TimeUtil.getFirstDayOfThisMonth(date);
		long lastDayOfMonth = TimeUtil.getFirstDayOfNextMonth(date);

		// TimeUtil.getTodayInMills返回系统当天时间0点0分0秒毫秒数：执行代码时，系统的当前时间
		startDate = Math.min(firstDayOfWeek, firstDayOfMonth);

		endDate = TimeUtil.getTodayInMillis() + GlobalConstants.DAY_OF_MILLISECONDS;

		if (endDate > lastDayOfWeek || endDate > lastDayOfMonth) {
			endDate = Math.max(lastDayOfMonth, lastDayOfWeek);
		} else {
			endDate = endOfDate;
		}

		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			// 构建Scan对象
			for (long begin = startDate; begin < endDate; begin += GlobalConstants.DAY_OF_MILLISECONDS) {
				// 表名组成：tablename = event-logs20170816
				// 拼接出来的结果是：20170816
				String tableNameSuffix = TimeUtil.parseLong2String(begin, TimeUtil.HBASE_TABLE_NAME_SUFFIX_FORMAT);
				String tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + tableNameSuffix;
				System.out.println(tableName);

				// 表是否存在
				if (admin.tableExists(Bytes.toBytes(tableName))) {
					Scan scan = new Scan();
					scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(tableName));
					scan.setFilter(filterList);
					scans.add(scan);
				}
			}

			// 访问HBase中的表中的数据
			if (scans.isEmpty()) {
				throw new RuntimeException("没有找到任何对应的数据！");
			}

			// 如果在Windwos上运行，需要改为false，如果打成 jar包提交linux运行，则需要为true，默认为：true
			TableMapReduceUtil.initTableMapperJob(scans, NewInstallUsersMapper.class, StatsUserDimension.class,	Text.class, job, true);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Job脚本如下： bin/yarn jar ETL.jar com.z.transformer.mr.etl.AnalysisDataRunner
	 * -date 2017-08-14
	 */
	private void processArgs(Configuration conf, String[] args) {
		String date = null;
		for (int i = 0; i < args.length; i++) {
			if ("-date".equals(args[i])) {
				date = args[i + 1];
				break;
			}
		}

		if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
			// 默认清洗昨天的数据到HBase
			date = TimeUtil.getYesterday();
		}
		// 将要清洗的目标时间字符串保存到conf对象中
		conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
	}

	/**
	 * 得到过滤的列的对象
	 * 
	 * @param columns
	 * @return
	 */
	private Filter getColumnFilter(String[] columns) {
		byte[][] prefixes = new byte[columns.length][];
		for (int i = 0; i < columns.length; i++) {
			prefixes[i] = Bytes.toBytes(columns[i]);
		}
		return new MultipleColumnPrefixFilter(prefixes);
	}
}
