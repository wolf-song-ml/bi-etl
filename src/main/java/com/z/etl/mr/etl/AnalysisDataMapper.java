package com.z.etl.mr.etl;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.z.etl.common.EventLogConstants;
import com.z.etl.common.EventLogConstants.EventEnum;
import com.z.etl.util.LoggerUtil;
import com.z.etl.util.TimeUtil;
/**
 * 日志清洗、过滤、结构，并写到hbase
 * 
 * @author wolf
 *
 */
public class AnalysisDataMapper extends Mapper<Object, Text, NullWritable, Put>{
	private static final Logger logger = Logger.getLogger(AnalysisDataMapper.class);
	
	private CRC32 crc32 = null;
	private byte[] family = null;
	private long currentDayInMills = -1;

	@Override
	protected void setup(Mapper<Object, Text, NullWritable, Put>.Context context) throws IOException, InterruptedException {
		crc32 = new CRC32();
		this.family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;
		currentDayInMills = TimeUtil.getTodayInMillis();
	}

	//1、覆写map方法
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//2、将原始数据通过LoggerUtil解析成Map键值对
		Map<String, String> clientInfo = LoggerUtil.handleLogText(value.toString());
		
		//2.1、如果解析失败，则Map集合中无数据
		if(clientInfo.isEmpty()){
			logger.debug("日志解析失败：" + value.toString());
			return;
		}
		
		//3、根据解析后的数据，生成对应的Event事件类型
		EventEnum event = EventEnum.valueOfAlias(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME));
		if(event == null){
			//4、无法处理的事件，直接输出事件类型
			logger.debug("无法匹配对应的事件类型：" + clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME));
		}else{
			//5、处理具体的事件
			handleEventData(clientInfo, event, context, value);
		}
		
	}
	
	/**
	 * 处理具体的事件
	 * @param clientInfo
	 * @param event
	 * @param context
	 * @param value
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void handleEventData (Map<String, String> clientInfo, EventEnum event, Context context, Text value) throws IOException, InterruptedException{
		//6、事件成功通过过滤，则处理事件
		if(filterEventData(clientInfo, event)){
			outPutData(clientInfo, context);
		}else{
			//事件没有通过过滤，输出
			logger.debug("事件格式不正确：" + value.toString());
		}
	}
	
	//6、事件成功通过过滤，则处理事件
	public boolean filterEventData(Map<String, String> clientInfo, EventEnum event){
		//事件数据全局过滤
		boolean result = StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME))
				&& StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_PLATFORM));
		//后面几乎全部是&&操作，只要有一个false，那么该Event事件就无法处理
		
		
//        public static final String PC_WEBSITE_SDK = "website";
//        public static final String JAVA_SERVER_SDK = "java_server";
		switch (clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)) {
		case EventLogConstants.PlatformNameConstants.JAVA_SERVER_SDK:
			//Java Server发来的数据
			//判断会员ID是否存在
			result = result && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID));
			switch (event) {
			case CHARGEREFUND:
				//退款事件
				break;
			case CHARGESUCCESS:
				//订单支付成功
				result = result && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_ID));
				break;
			default:
				logger.debug("无法处理指定事件：" + clientInfo);
				result = false;
				break;
			}
			break;
		case EventLogConstants.PlatformNameConstants.PC_WEBSITE_SDK:
			//WebSite发来的数据
			switch (event) {
			case CHARGEREQUEST:
				//下单
				result = result
					&& StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_ID))
//					&& StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL))
					&& StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_CURRENCY_TYPE))
					&& StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_PAYMENT_TYPE))
					&& StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_CURRENCY_AMOUNT));
				break;
			case EVENT:
				//Event事件
				result = result
					&& StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_CATEGORY))
					&& StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_ACTION));
				break;
			case LAUNCH:
				//Launch访问事件
				
				break;
			case PAGEVIEW:
				//PV事件
				result = result
				&& StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL));
				break;
			default:
				logger.debug("无法处理指定事件：" + clientInfo);
				result = false;
				break;
			}
			break;
		default:
			result = false;
			logger.debug("无法确定的数据来源：" + clientInfo);
			break;
		}
				
				
		
		return result;
	}
	
	//7，8、输出事件到HBase
	public void outPutData (Map<String, String> clientInfo, Context context) throws IOException, InterruptedException{
		String uuid = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
		long serverTime = Long.valueOf(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME));
		
		//因为浏览器信息已经解析完成，所以此时删除原始的浏览器信息
		clientInfo.remove(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);
		
		//创建rowKey
		byte[] rowkey = generateRowKey(uuid, serverTime, clientInfo);
		Put put = new Put(rowkey);
		
		for(Map.Entry<String, String> entry : clientInfo.entrySet()){
			if(StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue())){
				put.add(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
			}
		}
		
		context.write(NullWritable.get(), put);
	}
	
	//9、用为向HBase中写入数据依赖Put对象，Put对象的创建依赖RowKey，所以如下方法
	/**
	 * crc32
	 * 1、uuid
	 * 2、clientInfo
	 * 3、时间timeBytes + 前两步的内容
	 * @return
	 */
	public byte[] generateRowKey(String uuid, long serverTime, Map<String, String> clientInfo){
		//清空crc32集合中的数据内容
		crc32.reset();
		
		if(StringUtils.isNotBlank(uuid)){
			this.crc32.update(Bytes.toBytes(uuid));
		}
		
		this.crc32.update(Bytes.toBytes(clientInfo.hashCode()));
		
		//当前数据访问服务器的时间-当天00:00点的时间戳  8位数字 -- 4字节
		byte[] timeBytes = Bytes.toBytes(serverTime - this.currentDayInMills);
		byte[] uuidAndMapDataBytes = Bytes.toBytes(this.crc32.getValue());
		
		//综合字节数组
		byte[] buffer = new byte[timeBytes.length + uuidAndMapDataBytes.length];
		
		//数组合并
		System.arraycopy(timeBytes, 0, buffer, 0, timeBytes.length);
		System.arraycopy(uuidAndMapDataBytes, 0, buffer, timeBytes.length, uuidAndMapDataBytes.length);
		return buffer;
	}
}
