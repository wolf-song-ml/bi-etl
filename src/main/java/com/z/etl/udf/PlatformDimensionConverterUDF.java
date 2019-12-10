package com.z.etl.udf;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.z.etl.common.GlobalConstants;
import com.z.etl.converter.IDimensionConverter;
import com.z.etl.converter.impl.DimensionConverterImpl;
import com.z.etl.dimension.key.base.PlatformDimension;

/**
 * 自定义根据平台维度信息获取维度id 自定义udf的时候，如果使用到FileSystem（HDFS的api），记住一定不要调用close方法
 */
public class PlatformDimensionConverterUDF extends UDF {
	// 用于根据维度值获取维度id的对象
	private IDimensionConverter converter = null;
	
	Configuration conf;

	/**
	 * 默认无参构造方法，必须给定的
	 */
	public PlatformDimensionConverterUDF() {
		// 初始化操作
		this.converter = new DimensionConverterImpl();

		// 初始化运行环境
		conf = new Configuration();
		conf.addResource("output-collector.xml");
		conf.addResource("query-mapping.xml");
		conf.addResource("etl-env.xml");
	}

	/**
	 * 根据给定的平台维度名称和平台维度版本获取对应的维度id值
	 * 
	 * @param platformName    维度名称
	 * @param platformVersion 维度版本
	 * @return
	 * @throws IOException 获取id的时候产生的异常
	 */
	public int evaluate(String platformName, String platformVersion) throws IOException {
		// 1. 要求参数不能为空，当为空的时候，设置为unknown默认值
		if (StringUtils.isBlank(platformName) || StringUtils.isBlank(platformVersion)) {
			platformName = GlobalConstants.DEFAULT_VALUE;
			platformVersion = GlobalConstants.DEFAULT_VALUE;
		}
		// 2. 构建一个对象
		PlatformDimension pf = new PlatformDimension(platformName, platformVersion);
		// 3. 获取维度id值，使用写好的DimensionConverterImpl类解析
		return this.converter.getDimensionIdByValue(pf, conf);
	}
	
}
