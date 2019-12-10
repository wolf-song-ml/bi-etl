package com.z.etl.udf;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.z.etl.common.DateEnum;
import com.z.etl.converter.IDimensionConverter;
import com.z.etl.converter.impl.DimensionConverterImpl;
import com.z.etl.dimension.key.base.DateDimension;
import com.z.etl.util.TimeUtil;

/**
 * 根据给定的时间值返回对应的时间维度id
 */
public class DateDimensionConverterUDF extends UDF {
	// 用于根据维度值获取维度id的对象
	private IDimensionConverter converter = null;
	
	private Configuration conf;

	/**
	 * 默认无参构造方法，必须给定的
	 */
	public DateDimensionConverterUDF() {
		// 初始化操作
		this.converter = new DimensionConverterImpl();
		// 初始化运行环境
		conf = new Configuration();
        conf.addResource("output-collector.xml");
        conf.addResource("query-mapping.xml");
        conf.addResource("etl-env.xml");
	}

	/**
	 * 如果给定的参数不在处理范围，那么直接抛出异常
	 * 
	 * @param date   字符串类型的时间值，例如：2015-12-20
	 * @param type   需要的时间维度所属类型，参数可选为： （year、month、week、day、season）
	 * @return 对应的维度id值
	 * @throws IOException  根据维度对象获取维度id值的时候产生的异常
	 */
	public int evaluate(String date, String type) throws IOException {
		// 1. 参数过滤，时间格式不正确的数据直接过滤
		if (StringUtils.isBlank(date) || StringUtils.isBlank(type)) {
			throw new IllegalArgumentException("参数异常，date和type参数不能为空,date=" + date + ", type =" + type);
		}
		if (!TimeUtil.isValidateRunningDate(date)) {
			// 不是一个有效的输入时间
			throw new IllegalArgumentException("参数异常，date参数格式要求为:yyyy-MM-dd，当前值为:" + date);
		}
		// 2. 根据给定的type值来构建DateEnum对象
		DateEnum dateEnum = DateEnum.valueOfName(type); // 根据名称获取对应的值，如果没有返回的是null
		if (dateEnum == null) {
			// 给定的type值异常，无法转换为DateEnum枚举对象
			throw new IllegalArgumentException("参数异常，type参数只能是[year、month、week、day、season]其中的一个，当前值为:" + type);
		}
		// 3. 创建DateDimension维度对象
		DateDimension dateDimension = DateDimension.buildDate(TimeUtil.parseString2Long(date), dateEnum);
		// 4. 获取id的值
		return this.converter.getDimensionIdByValue(dateDimension, conf);
	}
}
