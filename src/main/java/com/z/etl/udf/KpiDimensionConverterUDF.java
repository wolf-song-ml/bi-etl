package com.z.etl.udf;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.z.etl.converter.IDimensionConverter;
import com.z.etl.converter.impl.DimensionConverterImpl;
import com.z.etl.dimension.key.base.KpiDimension;

/**
 * 用于根据kpi名称获取kpi维度id
 */
public class KpiDimensionConverterUDF extends UDF {
	// 用于根据维度值获取维度id的对象
	private IDimensionConverter converter = null;
	
	Configuration conf;

	/**
	 * 默认无参构造方法，必须给定的
	 */
	public KpiDimensionConverterUDF() {
		// 初始化操作
		this.converter = new DimensionConverterImpl();

		// 初始化运行环境
		conf = new Configuration();
		conf.addResource("output-collector.xml");
		conf.addResource("query-mapping.xml");
		conf.addResource("etl-env.xml");
	}

	/**
	 * @param kpiName
	 * @return
	 * @throws IOException
	 */
	public int evaluate(String kpiName) throws IOException {
		// 1. 判断参数是否为空
		if (StringUtils.isBlank(kpiName)) {
			throw new IllegalArgumentException("参数异常，kpiName不能为空!!!");
		}
		// 2. 构建kpi对象
		KpiDimension kpi = new KpiDimension(kpiName);
		// 3. 获取id的值
		return this.converter.getDimensionIdByValue(kpi, conf);
	}
}
