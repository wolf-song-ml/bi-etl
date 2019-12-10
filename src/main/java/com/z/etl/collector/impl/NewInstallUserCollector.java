package com.z.etl.collector.impl;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import com.z.etl.collector.ICollector;
import com.z.etl.common.GlobalConstants;
import com.z.etl.converter.IDimensionConverter;
import com.z.etl.dimension.key.BaseDimension;
import com.z.etl.dimension.key.stats.StatsUserDimension;
import com.z.etl.dimension.value.BaseStatsValueWritable;
import com.z.etl.dimension.value.MapWritableValue;

/**
 * 设置sql参数，对应于new install user kpi的计算 
 * 
 * @author wolf
 * 
 */
public class NewInstallUserCollector implements ICollector {

	// java8工厂模式反射没有这个也不报错
	public NewInstallUserCollector() {
		super();
	}

	@Override
	public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt,
			IDimensionConverter converter) throws IOException {
		// 1. 强转key/value键值对
		StatsUserDimension statsUserDimension = (StatsUserDimension) key;
		MapWritableValue mapWritableValue = (MapWritableValue) value;
		int newInstallUsers = ((IntWritable) mapWritableValue.getValue().get(new IntWritable(-1))).get();

		try {
			// 2. 构建/设置参数
			int i = 0;
			// 需要将维度的信息转换为维度id
			pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform(), conf));
			pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate(), conf));
			pstmt.setInt(++i, newInstallUsers);
			pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
			pstmt.setInt(++i, newInstallUsers);
			// 3. 添加到批量执行中
			pstmt.addBatch();
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

}

