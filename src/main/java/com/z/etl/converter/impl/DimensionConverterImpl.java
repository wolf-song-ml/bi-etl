package com.z.etl.converter.impl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.z.etl.common.GlobalConstants;
import com.z.etl.converter.IDimensionConverter;
import com.z.etl.dimension.key.BaseDimension;
import com.z.etl.dimension.key.base.BrowserDimension;
import com.z.etl.dimension.key.base.DateDimension;
import com.z.etl.dimension.key.base.KpiDimension;
import com.z.etl.dimension.key.base.PlatformDimension;
import com.z.etl.util.JDBCManager;

/**
 * mysql根据维度值获取维度id的具体实现类
 * 
 * @author wolf
 * 
 */
public class DimensionConverterImpl implements IDimensionConverter {
	private static final Logger logger = LoggerFactory.getLogger(DimensionConverterImpl.class);

	/**
	 * 维度数据缓存器
	 */
	private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
		private static final long serialVersionUID = -3084359201061689731L;

		@Override
		protected boolean removeEldestEntry(Entry<String, Integer> eldest) {
			// 缓存容量， 如果这里返回true，那么删除最早加入的数据
			// 当集合的大小大于5000的情况下 ，返回true，删除最早加入的数据
			return this.size() > 5000;
		}
	};

	/**
	 * 构造函数，默认无参构造函数
	 */
	public DimensionConverterImpl() {
		// 添加关闭的钩子，jvm关闭时，会触发该线程执行数据库链接关闭操作
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				logger.info("开始关闭数据库......");
				JDBCManager.closeConnection(JDBCManager.localConn.get(), null, null);
				logger.info("关闭数据库成功！");
			}
		}));
	}

	/**
	 * 获取方式<br/>
	 * 1. 判断缓存中是否存储对于维度的维度id<br/>
	 * 2. 如果缓存中存在，那么直接返回；否则进行下一步<br/>
	 * 3. 查询数据库中是否存在对应的数据库维度id的记录，如果存在，直接返回id值；如果不存在，进行下一步<br/>
	 * 4. 将维度数据插入到关系型数据库中<br/>
	 * 5. 再重新获取维度id的值，此时维度id的值一定存在
	 */
	@Override
	public int getDimensionIdByValue(BaseDimension value, Configuration conf) throws IOException {
		// 1. 创建cache key(缓存key)
		String cacheKey = buildCacheKey(value); // 获取cache
		// 2. 判断缓存中是否存在对于key的数据
		if (this.cache.containsKey(cacheKey)) {
			// 3. 缓存中存在对应数据，直接返回结果值
			return this.cache.get(cacheKey);
		}

		try {
			// 4. 创建sql语句数组，包括查询语句和插入数据语句，第一条为查询语句，第二条为插入数据语句
			String[] sql = null;
			if (value instanceof DateDimension) {
				sql = this.buildDateSql();
			} else if (value instanceof PlatformDimension) {
				sql = this.buildPlatformSql();
			} else if (value instanceof BrowserDimension) {
				sql = this.buildBrowserSql();
			} else if (value instanceof KpiDimension) {
				sql = this.buildKpiSql();
			} else {
				throw new IOException("不支持此dimensionid的获取:" + value.getClass());
			}

			// 5. 获取数据库连接
			Connection conn = JDBCManager.getConnectionLocal(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
			int id = 0;
			// 6. 并发控制，每次只允许一个维度进行id获取操作
//			synchronized (this) {
			id = this.executeSql(conn, cacheKey, sql, value);
			// 7. 返回结果
			return id;
		} catch (Throwable e) {
			logger.error("操作数据库出现异常", e);
			throw new IOException(e);
		}
	}

	/**
	 * 创建cache key
	 * 
	 * @param dimension
	 * @return
	 */
	public static String buildCacheKey(BaseDimension dimension) {
		StringBuilder sb = new StringBuilder();
		// 1. 根据不同数据类型创建对于的cache key值
		if (dimension instanceof DateDimension) {
			sb.append("date_dimension");
			DateDimension date = (DateDimension) dimension;
			sb.append(date.getYear()).append(date.getSeason()).append(date.getMonth());
			sb.append(date.getWeek()).append(date.getDay()).append(date.getType());
		} else if (dimension instanceof PlatformDimension) {
			sb.append("platform_dimension");
			PlatformDimension platform = (PlatformDimension) dimension;
			sb.append(platform.getPlatformName()).append(platform.getPlatformVersion());
		} else if (dimension instanceof BrowserDimension) {
			sb.append("browser_dimension");
			BrowserDimension browser = (BrowserDimension) dimension;
			sb.append(browser.getBrowser()).append(browser.getBrowserVersion());
		} else if (dimension instanceof KpiDimension) {
			sb.append("kpi_dimension");
			KpiDimension kpiDimension = (KpiDimension) dimension;
			sb.append(kpiDimension.getKpiName());
		}

		// 2. 如果cache值为空，那么直接抛出异常
		if (sb.length() == 0) {
			throw new RuntimeException("无法创建指定dimension的cachekey：" + dimension.getClass());
		}
		// 3. 返回cache key值
		return sb.toString();
	}

	
	/**
	 * 具体执行sql的方法<br/>
	 * 执行逻辑：<br/>
	 * 1. 查询数据库中是否存在对于维度的维度id<br/>
	 * 2. 存在则直接返回结果，不存在进行下一步<br/>
	 * 3. 将维度信息插入到数据库中<br/>
	 * 4. 重新获取维度id的值
	 * 
	 * @param conn      数据库连接信息
	 * @param cacheKey  缓存key
	 * @param sqls      要执行的sql语句数组
	 * @param dimension 维度对象
	 * @return 维度对象对应的维度id
	 * @throws SQLException
	 */
	private int executeSql(Connection conn, String cacheKey, String[] sqls, BaseDimension dimension)
			throws SQLException {
		// 发送sql语句的对象
		PreparedStatement pstmt = null;
		// sql执行结果对象
		ResultSet rs = null;

		try {
			// 1. 开始查询操作
			pstmt = conn.prepareStatement(sqls[0]); // 创建查询sql的pstmt对象
			// 设置参数
			this.setArgs(pstmt, dimension);
			// 执行查询操作
			rs = pstmt.executeQuery();
			if (rs.next()) {
				this.cache.put(cacheKey, rs.getInt(1));
				return rs.getInt(1); 
			}
			// 关闭连接
			JDBCManager.closeConnection(null, pstmt, rs);

			// 2. 插入数据操作
			// 代码运行到这儿，表示该dimension在数据库中不存在，进行插入
			pstmt = conn.prepareStatement(sqls[1]);
			// 设置参数
			this.setArgs(pstmt, dimension);
			// 执行更新操作
			pstmt.executeUpdate();
			// 关闭连接
			JDBCManager.closeConnection(null, pstmt, rs);

			// 3. 重新获取维度id的值
			pstmt = conn.prepareStatement(sqls[0]); // 创建查询sql的pstmt对象
			// 设置参数
			this.setArgs(pstmt, dimension);
			// 执行查询操作
			rs = pstmt.executeQuery();
			if (rs.next()) {
				this.cache.put(cacheKey, rs.getInt(1));
				return rs.getInt(1); 
			}
		} finally {
			// 关闭连接
			JDBCManager.closeConnection(null, pstmt, rs);
		}
		throw new RuntimeException("从数据库获取id失败");
	}

	/**
	 * 设置参数
	 * 
	 * @param pstmt
	 * @param dimension
	 * @throws SQLException
	 */
	private void setArgs(PreparedStatement pstmt, BaseDimension dimension) throws SQLException {
		int i = 0;
		if (dimension instanceof DateDimension) {
			DateDimension date = (DateDimension) dimension;
			pstmt.setInt(++i, date.getYear());
			pstmt.setInt(++i, date.getSeason());
			pstmt.setInt(++i, date.getMonth());
			pstmt.setInt(++i, date.getWeek());
			pstmt.setInt(++i, date.getDay());
			pstmt.setString(++i, date.getType());
			pstmt.setDate(++i, new Date(date.getCalendar().getTime()));
		} else if (dimension instanceof PlatformDimension) {
			PlatformDimension platform = (PlatformDimension) dimension;
			pstmt.setString(++i, platform.getPlatformName());
			pstmt.setString(++i, platform.getPlatformVersion());
		} else if (dimension instanceof BrowserDimension) {
			BrowserDimension browser = (BrowserDimension) dimension;
			pstmt.setString(++i, browser.getBrowser());
			pstmt.setString(++i, browser.getBrowserVersion());
		} else if (dimension instanceof KpiDimension) {
			KpiDimension kpi = (KpiDimension) dimension;
			pstmt.setString(++i, kpi.getKpiName());
		}
	}

	/**
	 * 创建date dimension相关sql
	 * 
	 * @return
	 */
	private String[] buildDateSql() {
		String querySql = "SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ? order by `id`";
		String insertSql = "INSERT INTO `dimension_date`(`year`, `season`, `month`, `week`, `day`, `type`, `calendar`) VALUES(?, ?, ?, ?, ?, ?, ?)";
		return new String[] { querySql, insertSql };
	}

	/**
	 * 创建polatform dimension相关sql
	 * 
	 * @return
	 */
	private String[] buildPlatformSql() {
		String querySql = "SELECT `id` FROM `dimension_platform` WHERE `platform_name` = ? AND `platform_version` = ? order by `id`";
		String insertSql = "INSERT INTO `dimension_platform`(`platform_name`, `platform_version`) VALUES(?, ?)";
		return new String[] { querySql, insertSql };
	}

	/**
	 * 创建browser dimension相关sql
	 * 
	 * @return
	 */
	private String[] buildBrowserSql() {
		String querySql = "SELECT `id` FROM `dimension_browser` WHERE `browser_name` = ? AND `browser_version` = ? order by `id`";
		String insertSql = "INSERT INTO `dimension_browser`(`browser_name`, `browser_version`) VALUES(?, ?)";
		return new String[] { querySql, insertSql };
	}

	/**
	 * 创建kpi dimension相关sql
	 * 
	 * @return
	 */
	private String[] buildKpiSql() {
		String querySql = "SELECT `id` FROM `dimension_kpi` WHERE `kpi_name` = ? order by `id`";
		String insertSql = "INSERT INTO `dimension_kpi`(`kpi_name`) VALUES(?)";
		return new String[] { querySql, insertSql };
	}

	@Override
	public void close() throws IOException {
		logger.debug("进行关闭资源操作....");
		JDBCManager.closeConnection(JDBCManager.localConn.get(), null, null);
		logger.debug("关闭资源操作完成");
	}

}
