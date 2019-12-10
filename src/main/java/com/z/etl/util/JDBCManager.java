package com.z.etl.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.z.etl.common.GlobalConstants;

/**
 * JDBC管理工具
 * 
 * @author wolf
 *
 */
public class JDBCManager {
	private static final Logger logger = Logger.getLogger(JDBCManager.class);
	
	/**
	 * 线程局部缓存器，没有线程缓存一个属于当前线程的数据库连接
	 */
	public static ThreadLocal<Connection> localConn = new ThreadLocal<Connection>();

	/**
	 * 根据配置获取获取关系型数据库的jdbc连接
	 * 
	 * @param conf hadoop配置信息
	 * @param flag 区分不同数据源的标志位
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection(Configuration conf, String flag) throws SQLException {
		// 转换成为正常的字符串，比如mysql.report.driver之类的，当flag的值为"report"的时候
		String driverStr = String.format(GlobalConstants.JDBC_DRIVER, flag);

		String urlStr = String.format(GlobalConstants.JDBC_URL, flag);
		String usernameStr = String.format(GlobalConstants.JDBC_USERNAME, flag);
		String passwordStr = String.format(GlobalConstants.JDBC_PASSWORD, flag);

		// 从configuration中获取我们配置的jdbc参数
		String driverClass = conf.get(driverStr).trim();
		String url = conf.get(urlStr).trim();
		String username = conf.get(usernameStr).trim();
		String password = conf.get(passwordStr).trim();
		try {
			Class.forName(driverClass); // 加载class
		} catch (ClassNotFoundException e) {
		}
		// 获取jdbc连接
		return DriverManager.getConnection(url, username, password);
	}

	/**
	 * 获取数据库连接 如果在当前线程的缓存中没有找到对于的数据库连接，那么进行新建操作
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnectionLocal(Configuration conf, String flag) throws SQLException {
		// 从缓存中获取对应的数据库连接值
		Connection conn = localConn.get();
		if (conn == null || conn.isClosed() || !conn.isValid(3)) {
			// 转换成为正常的字符串，比如mysql.report.driver之类的，当flag的值为"report"的时候
			String driverStr = String.format(GlobalConstants.JDBC_DRIVER, flag);
			String urlStr = String.format(GlobalConstants.JDBC_URL, flag);
			String usernameStr = String.format(GlobalConstants.JDBC_USERNAME, flag);
			String passwordStr = String.format(GlobalConstants.JDBC_PASSWORD, flag);

			// 从configuration中获取我们配置的jdbc参数
			String driverClass = conf.get(driverStr).trim();
			String url = conf.get(urlStr).trim();
			String username = conf.get(usernameStr).trim();
			String password = conf.get(passwordStr).trim();
			
			try {
				Class.forName(driverClass); 
				conn = DriverManager.getConnection(url, username, password);
				localConn.set(conn);
			} catch (ClassNotFoundException e) {
//				JDBCManager.closeConnection(conn, null, null);
				logger.error(e.getMessage());
			}
		}
		return conn;
	}

	/**
	 * 关闭数据库连接
	 * 
	 * @param conn
	 * @param stmt
	 * @param rs
	 */
	public static void closeConnection(Connection conn, Statement stmt, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				// nothigns
			}
		}

		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
				// nothings
			}
		}

		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				// nothings
			}
		}
	}

}
