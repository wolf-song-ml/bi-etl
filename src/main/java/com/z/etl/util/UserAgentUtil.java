package com.z.etl.util;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;

/**
 * 解析浏览器信息，解析useragent信息<br/>
 * 根据第三方jar文件:uasparser.jar进行解析<br/>
 *
 */
public class UserAgentUtil {
	private static UASparser sparser = null;

	// 初始化
	static {
		try {
			sparser = new UASparser(OnlineUpdater.getVendoredInputStream());
		} catch (IOException e) {
			// nothings
		}
	}

	/**
	 * 解析浏览器的user agent字符串，返回useragentinfo对象<br/>
	 * 如果字符串为空，返回null，解析失败，也返回null
	 * 
	 * @param userAgent
	 * @return
	 */
	public static UserAgentInfo analyticUserAgent(String userAgent) {
		if (StringUtils.isBlank(userAgent)) {
			return null;
		}

		UserAgentInfo info = null;
		try {
			info = sparser.parse(userAgent);
		} catch (IOException e) {
			// nothing
		}

		return info;
	}
}
