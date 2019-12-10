package com.z.etl;

import java.util.Map;
import java.util.Optional;
import java.util.zip.CRC32;

import com.z.etl.util.LoggerUtil;

public class LoggerUtilTest {

	public static void main(String[] args) {
		String infoString = "114.92.217.149^A1450569601.351^A/what.png?u_nu=1&u_sd=6D4F89C0-E17B-45D0-BFE0-059644C1878D&c_time=1450569596991&ver=1&en=e_l&pl=website&sdk=js&b_rst=1440*900&u_ud=4B16B8BB-D6AA-4118-87F8-C58680D22657&b_iev=Mozilla%2F5.0%20(Windows%20NT%205.1)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F45.0.2454.101%20Safari%2F537.36&l=zh-CN&bf_sid=33cbf257-3b11-4abd-ac70-c5fc47afb797_11177014";
		String infoString2 = "192.168.122.20^A1502615208.603^A/what.png?ver=1&u_mid=itguigu447&c_time=1502615208603&en=e_cs&oid=orderid86069&sdk=jdk&pl=java_server";
		Map<String, String> clientInfo = LoggerUtil.handleLogText(infoString);
		System.out.println(clientInfo);
		byte b[]={1,2,3,4,5};//数据的数组
		
		CRC32 c=new CRC32();
		
		c.update(b);
		
		System.out.print(c.getValue());//打印crc32的校验值
		
	}

}
