package com.z.etl.dimension.key.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.z.etl.common.GlobalConstants;
import com.z.etl.dimension.key.BaseDimension;

/**
 * 平台维度（平台名称，平台版本）
 * 
 */
public class PlatformDimension extends BaseDimension {
	/**
	 * 数据库主键
	 */
	private int id;
	/**
	 * 平台名称
	 */
	private String platformName;
	/**
	 * 平台版本号
	 */
	private String platformVersion;

	/**
	 * 无参构造函数，必须给定
	 */
	public PlatformDimension() {
		this(GlobalConstants.DEFAULT_VALUE, GlobalConstants.DEFAULT_VALUE);
	}

	/**
	 * 给定平台信息的构造函数
	 * 
	 * @param platformName
	 *            名称
	 * @param platformVersion
	 *            版本号
	 */
	public PlatformDimension(String platformName, String platformVersion) {
		super();
		this.platformName = platformName;
		this.platformVersion = platformVersion;
	}

	/**
	 * 给定全部参数的构造函数
	 * 
	 * @param id
	 *            主键id
	 * @param platformName
	 *            平台名称
	 * @param platformVersion
	 *            平台版本号
	 */
	public PlatformDimension(int id, String platformName, String platformVersion) {
		super();
		this.id = id;
		this.platformName = platformName;
		this.platformVersion = platformVersion;
	}

	/**
	 * 根据给定的参数值，构建多个不同维度的平台维度对象
	 * 
	 * @param platformName
	 *            平台名称
	 * @param platformVersion
	 *            平台版本(其实就是sdk版本号)
	 * @return
	 */
	public static List<PlatformDimension> buildList(String platformName, String platformVersion) {
		/**
		 * 如果给定的参数平台名称为空，那么将平台名称和平台版本号全部设置为unknown
		 */
		if (StringUtils.isBlank(platformName)) {
			platformName = GlobalConstants.DEFAULT_VALUE;
			platformVersion = GlobalConstants.DEFAULT_VALUE;
		}

		/**
		 * 如果给定的平台版本号为空，那么版本号设置为unknown
		 */
		if (StringUtils.isBlank(platformVersion)) {
			platformVersion = GlobalConstants.DEFAULT_VALUE;
		}

		// 开始构建平台维度
		List<PlatformDimension> pds = new ArrayList<PlatformDimension>();
		// (name,version)
		pds.add(new PlatformDimension(platformName, platformVersion));
		// (name,all)
		pds.add(new PlatformDimension(platformName,	GlobalConstants.VALUE_OF_ALL));
		// (all,all)
		pds.add(new PlatformDimension(GlobalConstants.VALUE_OF_ALL,	GlobalConstants.VALUE_OF_ALL));
		return pds;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getPlatformName() {
		return platformName;
	}

	public void setPlatformName(String platformName) {
		this.platformName = platformName;
	}

	public String getPlatformVersion() {
		return platformVersion;
	}

	public void setPlatformVersion(String platformVersion) {
		this.platformVersion = platformVersion;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result
				+ ((platformName == null) ? 0 : platformName.hashCode());
		result = prime * result
				+ ((platformVersion == null) ? 0 : platformVersion.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PlatformDimension other = (PlatformDimension) obj;
		if (id != other.id)
			return false;
		if (platformName == null) {
			if (other.platformName != null)
				return false;
		} else if (!platformName.equals(other.platformName))
			return false;
		if (platformVersion == null) {
			if (other.platformVersion != null)
				return false;
		} else if (!platformVersion.equals(other.platformVersion))
			return false;
		return true;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(platformName);
		out.writeUTF(platformVersion);

	}

	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.platformName = in.readUTF();
		this.platformVersion = in.readUTF();

	}

	public int compareTo(BaseDimension o) {
		if (this == o)
			return 0;
		PlatformDimension pfd = (PlatformDimension) o;
		int result = Integer.compare(id, pfd.getId());
		if (0 != result) {
			return result;
		}
		result = this.platformName.compareTo(pfd.getPlatformName());
		if (0 != result) {
			return result;
		}
		return this.platformVersion.compareTo(pfd.getPlatformVersion());
	}
}
