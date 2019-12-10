package com.z.etl.dimension.key.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.z.etl.dimension.key.BaseDimension;

/**
 * kpi维度类
 * 
 * @author wolf
 * 
 */
public class KpiDimension extends BaseDimension {

	/**
	 * 数据库主键id
	 */
	private int id;
	/**
	 * kpi名称
	 */
	private String kpiName;

	/**
	 * 无参构造函数，必须给定
	 */
	public KpiDimension() {
		super();
	}

	/**
	 * 给定具体参数的构造函数
	 * 
	 * @param id
	 * @param kpiName
	 */
	public KpiDimension(int id, String kpiName) {
		super();
		this.id = id;
		this.kpiName = kpiName;
	}

	/**
	 * 给定kpi名称的构造函数
	 * 
	 * @param kpiName
	 *            kpi名称
	 */
	public KpiDimension(String kpiName) {
		super();
		this.kpiName = kpiName;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getKpiName() {
		return kpiName;
	}

	public void setKpiName(String kpiName) {
		this.kpiName = kpiName;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(kpiName);

	}

	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.kpiName = in.readUTF();

	}

	public int compareTo(BaseDimension o) {
		if (o == this)
			return 0;

		KpiDimension kd = (KpiDimension) o;

		int result = Integer.compare(this.id, kd.getId());
		if (result == 0) {
			return this.kpiName.compareTo(kd.getKpiName());
		}
		return result;
	}
}
