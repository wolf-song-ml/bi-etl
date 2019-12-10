package com.z.etl.dimension.value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import com.z.etl.common.KpiType;

/**
 * 填充Map对象的输出对象
 * 
 */
public class MapWritableValue extends BaseStatsValueWritable {
    /**
     * Map Writable对象
     */
    private MapWritable value = new MapWritable();
    /**
     * kpi 类型
     */
    private KpiType kpi;

    /**
     * 无参默认构造方法，必须给定
     */
    public MapWritableValue() {
        super();
    }

    /**
     * 给定全部参数的构造方法
     * 
     * @param value
     * @param kpi
     */
    public MapWritableValue(MapWritable value, KpiType kpi) {
        super();
        this.value = value;
        this.kpi = kpi;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

   

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		 this.value.write(out);
	        // 枚举类型序列化
	        WritableUtils.writeEnum(out, this.kpi);
		
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.value.readFields(in);
        // 枚举类型反序列化
        this.kpi = WritableUtils.readEnum(in, KpiType.class);
		
	}
}
