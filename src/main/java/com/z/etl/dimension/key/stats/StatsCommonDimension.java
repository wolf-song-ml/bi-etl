package com.z.etl.dimension.key.stats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.z.etl.dimension.key.BaseDimension;
import com.z.etl.dimension.key.base.DateDimension;
import com.z.etl.dimension.key.base.KpiDimension;
import com.z.etl.dimension.key.base.PlatformDimension;

/**
 * 公用的dimension信息组合
 * 
 * @author wolf
 *
 */
public class StatsCommonDimension extends StatsDimension {
    /**
     * 日期维度
     */
    private DateDimension date = new DateDimension();
    /**
     * 平台维度
     */
    private PlatformDimension platform = new PlatformDimension();
    /**
     * kpi维度
     */
    private KpiDimension kpi = new KpiDimension();

    /**
     * 根据一个已有的对象clone一个对象出来
     * 
     * @param dimension
     * @return
     */
    public static StatsCommonDimension clone(StatsCommonDimension dimension) {
        /**
         * 创建一个新的时间维度对象
         */
        DateDimension date = new DateDimension(dimension.date.getId(), dimension.date.getYear(),
                dimension.date.getSeason(), dimension.date.getMonth(), dimension.date.getWeek(),
                dimension.date.getDay(), dimension.date.getType(), dimension.date.getCalendar());
        /**
         * 创建一个新的平台维度对象
         */
        PlatformDimension platform = new PlatformDimension(dimension.platform.getId(),
                dimension.platform.getPlatformName(), dimension.platform.getPlatformVersion());
        /**
         * 创建一个新的kpi维度对象
         */
        KpiDimension kpi = new KpiDimension(dimension.kpi.getId(), dimension.kpi.getKpiName());

        // 创建一个新的组合维度对象
        return new StatsCommonDimension(date, platform, kpi);
    }

    /**
     * 无参构造方法，必须给定
     */
    public StatsCommonDimension() {
        super();
    }

    /**
     * 给定全部子维度的构造函数
     * 
     * @param date
     *            日期维度
     * @param platform
     *            平台维度
     * @param kpi
     *            kpi维度
     */
    public StatsCommonDimension(DateDimension date, PlatformDimension platform, KpiDimension kpi) {
        super();
        this.date = date;
        this.platform = platform;
        this.kpi = kpi;
    }

    public DateDimension getDate() {
        return date;
    }

    public void setDate(DateDimension date) {
        this.date = date;
    }

    public PlatformDimension getPlatform() {
        return platform;
    }

    public void setPlatform(PlatformDimension platform) {
        this.platform = platform;
    }

    public KpiDimension getKpi() {
        return kpi;
    }

    public void setKpi(KpiDimension kpi) {
        this.kpi = kpi;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((date == null) ? 0 : date.hashCode());
        result = prime * result + ((kpi == null) ? 0 : kpi.hashCode());
        result = prime * result + ((platform == null) ? 0 : platform.hashCode());
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
        StatsCommonDimension other = (StatsCommonDimension) obj;
        if (date == null) {
            if (other.date != null)
                return false;
        } else if (!date.equals(other.date))
            return false;
        if (kpi == null) {
            if (other.kpi != null)
                return false;
        } else if (!kpi.equals(other.kpi))
            return false;
        if (platform == null) {
            if (other.platform != null)
                return false;
        } else if (!platform.equals(other.platform))
            return false;
        return true;
    }

	public void write(DataOutput out) throws IOException {
		this.date.write(out);
        this.platform.write(out);
        this.kpi.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.date.readFields(in);
        this.platform.readFields(in);
        this.kpi.readFields(in);
	}

	public int compareTo(BaseDimension o) {
		if (this == o) {
            return 0;
        }

        StatsCommonDimension other = (StatsCommonDimension) o;
        int tmp = this.date.compareTo(other.date);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.platform.compareTo(other.platform);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.kpi.compareTo(other.kpi);
        return tmp;
	}
}
