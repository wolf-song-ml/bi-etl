package com.z.etl.dimension.key.stats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.z.etl.dimension.key.BaseDimension;
import com.z.etl.dimension.key.base.BrowserDimension;

/**
 * 进行用户分析(用户基本分析和浏览器分析)定义的组合维度
 * 
 * @author wolf
 *
 */
public class StatsUserDimension extends StatsDimension {
    /**
     * 公用维度对象
     */
    private StatsCommonDimension statsCommon = new StatsCommonDimension();
    /**
     * 浏览器维度对象
     */
    private BrowserDimension browser = new BrowserDimension();

    /**
     * 依照一个已有对象克隆出一个新的组合维度对象
     * 
     * @param dimension
     * @return
     */
    public static StatsUserDimension clone(StatsUserDimension dimension) {
        /**
         * 创建一个浏览器维度对象
         */
        BrowserDimension browser = new BrowserDimension(dimension.browser.getBrowser(),
                dimension.browser.getBrowserVersion());

        /**
         * clone一个新的StatsCommonDimension组合类对象
         */
        StatsCommonDimension statsCommon = StatsCommonDimension.clone(dimension.statsCommon);

        // 新建对象
        return new StatsUserDimension(statsCommon, browser);
    }

    /**
     * 无参构造方法，必须给定
     */
    public StatsUserDimension() {
        super();
    }

    /**
     * 给定全部参数的构造方法
     * 
     * @param statsCommon
     *            stats基本组合维度
     * @param browser
     *            浏览器维度
     */
    public StatsUserDimension(StatsCommonDimension statsCommon, BrowserDimension browser) {
        super();
        this.statsCommon = statsCommon;
        this.browser = browser;
    }

    public StatsCommonDimension getStatsCommon() {
        return statsCommon;
    }

    public void setStatsCommon(StatsCommonDimension statsCommon) {
        this.statsCommon = statsCommon;
    }

    public BrowserDimension getBrowser() {
        return browser;
    }

    public void setBrowser(BrowserDimension browser) {
        this.browser = browser;
    }



    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((browser == null) ? 0 : browser.hashCode());
        result = prime * result + ((statsCommon == null) ? 0 : statsCommon.hashCode());
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
        StatsUserDimension other = (StatsUserDimension) obj;
        if (browser == null) {
            if (other.browser != null)
                return false;
        } else if (!browser.equals(other.browser))
            return false;
        if (statsCommon == null) {
            if (other.statsCommon != null)
                return false;
        } else if (!statsCommon.equals(other.statsCommon))
            return false;
        return true;
    }

	public void write(DataOutput out) throws IOException {
		this.statsCommon.write(out);
        this.browser.write(out);		
	}

	public void readFields(DataInput in) throws IOException {
		this.statsCommon.readFields(in);
        this.browser.readFields(in);		
	}

	public int compareTo(BaseDimension o) {
		if (this == o) {
            return 0;
        }

        StatsUserDimension other = (StatsUserDimension) o;
        int tmp = this.statsCommon.compareTo(other.statsCommon);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.browser.compareTo(other.browser);
        return tmp;
	}
}
