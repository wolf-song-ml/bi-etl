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
 * 浏览器维度（浏览器名称和浏览器版本号）
 * 
 * @author wolf
 *
 */
public class BrowserDimension extends BaseDimension {
    // 数据库主键id
    private int id;
    // 浏览器名称
    private String browser;
    // 浏览器版本号
    private String browserVersion;

    /**
     * 默认构造函数，必须给定
     */
    public BrowserDimension() {
        super();
    }

    /**
     * 给定浏览器信息的构造函数
     * 
     * @param browser
     *            浏览器名称
     * @param browserVersion
     *            浏览器版本
     */
    public BrowserDimension(String browser, String browserVersion) {
        super();
        this.browser = browser;
        this.browserVersion = browserVersion;
    }

    /**
     * 静态方法，根据给定的浏览器信息创建一个dim对象
     * 
     * @param browser
     *            浏览器版本名称
     * @param browserVersion
     *            浏览器版本号
     * @return
     */
    public static BrowserDimension newInstance(String browser, String browserVersion) {
        BrowserDimension bd = new BrowserDimension();
        bd.setBrowser(browser);
        bd.setBrowserVersion(browserVersion);
        return bd;
    }

    /**
     * 根据给定的浏览器信息构造多个浏览器维度的对象集合<br/>
     * 
     * 
     * @param browser
     *            给定的浏览器名称
     * @param browserVersion
     *            给定的浏览器版本号
     * @return 返回一个不同维度的浏览器对象集合
     */
    public static List<BrowserDimension> buildList(String browser, String browserVersion) {
        if (StringUtils.isBlank(browser)) {
            /**
             * 如果给定的参数browser为空，那么设置为默认值unknown
             */
            browser = GlobalConstants.DEFAULT_VALUE;
            browserVersion = GlobalConstants.DEFAULT_VALUE;
        }

        if (StringUtils.isBlank(browserVersion)) {
            /**
             * 如果给定的参数browserVersion为空，那么设置版本号为默认值unknown
             */
            browserVersion = GlobalConstants.DEFAULT_VALUE;
        }

        // 开始创建维度信息
        List<BrowserDimension> bds = new ArrayList<BrowserDimension>();
        // 创建一个:(browser,all)的维度信息
        bds.add(BrowserDimension.newInstance(browser, GlobalConstants.VALUE_OF_ALL));
        // 创建一个:(browser,version)的具体维度信息
        bds.add(BrowserDimension.newInstance(browser, browserVersion));
        return bds;
    }

   

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((browser == null) ? 0 : browser.hashCode());
        result = prime * result + ((browserVersion == null) ? 0 : browserVersion.hashCode());
        result = prime * result + id;
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
        BrowserDimension other = (BrowserDimension) obj;
        if (browser == null) {
            if (other.browser != null)
                return false;
        } else if (!browser.equals(other.browser))
            return false;
        if (browserVersion == null) {
            if (other.browserVersion != null)
                return false;
        } else if (!browserVersion.equals(other.browserVersion))
            return false;
        if (id != other.id)
            return false;
        return true;
    }

	public void write(DataOutput out) throws IOException {
		 out.writeInt(id);
	     out.writeUTF(browser);
	     out.writeUTF(browserVersion);
	}

	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
        this.browser = in.readUTF();
        this.browserVersion = in.readUTF();
		
	}

	public int compareTo(BaseDimension o) {
		if (o == this)
            return 0;
        BrowserDimension bd = (BrowserDimension) o;
        int result = Integer.compare(this.id, bd.getId());
        if (result == 0) {
            int result1 = this.browser.compareTo(bd.getBrowser());
            if (result1 == 0) {
                return this.browserVersion.compareTo(bd.getBrowserVersion());
            }
            return result1;
        }
        return result;
	}
}
