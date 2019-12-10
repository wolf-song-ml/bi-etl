package com.z.etl.dimension.key.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import com.z.etl.common.DateEnum;
import com.z.etl.dimension.key.BaseDimension;
import com.z.etl.util.TimeUtil;

/**
 * 日期维度类
 * 
 * @author wolf
 *
 */
public class DateDimension extends BrowserDimension {

    /**
     * 数据库主键
     */
    private int id;
    /**
     * 年份
     */
    private int year;
    /**
     * 季度
     */
    private int season;
    /**
     * 月份
     */
    private int month;
    /**
     * 周
     */
    private int week;
    /**
     * 天
     */
    private int day;
    /**
     * 时间维度类型：year、season、month、week、day
     */
    private String type;
    /**
     * 具体日期
     */
    private Date calendar = new Date();

    /**
     * 根据给定的毫米级时间戳和需要创建的日期维度类型创建一个日期维度对象
     * 
     * @param time
     *            毫秒级时间戳
     * @param type
     *            需要创建的日期维度类型
     * @throws RuntimeException
     *             如果给定的type没法创建，那么直接抛出此异常
     * @return 返回一个对应的时间维度对象
     */
    public static DateDimension buildDate(long time, DateEnum type) {
        // 创建一个日历对象
        Calendar calendar = Calendar.getInstance();
        calendar.clear();

        /**
         * 获取给定时间戳中对应的年份
         */
        int year = TimeUtil.getDateInfo(time, DateEnum.YEAR);
        if (DateEnum.YEAR.equals(type)) {
            // 如果给定的是需要一个年份对应的时间维度对象
            calendar.set(year, 0, 1); // 设置日历为当年的第一天
            return new DateDimension(year, 0, 0, 0, 0, type.name, calendar.getTime());
        }

        /**
         * 获取给定时间戳对于的季度，取值范围:[1,4]
         */
        int season = TimeUtil.getDateInfo(time, DateEnum.SEASON);
        if (DateEnum.SEASON.equals(type)) {
            // 如果给定的参数是需要一个季度对于的时间维度对象
            int month = (3 * season - 2); // 计算当前季度的第一个月所属月份
            calendar.set(year, month - 1, 1); // 设置日历为当季度的第一天
            return new DateDimension(year, season, 0, 0, 0, type.name, calendar.getTime());
        }

        /**
         * 获取给定时间戳对于的月份，取值范围: [1,12]
         */
        int month = TimeUtil.getDateInfo(time, DateEnum.MONTH);
        if (DateEnum.MONTH.equals(type)) {
            // 如果给定的参数是需要一个月份对于的时间维度对象
            calendar.set(year, month - 1, 1); // 设置日历为当月的第一天
            return new DateDimension(year, season, month, 0, 0, type.name, calendar.getTime());
        }

        /**
         * 获取给定时间戳对应的周数，取值范围：[1,53]
         */
        int week = TimeUtil.getDateInfo(time, DateEnum.WEEK);
        if (DateEnum.WEEK.equals(type)) {
            // 如果给定的参数是需要一个对应周数的时间维度对象
            long firstDayOfWeek = TimeUtil.getFirstDayOfThisWeek(time); // 获取给定时间戳所属周的第一天
            /**
             * 为了解决跨年时间问题，需要重新获取年份、季度、月份、周
             */
            year = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.YEAR);
            season = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.SEASON);
            month = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.MONTH);
            week = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.WEEK);
            if (month == 12 && week == 1) {
                // 如果是第12月，而且week为1，那么直接设置为53周
                week = 53;
            }
            return new DateDimension(year, season, month, week, 0, type.name,
                    new Date(firstDayOfWeek));
        }

        /**
         * 获取给定时间戳对应的天，取值范围: [1,31]
         */
        int day = TimeUtil.getDateInfo(time, DateEnum.DAY);
        if (DateEnum.DAY.equals(type)) {
            calendar.set(year, month - 1, day); // 设置日历为当天
            if (month == 12 && week == 1) {
                // 如果是第12月，而且week为1，那么直接设置为53周
                week = 53;
            }
            return new DateDimension(year, season, month, week, day, type.name, calendar.getTime());
        }

        /**
         * 其他类型无法创建，比如Hour
         */
        throw new RuntimeException("不支持创建给定时间类型的时间维度对象，给定时间类型为:" + type);
    }

    /**
     * 无参构造函数，必须给定
     */
    public DateDimension() {
        super();
    }

    /**
     * 给定全部参数的构造函数
     * 
     * @param id
     *            数据库主键id
     * @param year
     *            年份
     * @param season
     *            季度
     * @param month
     *            月
     * @param week
     *            周
     * @param day
     *            天
     * @param type
     *            日期维度类型
     * @param calendar
     *            具体对于的日期
     */
    public DateDimension(int id, int year, int season, int month, int week, int day, String type,
            Date calendar) {
        this(year, season, month, week, day, type, calendar);
        this.id = id;
    }

    /**
     * 有参构造方法， 主要用于构造具体的时间维度对象
     * 
     * @param year
     *            年份
     * @param season
     *            季度
     * @param month
     *            月
     * @param week
     *            周
     * @param day
     *            天
     * @param type
     *            日期维度类型
     * @param calendar
     *            具体对于的日期
     */
    public DateDimension(int year, int season, int month, int week, int day, String type,
            Date calendar) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
        this.type = type;
        this.calendar = calendar;
    }

    /* ======get/setter方法结束 */
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Date getCalendar() {
        return calendar;
    }

    public void setCalendar(Date calendar) {
        this.calendar = calendar;
    }
    /* ======get/setter方法结束 */

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((calendar == null) ? 0 : calendar.hashCode());
        result = prime * result + day;
        result = prime * result + id;
        result = prime * result + month;
        result = prime * result + season;
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + week;
        result = prime * result + year;
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
        DateDimension other = (DateDimension) obj;
        if (calendar == null) {
            if (other.calendar != null)
                return false;
        } else if (!calendar.equals(other.calendar))
            return false;
        if (day != other.day)
            return false;
        if (id != other.id)
            return false;
        if (month != other.month)
            return false;
        if (season != other.season)
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        if (week != other.week)
            return false;
        if (year != other.year)
            return false;
        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeInt(this.year);
        out.writeInt(this.season);
        out.writeInt(this.month);
        out.writeInt(this.week);
        out.writeInt(this.day);
        out.writeUTF(this.type);
        out.writeLong(this.calendar.getTime());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.year = in.readInt();
        this.season = in.readInt();
        this.month = in.readInt();
        this.week = in.readInt();
        this.day = in.readInt();
        this.type = in.readUTF();
        this.calendar.setTime(in.readLong());
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this)
            return 0;

        DateDimension dd = (DateDimension) o;
        int tmp = Integer.compare(this.id, dd.getId());
        if (tmp != 0)
            return tmp;

        tmp = Integer.compare(this.year, dd.getYear());
        if (tmp != 0)
            return tmp;

        tmp = Integer.compare(this.season, dd.getSeason());
        if (tmp != 0)
            return tmp;

        tmp = Integer.compare(this.month, dd.getMonth());
        if (tmp != 0)
            return tmp;

        tmp = Integer.compare(this.week, dd.getWeek());
        if (tmp != 0)
            return tmp;

        tmp = Integer.compare(this.day, dd.getDay());
        if (tmp != 0)
            return tmp;

        return this.type.compareTo(dd.getType());
    }
}
