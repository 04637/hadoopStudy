package mapreduce.temperature;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 封装年,月,温度
 *
 * @author: 04637@163.com
 * @date: 2019/6/4
 */
public class TempKey implements WritableComparable {

    private int year;
    private int month;
    private double air;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public double getAir() {
        return air;
    }

    public void setAir(double air) {
        this.air = air;
    }

    @Override
    public int compareTo(Object o) {
        return this==o?0:-1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeDouble(air);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        month = in.readInt();
        air = in.readDouble();
    }
}
