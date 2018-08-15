package com.vivek.spark.markLewis;

import java.io.Serializable;

/**
 * Created by Vivek Kumar Mishra on 06/06/2018.
 */
public class TempData implements Serializable{
    private static final long serialVersionUID = 3048412264356334454L;

    private int day;
    private int dayOfYear;
    private int month;
    private int stateId;
    private int year;
    private double prcp;
    private double snow;
    private double tAvg;
    private double tMax;
    private double tMin;

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getDayOfYear() {
        return dayOfYear;
    }

    public void setDayOfYear(int dayOfYear) {
        this.dayOfYear = dayOfYear;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getStateId() {
        return stateId;
    }

    public void setStateId(int stateId) {
        this.stateId = stateId;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public double getPrcp() {
        return prcp;
    }

    public void setPrcp(double prcp) {
        this.prcp = prcp;
    }

    public double getSnow() {
        return snow;
    }

    public void setSnow(double snow) {
        this.snow = snow;
    }

    public double gettAvg() {
        return tAvg;
    }

    public void settAvg(double tAvg) {
        this.tAvg = tAvg;
    }

    public double gettMax() {
        return tMax;
    }

    public void settMax(double tMax) {
        this.tMax = tMax;
    }

    public double gettMin() {
        return tMin;
    }

    public void settMin(double tMin) {
        this.tMin = tMin;
    }
}
