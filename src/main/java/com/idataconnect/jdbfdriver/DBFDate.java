/*
 * Copyright (c) 2009-2012, i Data Connect!
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of i Data Connect! nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package com.idataconnect.jdbfdriver;

import java.io.Serializable;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * A simplified date class to deal with dBase III DBF dates, which don't contain
 * time or timezone information. The date may also represent a blank date,
 * which is valid in xBase, and is similar to a null type.
 * 
 * @author ben
 */
public class DBFDate implements Serializable, Comparable<DBFDate>, Cloneable {

    private static final long serialVersionUID = 1L;

    /**
     * Day of week names in English. This is simply to avoid having to call
     * the Java localization routines when printing the day names in English.
     */
    transient static final String[] EN_DAY_NAMES = { "Sunday", "Monday", "Tuesday",
            "Wednesday", "Thursday", "Friday", "Saturday" };

    /** Year portion of the date value. */
    private short year = 0;
    /** Month portion of the date value. */
    private byte month = 0;
    /** Day of month portion of the date value. */
    private byte day = 0;

    /**
     * Constructs a new date object with a blank initial date.
     */
    public DBFDate() {
    }

    /**
     * Constructs a new date with the given month, day, and year.
     * 
     * @param month the month of the new date
     * @param day   the day of the new date
     * @param year  the year of the new date
     */
    public DBFDate(int month, int day, int year) {
        this.month = (byte) month;
        this.day = (byte) day;
        this.year = (short) year;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /**
     * Gets the current date as a DBF date.
     * 
     * @return a DBF date representing the current date
     */
    public static DBFDate getCurrentDate() {
        DBFDate current = new DBFDate();
        GregorianCalendar cal = new GregorianCalendar();
        current.setMonth((byte) (cal.get(Calendar.MONTH) + 1));
        current.setDay((byte) cal.get(Calendar.DAY_OF_MONTH));
        current.setYear((short) cal.get(Calendar.YEAR));

        return current;
    }

    /**
     * Makes this date a blank date. In other words, clears the date locally.
     */
    public void clear() {
        setMonth((byte) 0);
        setDay((byte) 0);
        setYear((short) 0);
    }

    /**
     * Checks if this date instance is a blank date.
     * 
     * @return whether the date is blank
     */
    public boolean isBlank() {
        return getDay() == 0;
    }

    /**
     * Gets the Julian day of this date.
     *
     * @return the Julian day, or <code>-1</code> if the current date is blank.
     */
    public int getJulianDay() {
        if (isBlank()) {
            return -1;
        }

        int y = getYear();
        int m = getMonth();
        final int d = getDay();

        if (m <= 2) {
            y--;
            m += 12;
        }

        final int a = y / 100;
        final int b = a / 4;
        final int c = 2 - a + b;
        final int e = (int) (365.25 * (y + 4716));
        final int f = (int) (30.6001 * (m + 1));

        return c + d + e + f - 1525;
    }

    /**
     * Creates a DBF date from the given Julian day.
     *
     * @param julianDay the Julian day to convert from, as returned
     *                  by {@link #getJulianDay}
     * @return a new DBF date instance
     */
    public static DBFDate fromJulianDay(int julianDay) {
        final int z = julianDay;
        final int w = (int) ((z - 1867216.25) / 36524.25);
        final int x = w / 4;
        final int a = z + 1 + w - x;
        final int b = a + 1525;
        final int c = (int) ((b - 122.1) / 365.25);
        final int d = (int) (365.25 * c);
        final int e = (int) ((b - d) / 30.6001);
        final int f = (int) (30.6001 * e);
        final int day = b - d - f;
        final int month = e <= 13 ? e - 1 : e - 13;
        final int year = month <= 2 ? c - 4715 : c - 4716;

        return new DBFDate(month, day, year);
    }

    /**
     * Calculates the day of the week that the date represented by this date
     * object, occurs on. Sunday is zero, Monday is one, etc.
     * 
     * @return the day of the week that this date occurs on, or -1 if this
     *         is an empty date
     */
    public int getDayOfWeek() {
        if (getDay() == 0) {
            return -1;
        }

        int m = getMonth();
        int y = getYear();
        if (m > 2) {
            // Mar-Dec: subtract 2 from month
            m -= 2;
        } else {
            // Jan-Feb: months 11 & 12 of previous year
            m += 10;
            y--;
        }

        int dow = (getDay() + (7 + 31 * (m - 1)) / 12 + y + y / 4 - y / 100 + y / 400) % 7;
        dow += 2;
        if (dow > 6) {
            dow -= 2;
        }
        return dow;
    }

    /**
     * Gets the day of week represented by this date, localized in English.
     * 
     * @return the day of week as an English localized string
     */
    public String getDayOfWeekEn() {
        int dayOfWeek = getDayOfWeek();
        if (dayOfWeek == -1) {
            return "";
        } else {
            return EN_DAY_NAMES[dayOfWeek];
        }
    }

    /**
     * Returns the date in xBase's DTOS() function format. This includes the
     * year with century, followed by the month, followed by the day of month.
     *
     * @return the date in xBase DTOS format
     */
    public String dtos() {
        return String.format("%02d%02d%02d", getYear(), getMonth(), getDay());
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation compares based on the Julian days.
     * </p>
     */
    public int compareTo(DBFDate other) {
        return Integer.valueOf(getJulianDay()).compareTo(Integer.valueOf(other.getJulianDay()));
    }

    /**
     * Returns the date in U.S. xBase style, including the century. For example,
     * for August 1, 1980, <tt>"{8/1/1980}"</tt> is returned. If the date is
     * blank, <tt>"{  /  /    }"</tt> is returned.
     * 
     * @return the date represented by this date object
     */
    @Override
    public String toString() {
        if (isBlank()) {
            return "{  /  /    }";
        } else {
            return "{" + getMonth() + "/" + getDay() + "/" + getYear() + "}";
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation considers two dates equal if their Julian days
     * are equal.
     * </p>
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DBFDate)) {
            return false;
        }

        DBFDate other = (DBFDate) obj;
        return getJulianDay() == other.getJulianDay();
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation uses the integer hash code of the
     * Julian day.
     * </p>
     */
    @Override
    public int hashCode() {
        return Integer.valueOf(getJulianDay()).hashCode();
    }

    /**
     * Gets the four digit year portion of the date, for example: 2000.
     *
     * @return the year
     */
    public short getYear() {
        return year;
    }

    /**
     * Sets the four digit year portion of the date, for example: 2000.
     *
     * @param year the year to set
     */
    public void setYear(short year) {
        this.year = year;
    }

    /**
     * Gets the month of the year, starting at 1.
     * 
     * @return the month
     */
    public byte getMonth() {
        return month;
    }

    /**
     * Sets the month of the year, starting at 1.
     * 
     * @param month the month to set
     */
    public void setMonth(byte month) {
        this.month = month;
    }

    /**
     * Gets the day of the month, starting at 1.
     *
     * @return the day
     */
    public byte getDay() {
        return day;
    }

    /**
     * Sets the day of the month, starting at 1.
     *
     * @param day the day to set
     */
    public void setDay(byte day) {
        this.day = day;
    }
}
