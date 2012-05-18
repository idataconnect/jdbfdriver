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
// Some date routines in this file were inspired by: http://panda.com/calendar.html

package com.idataconnect.jdbfdriver;

import java.io.Serializable;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * A simplified date class to deal with dBase III DBF dates, which don't contain
 * time or timezone information. The date may also represent a blank date,
 * which is valid in xBase, and is similar to a null type.
 * @author ben
 */
public class DBFDate implements Serializable, Comparable<DBFDate> {

    private static final long serialVersionUID = 1L;

    /**
     * Day of week names in English. This is simply to avoid having to call
     * the Java localization routines when printing the day names in English.
     */
    static final String[] EN_DAY_NAMES = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};

    /** Year portion of the date value. */
    public short year;
    /** Month portion of the date value. */
    public byte month;
    /** Day of month portion of the date value. */
    public byte day;

    /**
     * Constructs a new date object with a blank initial date.
     */
    public DBFDate() {
    }

    /**
     * Constructs a new date with the given month, day, and year.
     * @param month The month of the new date.
     * @param day The day of the new date.
     * @param year The year of the new date.
     */
    public DBFDate(int month, int day, int year) {
        this.month = (byte) month;
        this.day = (byte) day;
        this.year = (short) year;
    }

    /**
     * Gets the current date as a DBF date.
     * @return A DBF date representing the current date.
     */
    public static DBFDate getCurrentDate() {
        DBFDate current = new DBFDate();
        GregorianCalendar cal = new GregorianCalendar();
        current.month = (byte) (cal.get(Calendar.MONTH) + 1);
        current.day = (byte) cal.get(Calendar.DAY_OF_MONTH);
        current.year = (short) cal.get(Calendar.YEAR);

        return current;
    }

    /**
     * Makes this date a blank date. In other words, clears the date locally.
     */
    public void clear() {
        month = 0;
        day = 0;
        year = 0;
    }

    /**
     * Checks if this date instance is a blank date.
     * @return Whether the date is blank.
     */
    public boolean isBlank() {
        return day == 0;
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

        int y = year;
        int m = month;
        final int d = day;
        
        if (m <= 2) {
            y--;
            m += 12;
        }

        final int a = y / 100;
        final int b = a / 4;
        final int c = 2 - a + b;
        final int e = (int) (365.25 * (y + 4716));
        final int f = (int) (30.6001 * (m + 1));

        return (int) c + d + e + f - 1525;
    }

    /**
     * Creates a DBF date from the given Julian day.
     *
     * @param julianDay the Julian day to convert from, as returned
     * by {@link #getJulianDay}.
     * @return a new DBF date instance
     */
    public static DBFDate fromJulianDay(int julianDay) {
        final int z = julianDay;
        final int w = (int) ((z - 1867216.25) / 36524.25);
        final int x = (int) (w / 4);
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
     * @return The day of the week that this date occurs on, or -1 if this
     * is an empty date.
     */
    public int getDayOfWeek() {
        if (day == 0)
            return -1;

        int m = month;
        int y = year;
        if (m > 2) {
            // Mar-Dec: subtract 2 from month
            m -= 2;
        } else {
            // Jan-Feb: months 11 & 12 of previous year
            m += 10;
            y--;
        }

        int dow = (day + (7 + 31 * (m - 1)) / 12 + y + y / 4 - y / 100 + y / 400) % 7;
        dow += 2;
        if (dow > 6)
            dow -= 2;
        return dow;
    }

    /**
     * Gets the day of week represented by this date, localized in English.
     * @return The day of week as an English localized string.
     */
    public String getDayOfWeekEn() {
        int dayOfWeek = getDayOfWeek();
        if (dayOfWeek == -1)
            return "";
        else
            return EN_DAY_NAMES[dayOfWeek];
    }

    /**
     * {@inheritDoc}
     * This implementation compares based on the Julian days.
     */
    public int compareTo(DBFDate other) {
        return new Integer(getJulianDay()).compareTo(new Integer(other.getJulianDay()));
    }

    /**
     * Returns the date in U.S. xBase style, including the century. For example,
     * for August 1, 1980, <tt>"{8/1/1980}"</tt> is returned. If the date is
     * blank, <tt>"{  /  /    }"</tt> is returned.
     * @return The date represented by this date object.
     */
    @Override
    public String toString() {
        if (isBlank())
            return "{  /  /    }";
        else
            return "{" + month + "/" + day + "/" + year + "}";
    }

    /**
     * {@inheritDoc}
     * This implementation considers two dates equal if their Julian days
     * are equal.
     */
    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof DBFDate))
            return false;

        DBFDate other = (DBFDate) obj;
        return getJulianDay() == other.getJulianDay();
    }

    /**
     * {@inheritDoc}
     * This implementation uses the integer hash code of the
     * Julian day.
     */
    @Override
    public int hashCode() {
        return new Integer(getJulianDay()).hashCode();
    }
}
