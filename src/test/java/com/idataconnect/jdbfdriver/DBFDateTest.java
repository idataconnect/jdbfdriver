package com.idataconnect.jdbfdriver;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class DBFDateTest {

    @Test
    public void testJulianDays() {
        System.out.println("fromCalendarDays");
        DBFDate d = DBFDate.getCurrentDate();
        assertEquals(d,
                DBFDate.fromJulianDay(d.getJulianDay()));
        d = new DBFDate(10, 10, 1950);
        assertEquals(d,
                DBFDate.fromJulianDay(d.getJulianDay()));
        d = new DBFDate(1, 1, 200);
        assertEquals(d,
                DBFDate.fromJulianDay(d.getJulianDay()));
        d = new DBFDate(1, 1, -1000);
        assertEquals(d,
                DBFDate.fromJulianDay(d.getJulianDay()));
        d = new DBFDate(1, 1, 1980);
        assertEquals(d,
                DBFDate.fromJulianDay(d.getJulianDay()));
        d = new DBFDate(1, 1, 2020);
        assertEquals(d,
                DBFDate.fromJulianDay(d.getJulianDay()));
    }

    @Test
    public void testGetDayOfWeek() {
        System.out.println("getDayOfWeek");
        DBFDate instance = new DBFDate(5, 18, 2012);
        int expResult = 5;
        int result = instance.getDayOfWeek();
        assertEquals(expResult, result);
    }

    @Test
    public void testCompareTo() {
        System.out.println("compareTo");
        DBFDate d1 = new DBFDate(5, 18, 2012);
        DBFDate d2 = DBFDate.getCurrentDate();
        int expResult = -1;
        int result = d1.compareTo(d2);
        assertEquals(expResult, result);

        DBFDate d3 = new DBFDate(5, 18, 2012);
        expResult = 0;
        result = d1.compareTo(d3);
        assertEquals(expResult, result);

        expResult = 1;
        result = d1.compareTo(new DBFDate(5, 18, 2011));
        assertEquals(expResult, result);
    }
}
