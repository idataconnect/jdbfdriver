package com.idataconnect.jdbfdriver;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 *
 */
public class DBFDateTest {

    /**
     * Test of fromCalendarDays method, of class DBFDate.
     */
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

    /**
     * Test of getDayOfWeek method, of class DBFDate.
     */
    @Test
    public void testGetDayOfWeek() {
        System.out.println("getDayOfWeek");
        DBFDate instance = new DBFDate(5, 18, 2012);
        int expResult = 5;
        int result = instance.getDayOfWeek();
        assertEquals(expResult, result);
    }
}
