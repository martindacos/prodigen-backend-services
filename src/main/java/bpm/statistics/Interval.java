package bpm.statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Interval {
    private long first;
    private long last;
    //Choose the interval unit
    //Seconds
    //private static String intervalUnits = "s";
    //Hours
    private static String intervalUnits = "h";
    //Days
    //private static String intervalUnits = "d";
    //To increase the cuartil intervals
    private static long intervalAdder = 0000000000001l;
    //Interval in timestamp
    private static double sInterval = 1d;
    //List with the cuartiles of the interval
    private static List<Interval> cuartiles = null;

    //Test interval
    public Interval() {
        //Interval for medeiros
        //this.first = 1264712714154l;
        //this.last = 1264785449120l;

        //Interval for amtega
        this.first = 1420066920000l;
        this.last = 1451603460000l;
        calcInterval();
    }

    public Interval(long f, long l) {
        this.first = f;
        this.last = l;

        calcInterval();
    }

    public Long getFirst() {
        return first;
    }

    public Long getLast() {
        return last;
    }

    public double getsInterval() {
        return sInterval;
    }

    public List<Interval> getCuartiles() {
        if (cuartiles == null) {
            calcCuartiles();
        }
        return cuartiles;
    }

    private void calcInterval(){
        //TODO Be careful with the interval, because if we don't choose a right time maybe goes to infinity interval
        long in = this.last - this.first;
        //System.out.println(this.last + " - " + this.first + " = " + in);
        if (intervalUnits.equals("s")) {
            sInterval = TimeUnit.MILLISECONDS.toSeconds(in);
        }
        if (intervalUnits.equals("h")) {
            sInterval = TimeUnit.MILLISECONDS.toHours(in);
        }
        if (intervalUnits.equals("d")) {
            sInterval = TimeUnit.MILLISECONDS.toDays(in);
        }
    }

    private void calcCuartiles(){
        long in = this.last - this.first;
        long cuartilInterval = in / 4l;

        cuartiles = new ArrayList();

        //Careful with the intervals, don't put the same number in two intervals

        long f1 = this.first + cuartilInterval;
        Interval i1 = new Interval(this.first, f1);

        long f2 = f1 + cuartilInterval;
        Interval i2 = new Interval(f1 + intervalAdder, f2);

        long f3 = f2 + cuartilInterval;
        Interval i3 = new Interval(f2 + intervalAdder, f3);

        Interval i4 = new Interval(f3 + intervalAdder, this.last);

        cuartiles.add(i1);
        cuartiles.add(i2);
        cuartiles.add(i3);
        cuartiles.add(i4);
    }
}
