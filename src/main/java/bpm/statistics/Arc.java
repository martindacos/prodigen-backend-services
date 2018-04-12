package bpm.statistics;

import java.io.Serializable;

public class Arc implements Serializable{
    private String activityA;
    private String activityB;
    private Double frequency;

    public Arc(String activityA, String activityB) {
        this.activityA = activityA;
        this.activityB = activityB;
        this.frequency = 0d;
    }

    public void setFrequency(Double frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "Arc{" +
                "activityA='" + activityA + '\'' +
                ", activityB='" + activityB + '\'' +
                ", frequency='" + frequency +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        Arc arc = (Arc) o;

        if (!activityA.equals(arc.activityA)) return false;
        return activityB.equals(arc.activityB);
    }

    @Override
    public int hashCode() {
        int result = activityA.hashCode();
        result = 31 * result + activityB.hashCode();
        return result;
    }

    public Double getFrequency() {
        return frequency;
    }

    public String getActivityA() {
        return activityA;
    }

    public String getActivityB() {
        return activityB;
    }
}
