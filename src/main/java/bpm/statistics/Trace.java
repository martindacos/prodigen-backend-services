package bpm.statistics;

import java.io.Serializable;

public class Trace implements Serializable {

    private String id;
    private Long initial;
    private Long last;

    public Trace(String id, Long initial, Long last) {
        this.id = id;
        this.initial = initial;
        this.last = last;
    }

    public String getId() {
        return id;
    }

    public Long getInitial() {
        return initial;
    }


    public Long getLast() {
        return last;
    }


    public void setId(String id) {
        this.id = id;
    }


    public void setInitial(Long initial) {
        this.initial = initial;
    }


    public void setLast(Long last) {
        this.last = last;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
