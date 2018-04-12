package bpm.statistics;

public class AveragesVO {
    private long max;
    private long min;
    private long media;
    private double desviacion;
    private double varianza;

    public AveragesVO(long max, long min, long media, double desviacion, double varianza) {
        this.max = max;
        this.min = min;
        this.media = media;
        this.desviacion = desviacion;
        this.varianza = varianza;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public long getMedia() {
        return media;
    }

    public void setMedia(long media) {
        this.media = media;
    }

    public double getDesviacion() {
        return desviacion;
    }

    public void setDesviacion(double desviacion) {
        this.desviacion = desviacion;
    }

    public double getVarianza() {
        return varianza;
    }

    public void setVarianza(double varianza) {
        this.varianza = varianza;
    }

    @Override
    public String toString() {
        return "AveragesVO{" +
                "max=" + max +
                ", min=" + min +
                ", media=" + media +
                ", desviacion=" + desviacion +
                ", varianza=" + varianza +
                '}';
    }

    public String toStringMax() {
        return "{" +
                "tiempoEjecucion=" + max +
                ", desviacionTipica=" + desviacion +
                '}';
    }

    public String toStringMin() {
        return "{" +
                "tiempoEjecucion=" + min +
                ", desviacionTipica=" + desviacion +
                '}';
    }
}
