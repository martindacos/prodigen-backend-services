package bpm.statistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Averages {
    public static void calcAverages(Map<Arc, List<Long>> arcs) {
        for (Map.Entry<Arc, List<Long>> entry : arcs.entrySet()) {
            Arc key = entry.getKey();
            List<Long> value = entry.getValue();
            long max = Long.MIN_VALUE;
            long min = Long.MAX_VALUE;
            long media = 0l;
            double desviacion;
            double varianza = 0d;

            for (int i=0; i < value.size(); i++) {
                long val = value.get(i);
                if (val > max) {
                    max = val;
                }
                if (val < min) {
                    min = val;
                }
                media = media + val;
            }
            media = media / value.size();
            for (int i=0; i < value.size(); i++) {
                double range = Math.pow(value.get(i) - media, 2f);
                varianza = varianza + range;
            }
            varianza = varianza / value.size();
            desviacion = Math.sqrt(varianza);

            System.out.println(key.toString() +" "+ max +" "+ min +" "+ media +" "+ desviacion);
        }
    }

    public static void calcAveragesTraces(Map<String, List<Long>> traces) {
        for (Map.Entry<String, List<Long>> entry : traces.entrySet()) {
            String key = entry.getKey();
            List<Long> value = entry.getValue();
            long max = Long.MIN_VALUE;
            long min = Long.MAX_VALUE;
            long media = 0l;
            double desviacion;
            double varianza = 0d;

            for (int i=0; i < value.size(); i++) {
                long val = value.get(i);
                if (val > max) {
                    max = val;
                }
                if (val < min) {
                    min = val;
                }
                media = media + val;
            }
            media = media / value.size();
            for (int i=0; i < value.size(); i++) {
                double range = Math.pow(value.get(i) - media, 2f);
                varianza = varianza + range;
            }
            varianza = varianza / value.size();
            desviacion = Math.sqrt(varianza);

            System.out.println("{ _id : " + key.toString() +", maximo : "+ max +", minimo : "+ min +", media : "+ media +", desviacionTipica : "+ desviacion + " }");
        }
    }

    public static void calcLongerTraces(Map<String, List<Long>> traces) {
        Map<String, AveragesVO> tracesAverages = new HashMap();
        String maxTrace = "";
        long maxValueTrace = Long.MIN_VALUE;
        String minTrace = "";
        long minValueTrace = Long.MAX_VALUE;

        for (Map.Entry<String, List<Long>> entry : traces.entrySet()) {
            String key = entry.getKey();
            List<Long> value = entry.getValue();
            long max = Long.MIN_VALUE;
            long min = Long.MAX_VALUE;
            long media = 0l;
            double desviacion;
            double varianza = 0d;

            for (int i=0; i < value.size(); i++) {
                long val = value.get(i);
                if (val > max) {
                    max = val;
                }
                if (val < min) {
                    min = val;
                }
                media = media + val;
            }
            media = media / value.size();
            for (int i=0; i < value.size(); i++) {
                double range = Math.pow(value.get(i) - media, 2f);
                varianza = varianza + range;
            }
            varianza = varianza / value.size();
            desviacion = Math.sqrt(varianza);

            //Save traces max/min
            if (max > maxValueTrace) {
                maxTrace = key.toString();
                maxValueTrace = max;
            }
            if (min < minValueTrace) {
                minTrace = key.toString();
                minValueTrace = min;
            }

            AveragesVO a = new AveragesVO(max, min, media, desviacion, varianza);
            tracesAverages.put(key.toString(), a);
        }

        //Print max/min traces
        System.out.println(maxTrace + " " + tracesAverages.get(maxTrace).toStringMax());
        System.out.println(minTrace + " " + tracesAverages.get(minTrace).toStringMin());
    }
}
