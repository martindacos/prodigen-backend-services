package bpm.statistics;

import com.mongodb.AggregationOutput;
import com.mongodb.DBObject;
import org.bson.Document;
import scala.Tuple2;

import java.util.*;

public class Utils {

    public static void printAggregationOutput(AggregationOutput output) {
        for (DBObject results : output.results()) {
            System.out.println(results);
        }
    }

    public static void printMap(Map<Object, Object> map) {
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            System.out.println("\t" + entry.getKey() + " , " + entry.getValue());
        }
    }

    public static Map<Object, Object> returnMap(Map<String, Tuple2<Long, Long>> mapActivities) {
        Map sol = new HashMap();
        for (Map.Entry entry : mapActivities.entrySet()) {
            Tuple2<Long, Long> value = (Tuple2<Long, Long>) entry.getValue();
            sol.put(entry.getKey(), value._1);
            //System.out.println(entry.getKey() + ", " + value._1);
        }
        return sol;
    }

    public static void printMap6(Map<String, Tuple2<Long, Long>> mapActivities) {
        for (Map.Entry entry : mapActivities.entrySet()) {
            Tuple2<Long, Long> value = (Tuple2<Long, Long>) entry.getValue();
            System.out.println(entry.getKey() + ", " + value._2);
        }
    }

    public static Map<Object, Object> returnMap2(Map<String, Tuple2<Long, Long>> mapActivities) {
        Map sol = new HashMap();
        for (Map.Entry entry : mapActivities.entrySet()) {
            Tuple2<Long, Long> value = (Tuple2<Long, Long>) entry.getValue();
            sol.put(entry.getKey(), value._2);
            //System.out.println(entry.getKey() + ", " + value._2);
        }
        return sol;
    }

    public static void printArcs(Map<Arc, List<Long>> arcs) {
        for (Arc name: arcs.keySet()){
            List<Long> value = arcs.get(name);
            System.out.println(name.toString() + " " + value.toString());
        }
    }

    public static void printTraces(Map<String, List<Long>> arcs) {
        for (String name: arcs.keySet()){
            List<Long> value = arcs.get(name);
            System.out.println(name + " " + value.toString());
        }
    }

    public static void printAverages(Map<String, Long> media, Map<String, Tuple2<Long, Integer>> max, Map<String, Tuple2<Long, Integer>> min, Map<String, Double> dev) {
        for (Map.Entry entry : media.entrySet()) {
            String key = (String) entry.getKey();
            System.out.println(" id: " + key + ", media: " + entry.getValue() + ", max: "+ max.get(key)._1 + ", min: "+ min.get(key)._1+
                    ", dev: "+dev.get(key));
        }
    }

    public static List<Object> returnAverages(Map<String, Long> media, Map<String, Tuple2<Long, Integer>> max, Map<String, Tuple2<Long, Integer>> min, Map<String, Double> dev, Map<String, Long> allTime, Map<String, Long> mediana) {
        Map<String, List<Object>> sol = new HashMap();

        for (Map.Entry entry : media.entrySet()) {
            String key = (String) entry.getKey();
            List<Object> list = new ArrayList();
            list.add(key);
            list.add(entry.getValue());
            list.add(max.get(key)._1);
            list.add(min.get(key)._1);
            list.add(dev.get(key));
            list.add(allTime.get(key));
            list.add(mediana.get(key));
            sol.put(key, list);
        }

        List<Object> sortedSol = sortByMedia(sol);

        return sortedSol;
    }

    public static Map<Object, Object> returnTraceActivitiesTime(List<Document> traces) {
        Map<Object, Object> sol = new HashMap<>();

        for (Document t : traces) {
            Object trace = t.get("trace");
            Object activities_time = t.get("activities_time");
            sol.put(trace, activities_time);
        }

        return sol;
    }

    public static List<Object> returnTraceAverages(Map<Tuple2<List<Object>, List<Object>>, List<Object>> tracesA ) {
        List<Object> sol = new ArrayList<>();

        for (Map.Entry entry : tracesA.entrySet()) {
            List<Object> preSol = new ArrayList<>();
            Tuple2<List<Object>, List<Object>> key = (Tuple2<List<Object>, List<Object>>) entry.getKey();
            preSol.add(key._1);
            preSol.add(key._2);
            List<Object> objects = tracesA.get(key);
            for (Object o : objects) {
                preSol.add(o);
            }
            sol.add(preSol);
        }

        return sol;
    }

    private static List<Object> sortByMedia(Map<String, List<Object>> unsortMap) {

        List<Map.Entry<String, List<Object>>> list = new LinkedList<Map.Entry<String, List<Object>>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Map.Entry<String, List<Object>>>() {
            public int compare(Map.Entry<String, List<Object>> o1,
                               Map.Entry<String, List<Object>> o2) {
                if ((Long) o1.getValue().get(1) > (Long) o2.getValue().get(1)) {
                    return -1;
                } else if ((Long) o1.getValue().get(1) < (Long) o2.getValue().get(1)) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        //Map<String, List<Object>> sortedMap = new LinkedHashMap<String, List<Object>>();
        List<Object> sortedMap = new ArrayList<>();
        for (Map.Entry<String, List<Object>> entry : list) {
            sortedMap.add(entry.getValue());
            //System.out.println(entry.getValue());
        }

        return sortedMap;
    }

    public static void printAverages2(Map<String, Long> media, Map<String, Long> max, Map<String, Long> min, Map<String, Double> dev) {
        for (Map.Entry entry : media.entrySet()) {
            String key = (String) entry.getKey();
            System.out.println(" id: " + key + ", media: " + entry.getValue() + ", max: "+ max.get(key) + ", min: "+ min.get(key) +
                    ", dev: "+dev.get(key));
        }
    }

    public static Map<String, List<Object>> returnAverages2(Map<String, Long> media, Map<String, Long> max, Map<String, Long> min, Map<String, Double> dev) {
        Map<String, List<Object>> sol = new HashMap();

        for (Map.Entry entry : media.entrySet()) {
            String key = (String) entry.getKey();
            List<Object> list = new ArrayList();
            list.add(entry.getValue());
            list.add(max.get(key));
            list.add(min.get(key));
            list.add(dev.get(key));
            sol.put(key, list);
            /* System.out.println(" id: " + key + ", media: " + entry.getValue() + ", max: "+ max.get(key) + ", min: "+ min.get(key) +
                    ", dev: "+dev.get(key)); */
        }

        return sol;
    }

    public static void printTracesStats(Tuple2<String, Long> maxTrace, Tuple2<String, Long> minTrace, Map<String, Double> dev) {
        System.out.println(" id: " + maxTrace._1 + ", time: " + maxTrace._2 + ", dev: "+ dev.get(maxTrace._1));
        System.out.println(" id: " + minTrace._1 + ", time: " + minTrace._2 + ", dev: "+ dev.get(minTrace._1));
    }

    public static List<String> returnActivities(List<Document> allTrace) {
        List<String> l = new ArrayList<>();
        for (Document results : allTrace) {
            l.add(results.getString("activity"));
        }
        return l;
    }
}