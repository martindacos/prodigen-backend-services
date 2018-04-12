package bpm.statistics.spark;

import bpm.statistics.Interval;
import bpm.statistics.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

import static bpm.log_editor.parser.Constants.*;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

public class ActivityStatistics {

    private static JavaPairRDD<String, Tuple2<Long, Integer>> activities = null;
    private static JavaPairRDD<String, Tuple2<Long, Integer>> sumActivities = null;

    private static Map<String, Double> deviationTimestamp = null;
    private static JavaPairRDD<String, Long> average = null;
    private static JavaPairRDD<String, Tuple2<Long, Integer>> sumActivitiesTimesAndOccurrences = null;

    /*--------------------------------------------------------------------
                                 ACTIVITY
    --------------------------------------------------------------------*/

    //Stat 0
    public static List<Tuple2<String, Double>> activityFrequency() {
        ActivityStatistics.getActivities();

        sumActivitiesTimesAndOccurrences = ActivityStatistics.sumActivitiesTimesAndOccurrences();

        JavaPairRDD<String, Double> frequency = sumActivitiesTimesAndOccurrences.mapValues(new Function<Tuple2<Long, Integer>, Double>() {
            @Override
            public Double call(Tuple2<Long, Integer> v1) throws Exception {
                return (double) v1._2;
            }
        });

        JavaPairRDD<String, Double> orderFrequency = orderFrequency(frequency);

        return orderFrequency.take((int)orderFrequency.count());
    }

    //Stat 1
    public static List<Tuple2<String, Double>> activityRelativeFrequency() {
        //Count all activities
        double interval = activities.count();

        JavaPairRDD<String, Double> frequency = sumActivitiesTimesAndOccurrences.mapValues(new Function<Tuple2<Long, Integer>, Double>() {
            @Override
            public Double call(Tuple2<Long, Integer> v1) throws Exception {
                return v1._2 / interval;
            }
        });

        JavaPairRDD<String, Double> orderFrequency = orderFrequency(frequency);

        return orderFrequency.take((int)orderFrequency.count());
    }

    //Stat 3 TODO not call this function outside
    private static List<Tuple2<String, Double>> activityFrequencyInterval(Interval i) {
        double interval;
        JavaPairRDD<String, Tuple2<Long, Integer>> sumActivitiesFilterLocal;
        JavaRDD<Document> filter = AssistantFunctions.filter(i);
        JavaPairRDD<String, Tuple2<Long, Integer>> activitiesFilter = ActivityStatistics.getActivitiesFilter(filter);
        interval = activitiesFilter.count();
        sumActivitiesFilterLocal = ActivityStatistics.sumActivitiesFilterTimesAndOccurrences(activitiesFilter);

        //Calc frequency for all activities
        JavaPairRDD<String, Double> frequency = sumActivitiesFilterLocal.mapValues(new Function<Tuple2<Long, Integer>, Double>() {
            @Override
            public Double call(Tuple2<Long, Integer> v1) throws Exception {
                return v1._2 / interval;
            }
        });

        JavaPairRDD<String, Double> orderFrequency = orderFrequency(frequency);

        return orderFrequency.take((int)orderFrequency.count());
    }

    //Stat 2
    public static List<List<Tuple2<String, Double>>> activityFrequencyCuartil() {
        Interval i = AssistantFunctions.calculateBDInterval();

        List<Interval> cuartilIntervals = i.getCuartiles();
        List<List<Tuple2<String, Double>>> sol = new ArrayList<>();

        for (Interval interval: cuartilIntervals) {
            sol.add(activityFrequencyInterval(interval));
        }

        return sol;
    }

    //Stat 5
    //Map with the activity and a list with media, max., min, dev & totalTime, mediana
    public static List<Object> activityAverages() {
        //Get min timestamp of each activity
        JavaPairRDD<String, Tuple2<Long, Integer>> minTimestamp = activities.reduceByKey(new Function2<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Tuple2<Long, Integer>>() {
            @Override
            //Tuple with min. timestamp and "trash"
            public Tuple2<Long, Integer> call(Tuple2<Long, Integer> v1, Tuple2<Long, Integer> v2) throws Exception {
                return (v1._1 > v2._1) ? v2 : v1;
            }
        });

        //Get max timestamp of each activity
        JavaPairRDD<String, Tuple2<Long, Integer>> maxTimestamp = activities.reduceByKey(new Function2<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Tuple2<Long, Integer>>() {
            @Override
            //Tuple with max. timestamp and "trash"
            public Tuple2<Long, Integer> call(Tuple2<Long, Integer> v1, Tuple2<Long, Integer> v2) throws Exception {
                return (v1._1 > v2._1) ? v1 : v2;
            }
        });

        //Calcular la mediana del tiempo de una actividad
        //Creamos una lista para agrupar todos los tiempos de una actividad
        JavaPairRDD<String, List<Long>> activitiesM = activities.mapValues(new Function<Tuple2<Long, Integer>, List<Long>>() {
            @Override
            public List<Long> call(Tuple2<Long, Integer> v1) throws Exception {
                List<Long> r = new ArrayList<>();
                r.add(v1._1);
                return r;
            }
        });

        //Creamos una lista con todos los valores del tiempo de una actividad
        JavaPairRDD<String, List<Long>> activitiesM2 = activitiesM.reduceByKey(new Function2<List<Long>, List<Long>, List<Long>>() {
            @Override
            public List<Long> call(List<Long> v1, List<Long> v2) throws Exception {
                v1.addAll(v2);
                return v1;
            }
        });

        activitiesM2.persist(StorageLevel.MEMORY_ONLY());

        //Calculamos el tiempo total de una actividad
        JavaPairRDD<String, Long> sumAllTimeActivity = activitiesM2.mapValues(new Function<List<Long>, Long>() {
            @Override
            public Long call(List<Long> v1) throws Exception {
                Long sum = 0l;
                for (Long l : v1) {
                    sum += l;
                }
                return sum;
            }
        });

        //Ordenamos la lista y calculamos su valor medio
        JavaPairRDD<String, Long> med = activitiesM2.mapValues(new Function<List<Long>, Long>() {
            @Override
            public Long call(List<Long> v1) throws Exception {
                Collections.sort(v1);
                int size = v1.size();
                Long m = 0l;
                int medio = size / 2;
                Long l2 = v1.get(medio);
                if (size % 2 == 0) {
                    Long l1 = v1.get(medio - 1);
                    m = (l1 + l2) / 2;
                } else {
                    m = l2;
                }
                return m;
            }
        });

        activitiesM2.unpersist();

        deviationTimestamp = calcActivitiesDeviation().collectAsMap();
        Map<String, Long> mediana = med.collectAsMap();
        Map<String, Long> allTimeActivity = sumAllTimeActivity.collectAsMap();

        activities.unpersist();

        //Utils.printAverages(average.collectAsMap(), maxTimestamp.collectAsMap(), minTimestamp.collectAsMap(), deviationTimestamp.collectAsMap());

        List<Object> map = Utils.returnAverages(average.collectAsMap(), maxTimestamp.collectAsMap(), minTimestamp.collectAsMap(), deviationTimestamp, allTimeActivity, mediana);

        return map;
    }

    //Stat 7
    //Two lists. First with the max. timestamp duration (activity, timestamp, dev.) and the other the min.
    public static List<List<Object>> activityLongerShorterDuration() {
        List<List<Object>> sol = new ArrayList<>();

        Tuple2<String, Tuple2<Long, Integer>> max = sumActivities.max(new ActivityStatistics.maxComparator());
        Tuple2<String, Tuple2<Long, Integer>> min = sumActivities.max(new ActivityStatistics.minComparator());

        sumActivities.unpersist();

        List<Object> sol1 = new ArrayList();
        sol1.add(max._1);
        sol1.add(max._2._1);
        sol1.add(deviationTimestamp.get(max._1));
        List<Object> sol2 = new ArrayList();
        sol2.add(min._1);
        sol2.add(min._2._1);
        sol2.add(deviationTimestamp.get(min._1));

        sol.add(sol1);
        sol.add(sol2);

        //System.out.println("Longer activity id: " + max._1 + ", totalTimestamp: " + max._2._1 +", dev: " + devMap.get(max._1));
        //System.out.println("Shorter activity id: " + min._1 + ", totalTimestamp: " + min._2._1 +", dev: " + devMap.get(min._1));
        return sol;
    }

    //Get the duration of each activity
    private static JavaPairRDD<String, Tuple2<Long, Integer>> getActivities() {
        JavaPairRDD<String, Tuple2<Long, Integer>> allActivities = AssistantFunctions.getInitialRDD().mapToPair(new PairFunction<Document, String, Tuple2<Long, Integer>>() {
            @Override
            //Tupla con el nombre de la actividad y una tupla con su diferencia de timestamp y un contador
            public Tuple2<String, Tuple2<Long, Integer>> call(Document document) throws Exception {
                Long activityTime = document.getLong(COMPLETE_TIME) - document.getLong(INITIAL_TIME);
                Tuple2 tuple = new Tuple2(activityTime, 1);
                return new Tuple2<>(document.getString(ACTIVITY), tuple);
            }
        });

        activities = allActivities.filter(new Function<Tuple2<String, Tuple2<Long, Integer>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Tuple2<Long, Integer>> v1) throws Exception {
                if (v1._1.equals(S_DUMMY_TASK) || v1._1.equals(E_DUMMY_TASK)) {
                    return false;
                } else {
                    return true;
                }
            }
        });

        activities.persist(StorageLevel.MEMORY_ONLY());

        return activities;
    }

    private static JavaPairRDD<String, Tuple2<Long, Integer>> getActivitiesFilter(JavaRDD<Document> filterLocal) {
        JavaPairRDD<String, Tuple2<Long, Integer>> activitiesFilterLocal = filterLocal.mapToPair(new PairFunction<Document, String, Tuple2<Long, Integer>>() {
            @Override
            //Tupla con el nombre de la actividad y una tupla con su diferencia de timestamp y un contador
            public Tuple2<String, Tuple2<Long, Integer>> call(Document document) throws Exception {
                Long activityTime = document.getLong(COMPLETE_TIME) - document.getLong(INITIAL_TIME);
                Tuple2 tuple = new Tuple2(activityTime, 1);
                return new Tuple2<>(document.getString(ACTIVITY), tuple);
            }
        });

        JavaPairRDD<String, Tuple2<Long, Integer>> filter = activitiesFilterLocal.filter(new Function<Tuple2<String, Tuple2<Long, Integer>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Tuple2<Long, Integer>> v1) throws Exception {
                if (v1._1.equals(S_DUMMY_TASK) || v1._1.equals(E_DUMMY_TASK)) {
                    return false;
                } else {
                    return true;
                }
            }
        });

        return filter;
    }

    //Get the sum of the duration of the same activity and number of repetitions
    private static JavaPairRDD<String, Tuple2<Long, Integer>> sumActivitiesTimesAndOccurrences() {
        //Sum all times
        sumActivities = activities.reduceByKey(new Function2<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Tuple2<Long, Integer>>() {
                @Override
                //Tupla la suma de los timestamp y las ocurrencias de las actividades
                public Tuple2<Long, Integer> call(Tuple2<Long, Integer> v1, Tuple2<Long, Integer> v2) throws Exception {
                    Tuple2 t = new Tuple2(v1._1 + v2._1, v1._2 + v2._2);
                    return t;
                }
        });

        sumActivities.persist(StorageLevel.MEMORY_ONLY());

        return sumActivities;
    }

    private static JavaPairRDD<String, Tuple2<Long, Integer>> sumActivitiesFilterTimesAndOccurrences(JavaPairRDD<String, Tuple2<Long, Integer>> activitiesFilterLocal){
        //Sum all times
        JavaPairRDD<String, Tuple2<Long, Integer>> sumActivitiesFilterLocal = activitiesFilterLocal.reduceByKey(new Function2<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Tuple2<Long, Integer>>() {
            @Override
            //Tupla la suma de los timestamp y las ocurrencias de las actividades
            public Tuple2<Long, Integer> call(Tuple2<Long, Integer> v1, Tuple2<Long, Integer> v2) throws Exception {
                Tuple2 t = new Tuple2(v1._1 + v2._1, v1._2 + v2._2);
                return t;
            }
        });

        return sumActivitiesFilterLocal;
    }

    private static JavaPairRDD<String, Double> calcActivitiesDeviation() {
        //Calc average activities time
        average = sumActivities.mapToPair(new PairFunction<Tuple2<String, Tuple2<Long, Integer>>, String, Long>() {
            @Override
            //Tupla con la actividades y la media del timestamp
            public Tuple2<String, Long> call(Tuple2<String, Tuple2<Long, Integer>> v1) throws Exception {
                //Dividimos el timestamp total entre las ocurrencias de las actividades
                return new Tuple2<>(v1._1, v1._2._1 / v1._2._2);
            }
        });

        Map<String, Long> averages = average.collectAsMap();
        Broadcast<Map<String, Long>> broadcastAverages = SparkConnection.getContext().broadcast(averages);

        JavaPairRDD<String, Tuple2<Double, Integer>> varianceTimestamp = activities.mapToPair(new PairFunction<Tuple2<String, Tuple2<Long, Integer>>, String, Tuple2<Double, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<Double, Integer>> call(Tuple2<String, Tuple2<Long, Integer>> v1) throws Exception {
                double i = v1._2._1 - broadcastAverages.value().get(v1._1);
                double pow = pow(i, 2);
                Tuple2 t = new Tuple2<>(pow, v1._2._2);
                return new Tuple2<>(v1._1, t);
            }
        });

        JavaPairRDD<String, Tuple2<Double, Integer>> reducedVariance = varianceTimestamp.reduceByKey(new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
            @Override
            public Tuple2<Double, Integer> call(Tuple2<Double, Integer> v1, Tuple2<Double, Integer> v2) throws Exception {
                return new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2);
            }
        });

        //Calc deviation
        JavaPairRDD<String, Double> dt = reducedVariance.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Integer>>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Integer>> v1) throws Exception {
                double l = sqrt(v1._2._1 / v1._2._2);
                return new Tuple2<>(v1._1, l);
            }
        });

        return dt;
    }

    static class maxComparator implements Comparator<Tuple2<String, Tuple2<Long, Integer>>>, Serializable {
        public int compare(Tuple2<String, Tuple2<Long, Integer>> v1, Tuple2<String, Tuple2<Long, Integer>> v2) {
            if (v1._2._1 < v2._2._1) {
                return -1;
            } else if (v1._2._1 > v2._2._1) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    static class minComparator implements Comparator<Tuple2<String, Tuple2<Long, Integer>>>, Serializable {
        public int compare(Tuple2<String, Tuple2<Long, Integer>> v1, Tuple2<String, Tuple2<Long, Integer>> v2) {
            if (v1._2._1 > v2._2._1) {
                return -1;
            } else if (v1._2._1 < v2._2._1) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    private static JavaPairRDD<String, Double> orderFrequency(JavaPairRDD<String, Double> frequency) {
        //TODO to order the frequency. Not work when i return the collectAsMap()
        //Exchange key and value to sort
        JavaPairRDD<Double, String> newFrequency = frequency.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
            @Override
            public Tuple2<Double, String> call(Tuple2<String, Double> v1) throws Exception {
                return v1.swap();
            }
        });

        //Sort by frequency
        JavaPairRDD<Double, String> orderedFrequency = newFrequency.sortByKey(false);

        //Exchange key and value to print
        JavaPairRDD<String, Double> reNewFrequency = orderedFrequency.mapToPair(new PairFunction<Tuple2<Double, String>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<Double, String> v1) throws Exception {
                return v1.swap();
            }
        });

        return reNewFrequency;
    }

    //En qué trazas está una actividad
    public static List<Object> caseOfActivity(String name) {
        //Filter activity
        JavaRDD<Document> filterActivity = AssistantFunctions.getInitialRDD().filter(new Function<Document, Boolean>() {
            @Override
            public Boolean call(Document v1) throws Exception {
                if (v1.get(ACTIVITY).equals(name)) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        //Get all id's of the traces
        JavaRDD<Object> allTracesOfActivity = filterActivity.map(new Function<Document, Object>() {
            @Override
            public Object call(Document v1) throws Exception {
                return v1.get(TRACE);
            }
        });

        //Reduce duplicated values
        JavaRDD<Object> distinct = allTracesOfActivity.distinct();

        return distinct.collect();
    }
}