package bpm.statistics.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;
import scala.Serializable;
import scala.Tuple2;
import bpm.statistics.Trace;
import bpm.statistics.Utils;

import java.util.*;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static bpm.log_editor.parser.Constants.*;
import static bpm.log_editor.parser.ConstantsImpl.*;

public class TracesStatistics {
    private static JavaPairRDD<String, Long> tracesTime = null;
    public static JavaPairRDD<List<Object>, List<Object>> allTracesPair;
    public static double tracesCount;
    private static JavaPairRDD<List<Object>, Object> frequency;
    private static JavaPairRDD<Tuple2<List<Object>, List<Object>>, Object> frequency2;
    /*--------------------------------------------------------------------
                             TRACES
    --------------------------------------------------------------------*/

    //Stat 17
    //Total appearances trace
    public static List<Tuple2<List<Object>, Object>> traceFrequency() {
        //Whith activities as key
        frequency = allTracesPair.mapToPair(new PairFunction<Tuple2<List<Object>,List<Object>>, List<Object>, Object>() {
            @Override
            public Tuple2<List<Object>, Object> call(Tuple2<List<Object>, List<Object>> v1) throws Exception {
                return new Tuple2<List<Object>, Object>(v1._1, v1._2.size());
            }
        });

        frequency.persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<List<Object>, Object> orderFrequency = orderFrequency(frequency);

        /*orderFrequency.foreach(data -> {
            System.out.println("traces="+data._1() + " frequency=" + data._2());
        });*/

        return orderFrequency.take(3);
    }

    //Stat 19
    //Percentage appearances trace
    public static List<Tuple2<List<Object>, Object>>  traceFrequencyRelative(){
        Broadcast<Double> broadcastTracesCount = SparkConnection.getContext().broadcast(tracesCount);
        //Whith activities as key
        JavaPairRDD<List<Object>, Object> percentageFrequency =  allTracesPair.mapToPair(new PairFunction<Tuple2<List<Object>,List<Object>>, List<Object>, Object>() {
            @Override
            public Tuple2<List<Object>, Object> call(Tuple2<List<Object>, List<Object>> v1) throws Exception {
                return new Tuple2<List<Object>, Object>(v1._1, v1._2.size() / broadcastTracesCount.value());
            }
        });

        JavaPairRDD<List<Object>, Object> orderPercentageFrequency = orderFrequency(percentageFrequency);

        return orderPercentageFrequency.take(3);
    }

    //Total appearances trace
    public static List<Tuple2<Tuple2<List<Object>, List<Object>>, Object>> traceFrequency2() {
        //Whith activities as key
        frequency2 = allTracesPair.mapToPair(new PairFunction<Tuple2<List<Object>,List<Object>>, Tuple2<List<Object>, List<Object>>, Object>() {
            @Override
            public Tuple2<Tuple2<List<Object>, List<Object>>, Object> call(Tuple2<List<Object>, List<Object>> v1) throws Exception {
                Tuple2<List<Object>, List<Object>> id = new Tuple2<>(v1._2, v1._1);
                return new Tuple2<Tuple2<List<Object>, List<Object>>, Object>(id, v1._2.size());
            }
        });

        frequency2.persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<Tuple2<List<Object>, List<Object>>, Object> orderFrequency = orderFrequency2(frequency2);

        /*orderFrequency.foreach(data -> {
            System.out.println("traces="+data._1() + " frequency=" + data._2());
        });*/

        return orderFrequency.take(3);
    }

    //Stat New
    public static Map<Object, Object> tracesActivitiesTimes(){
        JavaRDD<Document> tracesRDD = AssistantFunctions.getTracesRDD();
        Map<Object, Object> objects = Utils.returnTraceActivitiesTime(tracesRDD.collect());

        return objects;
    }

    //Stat 21
    public static List<Object> tracesAverages(){
        Map<String, Long> tracesT = TracesStatistics.sumTracesTimes(AssistantFunctions.getTracesRDD()).collectAsMap();

        JavaPairRDD<Tuple2<List<Object>, List<Object>>, List<Object>> averagesC = allTracesPair.mapToPair(new PairFunction<Tuple2<List<Object>, List<Object>>, Tuple2<List<Object>, List<Object>>, List<Object>>() {
            @Override
            public Tuple2<Tuple2<List<Object>, List<Object>>, List<Object>> call(Tuple2<List<Object>, List<Object>> v1) throws Exception {
                Long acumulator = 0l;
                Long minT = Long.MAX_VALUE;
                Long maxT = Long.MIN_VALUE;

                //Calculamos la media, valor mínimo y valor máximo de cada conjunto
                for (int i = 0; i < v1._2.size(); i++) {
                    String trace = (String) v1._2.get(i);
                    Long value = tracesT.get(trace);
                    acumulator = acumulator + value;
                    if (value < minT) {
                        minT = value;
                    }
                    if (value > maxT) {
                        maxT = value;
                    }
                }
                long media = acumulator / v1._2.size();

                //Restamos cada valor a su media y lo elevamos al cuadrado
                List<Double> devPar = new ArrayList<>();
                for (int j = 0; j < v1._2.size(); j++) {
                    String trace = (String) v1._2.get(j);
                    Long value = tracesT.get(trace);
                    double pow = pow((value - media), 2);
                    devPar.add(pow);
                }

                //Sumamos todas las desviaciones totales y las dividimos entre el número total, calculando su raíz
                double acu = 0d;
                for (int j = 0; j < devPar.size(); j++) {
                    acu = acu + devPar.get(j);
                }
                double aux = acu / devPar.size();
                double desvi = sqrt(aux);

                Tuple2<Tuple2<List<Object>, List<Object>>, List<Object>> t;
                List<Object> l = new ArrayList<>();
                l.add(maxT);
                l.add(minT);
                l.add(media);
                l.add(desvi);
                l.add(acumulator);

                //Add dummy activities
                v1._1.add(0, S_DUMMY_TASK);
                v1._1.add(v1._1.size(), E_DUMMY_TASK);

                Tuple2<List<Object>, List<Object>> ids = new Tuple2<>(v1._1, v1._2);
                t = new Tuple2<>(ids, l);
                return t;
            }
        });
        Map<Tuple2<List<Object>, List<Object>>, List<Object>> tracesA = averagesC.collectAsMap();

        frequency.unpersist();
        allTracesPair.unpersist();

        List<Object> objects = Utils.returnTraceAverages(tracesA);
        return objects;
    }

    //Stat 23
    //Two lists. First with the max. timestamp duration (trace, timestamp) and the other the min.
    public static List<List<Object>> traceLongerShorterDuration(){
        List<List<Object>> sol = new ArrayList<>();

        Tuple2<String, Long> maxTrace = tracesTime.max(new maxTraceComparator());
        Tuple2<String, Long> minTrace = tracesTime.max(new minTraceComparator());

        tracesTime.unpersist();

        List<Object> sol1 = new ArrayList();
        Document allTrace = getAllTrace(maxTrace._1);
        sol1.add(allTrace.get(ACTIVITIES));
        sol1.add(maxTrace._2);
        sol1.add(maxTrace._1);
        List<Object> sol2 = new ArrayList();
        Document allTrace2 = getAllTrace(minTrace._1);
        sol2.add(allTrace2.get(ACTIVITIES));
        sol2.add(minTrace._2);
        sol2.add(minTrace._1);

        sol.add(sol1);
        sol.add(sol2);

        //Utils.printTracesStats(maxTrace, minTrace, deviationTracesTimestamp.collectAsMap());
        return sol;
    }

    //Stat 25
    public static Map<Object, Object> firstTimeTraceActivity() {
        return AssistantFunctions.getTracesRDD().mapToPair(new PairFunction<Document, Object, Object>() {
            @Override
            public Tuple2<Object, Object> call(Document document) throws Exception {
                return new Tuple2<>(document.get(TRACE), document.get(FIRST_ACTIVITY_TIME));
            }
        }).collectAsMap();
    }

    //Stat 26
    public static Map<Object, Object> lastTimeTraceActivity(){
        Map<Object, Object> re = AssistantFunctions.getTracesRDD().mapToPair(new PairFunction<Document, Object, Object>() {
            @Override
            public Tuple2<Object, Object> call(Document document) throws Exception {
                return new Tuple2<>(document.get(TRACE), document.get(LAST_ACTIVITY_TIME));
            }
        }).collectAsMap();

        return re;
    }

    public static List<Trace> firstLastTimeTraceActivity() {

        JavaRDD<Trace> times = AssistantFunctions.getTracesRDD().map(new Function<Document, Trace>() {
            @Override
            public Trace call(Document document) throws Exception {
                Trace t = new Trace((String) document.get(TRACE), (long) document.get(FIRST_ACTIVITY_TIME), (long) document.get(LAST_ACTIVITY_TIME));
                return t;
            }
        });

        JavaRDD<Trace> sortedTimes = times.sortBy(new Function<Trace, Long>() {
            @Override
            public Long call(Trace v1) throws Exception {
                return v1.getInitial();
            }
        }, true, N_PARTITIONS);

        List<Trace> collect = sortedTimes.collect();
        /*for (Trace t : collect) {
            System.out.println(t);
        }*/

        return collect;
    }

    //Stat 27
    public static List<Long> timeTracesCuartil() {
        //Get all traces duration
        JavaRDD<Long> times = AssistantFunctions.getTracesRDD().map(new Function<Document, Long>() {
            @Override
            public Long call(Document v1) throws Exception {
                Long firstTime = (Long) v1.get("firstTime");
                Long lastTime = (Long) v1.get("lastTime");

                return (lastTime - firstTime);
            }
        });

        //Order traces duration
        JavaRDD<Long> rdd = times.sortBy(new Function<Long, Long>() {
            @Override
            public Long call(Long v1) throws Exception {
                return v1;
            }
        }, true, N_PARTITIONS);

        rdd.persist(StorageLevel.MEMORY_ONLY());

        /*List<Long> collect = rdd.collect();
        for (Long l : collect) {
            System.out.println(l);
        }*/

        long rddSize = rdd.count();
        //Assign to each trace an id to facilitate percentile retrieval
        JavaPairRDD<Long, Long> zipRdd = rdd.zipWithIndex().mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<Long, Long> t) throws Exception {
                return t.swap();
            }
        });
        zipRdd.persist(StorageLevel.MEMORY_ONLY());

        /*Map<Long, Long> map = zipRdd.collectAsMap();
        for (Map.Entry<Long, Long> entry : map.entrySet()) {
            System.out.println("\t" + entry.getKey() + " , " + entry.getValue());
        }*/

        long q1 = (long) (0.25 * rddSize);
        long q2 = (long) (0.5 * rddSize);
        long q3 = (long) (0.75 * rddSize);

        List<Long> cuart = new ArrayList<>();
        if (rddSize > 4) {
            //Min Time
            Long n = zipRdd.lookup(0l).get(0);
            cuart.add(n);

            //Cuartil 1
            n = zipRdd.lookup(q1).get(0);
            cuart.add(n);

            //Mediana
            n = zipRdd.lookup(q2).get(0);
            cuart.add(n);

            //Cuartil 2
            n = zipRdd.lookup(q3).get(0);
            cuart.add(n);

            //Max Time
            n = zipRdd.lookup(rddSize - 1).get(0);
            cuart.add(n);
        }

        zipRdd.unpersist();
        rdd.unpersist();
        AssistantFunctions.getTracesRDD().unpersist();

        return cuart;
    }

    //Median time of all traces
    public static Double medianExecutionTime(){
        return 120938712098312873d;
    }

    //TODO remove this shit
    //Get all things of a trace
    private static Document getAllTrace(String traceName) {

        JavaRDD<Document> trace1 = AssistantFunctions.tracesRDD.filter(new Function<Document, Boolean>() {
            @Override
            public Boolean call(Document v1) throws Exception {
                return (v1.getString(TRACE).equals(traceName));
            }
        });

        List<Document> take = trace1.take(1);
        return take.get(0);
    }

    private static JavaPairRDD<String, Long> sumTracesTimes(JavaRDD<Document> tracesRDD) {
        //Calc all traces times
        tracesTime = tracesRDD.mapToPair(new PairFunction<Document, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Document document) throws Exception {
                Long traceTime = document.getLong(LAST_ACTIVITY_TIME) - document.getLong(FIRST_ACTIVITY_TIME);
                return new Tuple2<>(document.getString(TRACE), traceTime);
            }
        });

        tracesTime.persist(StorageLevel.MEMORY_ONLY());

        return tracesTime;
    }

    static class maxTraceComparator implements Comparator<Tuple2<String, Long>>, Serializable {
        public int compare(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {
            if (v1._2 < v2._2) {
                return -1;
            } else if (v1._2 > v2._2) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    static class minTraceComparator implements Comparator<Tuple2<String, Long>>, Serializable {
        public int compare(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {
            if (v1._2 > v2._2) {
                return -1;
            } else if (v1._2 < v2._2) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    private static JavaPairRDD<List<Object>, Object> orderFrequency(JavaPairRDD<List<Object>, Object> frequency) {
        //Exchange key and value to sort
        JavaPairRDD<Object, List<Object>> newFrequency = frequency.mapToPair(new PairFunction<Tuple2<List<Object>, Object>, Object, List<Object>>() {
            @Override
            public Tuple2<Object, List<Object>> call(Tuple2<List<Object>, Object> v1) throws Exception {
                return new Tuple2<>(v1._2, v1._1);
            }
        });

        //Sort by frequency
        JavaPairRDD<Object, List<Object>> orderedFrequency = newFrequency.sortByKey(false);

        //Exchange key and value to print
        JavaPairRDD<List<Object>, Object> reNewFrequency = orderedFrequency.mapToPair(new PairFunction<Tuple2<Object, List<Object>>, List<Object>, Object>() {
            @Override
            public Tuple2<List<Object>, Object> call(Tuple2<Object, List<Object>> v1) throws Exception {
                //Add dummy activities
                v1._2.add(0, S_DUMMY_TASK);
                v1._2.add(v1._2.size(), E_DUMMY_TASK);
                return new Tuple2<>(v1._2, v1._1);
            }
        });

        return reNewFrequency;
    }


    private static JavaPairRDD<Tuple2<List<Object>, List<Object>>, Object> orderFrequency2(JavaPairRDD<Tuple2<List<Object>, List<Object>>, Object> frequency) {
        //Exchange key and value to sort
        JavaPairRDD<Object, Tuple2<List<Object>, List<Object>>> newF = frequency.mapToPair(new PairFunction<Tuple2<Tuple2<List<Object>, List<Object>>, Object>, Object, Tuple2<List<Object>, List<Object>>>() {
            @Override
            public Tuple2<Object, Tuple2<List<Object>, List<Object>>> call(Tuple2<Tuple2<List<Object>, List<Object>>, Object> v1) throws Exception {
                return new Tuple2<>(v1._2, v1._1);
            }
        });

        //Sort by frequency
        JavaPairRDD<Object, Tuple2<List<Object>, List<Object>>> orderedFrequency = newF.sortByKey(false);

        JavaPairRDD<Tuple2<List<Object>, List<Object>>, Object> reNewFrequency = orderedFrequency.mapToPair(new PairFunction<Tuple2<Object, Tuple2<List<Object>, List<Object>>>, Tuple2<List<Object>, List<Object>>, Object>() {
            @Override
            public Tuple2<Tuple2<List<Object>, List<Object>>, Object> call(Tuple2<Object, Tuple2<List<Object>, List<Object>>> v1) throws Exception {
                //Add dummy activities
                v1._2._2.add(0, S_DUMMY_TASK);
                v1._2._2.add(v1._2._2.size(), E_DUMMY_TASK);
                return new Tuple2<>(v1._2, v1._1);
            }
        });

        return reNewFrequency;
    }
}