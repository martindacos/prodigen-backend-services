package bpm.statistics.spark;

import es.usc.citius.prodigen.domainLogic.workflow.Task.Task;
import es.usc.citius.womine.model.Graph;
import es.usc.citius.womine.model.Trace;
import es.usc.citius.womine.model.TraceArcSet;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import bpm.statistics.Arc;

import java.util.*;

import static bpm.log_editor.parser.ConstantsImpl.*;

public class ArcsStatistics {

    /*--------------------------------------------------------------------
                             ARCS
    --------------------------------------------------------------------*/

    //Stat 9
    public static List<Arc> arcsFrequencyOrdered(Map<String, Trace> arcTraces, HashMap<Integer, Task> intToTasksLog) {
        //List of arcs and repetitions
        Map<Arc, Integer> arcs = new HashMap<>();

        for (Map.Entry<String, Trace> entry : arcTraces.entrySet()) {
            //Get all traces
            Trace v = entry.getValue();
            List<TraceArcSet> arcSequence = v.getArcSequence();
            //Get all arcs of the trace
            for (int i=0; i<arcSequence.size(); i++) {
                //Get the destination task
                Integer destinationTask = arcSequence.get(i).getDestinationId();
                //Get the from tasks
                TIntArrayList sourceIds = arcSequence.get(i).getSourceIds();
                TIntIterator it = sourceIds.iterator();
                while (it.hasNext()) {
                    Integer from = it.next();
                    Arc a = new Arc(intToTasksLog.get(from).getId(), intToTasksLog.get(destinationTask).getId());
                    if (arcs.containsKey(a)) {
                        arcs.put(a, arcs.get(a) + v.getNumRepetitions());
                    }else {
                        arcs.put(a, v.getNumRepetitions());
                    }
                }
            }
        }

        List<Arc> frequency = new ArrayList<>();

        //Calc frequency
        for (Map.Entry<Arc, Integer> entry : arcs.entrySet()) {
            //Double fre = (double) entry.getValue() / numberArcs;
            Double fre = (double) entry.getValue();
            entry.getKey().setFrequency(fre);
            frequency.add(entry.getKey());
        }

        //Order frequency
        Collections.sort(frequency, new Comparator<Arc>() {
            @Override
            public int compare(final Arc object1, final Arc object2) {
                if (object1.getFrequency() > object2.getFrequency()) {
                    return -1;
                } else if (object1.getFrequency() < object2.getFrequency()) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });

        /*for (Arc results : frequency) {
            System.out.println(results.toString());
        }*/


        return frequency;
    }

    //Stat 10
    public static List<Arc> arcsFrequencyOrderedSpark(JavaPairRDD<String, Trace> caseInstances, HashMap<Integer, Task> intToTasksLog) {

        JavaPairRDD<Arc, Integer> allArcs = caseInstances.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Trace>, Arc, Integer>() {
            @Override
            public Iterator<Tuple2<Arc, Integer>> call(Tuple2<String, Trace> trace) throws Exception {
                //List of arcs and repetitions
                List<Tuple2<Arc, Integer>> arcs = new ArrayList<>();

                List<TraceArcSet> arcSequence = trace._2.getArcSequence();

                //Get all arcs of the trace
                for (int i = 0; i < arcSequence.size(); i++) {
                    //Get the destination task
                    Integer destinationTask = arcSequence.get(i).getDestinationId();
                    //Get the from tasks
                    TIntArrayList sourceIds = arcSequence.get(i).getSourceIds();
                    TIntIterator it = sourceIds.iterator();
                    while (it.hasNext()) {
                        Integer from = it.next();
                        Arc a = new Arc(intToTasksLog.get(from).getId(), intToTasksLog.get(destinationTask).getId());
                        arcs.add(new Tuple2<>(a, trace._2.getNumRepetitions()));
                    }
                }

                return arcs.iterator();
            }
        });


        JavaPairRDD<Arc, Integer> arcs = allArcs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaRDD<Arc> frequency = arcs.map(new Function<Tuple2<Arc, Integer>, Arc>() {
            @Override
            public Arc call(Tuple2<Arc, Integer> v1) throws Exception {
                Double fre = (double) v1._2;
                v1._1.setFrequency(fre);
                return v1._1;
            }
        });

        JavaRDD<Arc> sortedFrequency = frequency.sortBy(new Function<Arc, Double>() {
            @Override
            public Double call(Arc v1) throws Exception {
                return v1.getFrequency();
            }
        }, false, N_PARTITIONS);

        /*for (Arc results : frequency) {
            System.out.println(results.toString());
        }*/


        return sortedFrequency.collect();
    }

    //Actividades con retorno a actividades anteriores o a sí mismas
    public static List<List<Object>> arcsLoops(Graph model) {
        model.exploreLoopArcs();
        List<Object> loops = new ArrayList<>();
        List<Object> end_loops = new ArrayList<>();
        Set<Object> nodes = new HashSet<>();
        Set<Object> nodes2 = new HashSet<>();
        for(es.usc.citius.womine.model.Arc arc : model.getArcs()){
            if (arc.isStartLoop() && arc.isEndLoop() && arc.getSource().getId().equals(arc.getDestination().getId())) {
                //Retorno a sí misma
                Arc myArc = new Arc(arc.getSource().getId(), arc.getDestination().getId());
                loops.add(myArc);
                nodes.add(arc.getSource().getId());
            } else if (arc.isEndLoop()) {
                //Retorno a actividades anteriores
                Arc myArc = new Arc(arc.getSource().getId(), arc.getDestination().getId());
                end_loops.add(myArc);
                nodes2.add(arc.getSource().getId());
            }
        }

        List<Object> nodesFinal = new ArrayList<>();
        nodesFinal.addAll(nodes);
        List<Object> nodesFinal2 = new ArrayList<>();
        nodesFinal2.addAll(nodes2);

        List<List<Object>> result = new ArrayList<>();
        result.add(loops);
        result.add(end_loops);
        result.add(nodesFinal);
        result.add(nodesFinal2);

        return result;
    }
}
