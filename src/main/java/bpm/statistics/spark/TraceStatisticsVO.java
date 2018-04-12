package bpm.statistics.spark;

import bpm.statistics.Trace;
import scala.Tuple2;

import java.util.List;
import java.util.Map;


public class TraceStatisticsVO {

    public List<Tuple2<List<Object>, Object>> traceFrequency;

    public List<Tuple2<List<Object>, Object>> traceRelativeFrequency;

    public List<Object> tracesAverages;

    public Map<Object, Object> tracesActivitiesTimes;

    public List<List<Object>> traceLongerShorterDuration;

    public List<Trace> firstLastTimeTraceActivity;

    public List<Tuple2<Tuple2<List<Object>, List<Object>>, Object>> traceFrequency2;

    public List<Tuple2<List<Object>, List<Object>>> traceArcsFrequency;

    public List<Long> tracesCuartil;

    public List<String> mostFrequentPath;
    public String criticalPath;
    public Double tracesCount;
}
