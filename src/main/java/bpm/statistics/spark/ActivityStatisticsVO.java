package bpm.statistics.spark;

import scala.Tuple2;

import java.util.List;


public class ActivityStatisticsVO {

    public List<Tuple2<String, Double>> activityFrequency;

    public List<Tuple2<String, Double>> activityRelativeFrequency;

    public List<List<Tuple2<String, Double>>> activityFrequencyCuartil;

    public List<Object> activityAverages;

    public List<List<Object>> activityLongerShorterDuration;

    public String activityFrequencyHeatMap;

    public String activityDurationHeatMap;

    public String activityFrequencyHeatMap2;

    public String activityDurationHeatMap2;

    public Object frequencyChart;

    public Object durationChart;
}

