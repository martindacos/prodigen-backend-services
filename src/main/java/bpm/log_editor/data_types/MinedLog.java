package bpm.log_editor.data_types;


import bpm.statistics.spark.ArcStatisticsVO;
import es.usc.citius.womine.model.Pattern;
import bpm.statistics.spark.ActivityStatisticsVO;
import bpm.statistics.spark.TraceStatisticsVO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MinedLog {

    private String modelFile;
    private String hn;
    private String model;
    private ArrayList<Pattern> FP = new ArrayList<>();
    private ArrayList<Pattern> IP = new ArrayList<>();
    private Map<Integer, List<String>> FP2 = new HashMap();
    private Map<Integer, List<String>> IP2 = new HashMap();
    private ActivityStatisticsVO actStats;
    private ArcStatisticsVO arcStats;
    private TraceStatisticsVO traceStats;
    private Node node;
    private Long span;
    private String folderName;


    public String getModel() {
        return model;
    }

    public String getHn() {
        return hn;
    }

    public void setModelFile(String modelFile) {
        this.modelFile = modelFile;
    }

    public String getModelFile() {
        return modelFile;
    }

    public void setHn(String hn) {
        this.hn = hn;
    }

    public void setIP(ArrayList<Pattern> IP) {
        this.IP = IP;
    }

    public void setFP(ArrayList<Pattern> FP) {
        this.FP = FP;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public void setActStats(ActivityStatisticsVO actStats) {
        this.actStats = actStats;
    }

    public void setArcStats(ArcStatisticsVO arcStats) {
        this.arcStats = arcStats;
    }

    public void setTraceStats(TraceStatisticsVO traceStats) {
        this.traceStats = traceStats;
    }

    public ArrayList<Pattern> getFP() {
        return FP;
    }

    public ArrayList<Pattern> getIP() {
        return IP;
    }

    public ActivityStatisticsVO getActStats() {
        return actStats;
    }

    public ArcStatisticsVO getArcStats() {
        return arcStats;
    }

    public TraceStatisticsVO getTraceStats() {
        return traceStats;
    }

    public void setSpan(Long span) {
        this.span = span;
    }

    public Long getSpan() {
        return span;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public Node getNode() {
        return node;
    }

    public Map<Integer, List<String>> getFP2() {
        return FP2;
    }

    public Map<Integer, List<String>> getIP2() {
        return IP2;
    }

    public void setFP2(Map<Integer, List<String>> FP2) {
        this.FP2 = FP2;
    }

    public void setIP2(Map<Integer, List<String>> IP2) {
        this.IP2 = IP2;
    }

    public void setFolderName(String folderName) {
        this.folderName = folderName;
    }

    public String getFolderName() {
        return folderName;
    }
}
