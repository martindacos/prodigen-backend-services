package bpm.heuristic;

import es.usc.citius.womine.model.Arc;
import es.usc.citius.womine.model.Graph;
import es.usc.citius.womine.model.INode;
import scala.Tuple2;

import java.io.File;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static bpm.log_editor.parser.Constants.E_DUMMY_TASK;
import static bpm.log_editor.parser.Constants.LOG_DIR;
import static bpm.log_editor.parser.Constants.S_DUMMY_TASK;

class ParserHNGraph {

    private final static String HEAD = "digraph G {\n"
            + " nodesep=\".1\"; ranksep=\".1\";  fontsize=\"9\"; remincross=true;  fontname=\"Verdana\";\n"
            + " node [ height=\".3\", width=\".3\", fontname=\"Verdana\",fontsize=\"9\"];\n  		"
            + " edge [arrowsize=\"0.7\",fontsize=\"9\",fontname=\"Verdana\",arrowhead=\"vee\"];\n";
    private final static String CLOSER = "];\n";
    private final static String propertiesArc = " [label=\"  \"];\n";
    private final static String propertiesArcOpacity = " [color=\"#cacfd2\", label=\"  \"];\n";
    private final static String NORMAL_ARC = " [label=\"  \", color=\"";
    private final static String NAMED_ARC = " [color=\"#afafaf\", label=\"";
    private final static String propertiesNode = "[shape=\"box\",href=\"javascript:void(click_node(\\\'\\\\N\\\'))\",label=";
    private final static String NORMAL_NODE = "[shape=\"box\",style=filled,fillcolor=\"";
    private final static String NODE = "[shape=\"box\",style=filled,fillcolor=\"#e0f2f1\",label=";
    private final static String OPACITY_NODE = "[shape=\"box\",style=filled,color=\"#cacfd2\",fillcolor=\"white\",fontcolor=\"#cacfd2\",label=";
    private final static GraphViz gv = new GraphViz();
    private final static NumberFormat formatter = new DecimalFormat("#0");
    private final static String RANKHEAD = "{ rank = sink;\n" +
            "    Legend [shape=none, margin=0, label=<\n" ;
    private final static String RANKHEAD2 = "\n" +
            "    Legend2 [shape=none, margin=0, label=<\n" ;
    private final static String TABLEHEAD = "    <TABLE BORDER=\"0\" CELLBORDER=\"0\" CELLSPACING=\"4\" CELLPADDING=\"5\">\n" +
            "     <TR>\n";
    private final static String TABLETAIL = "</TR>\n" +
            "     </TABLE>\n";
    private final static String RANKTAIL = "   >];\n";
    private final static String LABEL = "labelloc=top;\n" +
            "labeljust=left;" +
            "label=\"";


    static String translate(Graph graph, Boolean removeComplete) {
        String graphDot;

        graphDot = "";

        graphDot += HEAD;
        graphDot += "\n";
        graphDot += "\n";

        graphDot += writeNodes(graph, removeComplete);
        graphDot += "\n";

        graphDot += writeArcs(graph, removeComplete);
        graphDot += "\n";
        graphDot += "}";

        return graphDot;
    }

    static String normalTranslate(Graph graph, Boolean removeComplete, String hnName, String name, String filter) {
        String graphDot;

        graphDot = "";

        graphDot += HEAD;
        if (!filter.isEmpty()) {
            graphDot += LABEL + "Filter: " + filter + "\";";
        }
        graphDot += "\n";
        graphDot += "\n";

        graphDot += writeNodes(graph, removeComplete);
        graphDot += "\n";

        graphDot += writeArcs(graph, removeComplete);
        graphDot += "\n";
        graphDot += "}";

        File out = new File(LOG_DIR  + hnName + "/" + "0_ModelClean.png");
        gv.writeGraphToFile( gv.getGraph(graphDot, "png", "dot"), out);

        return graphDot;
    }

    private static String writeNodes(Graph graph, Boolean removeComplete) {
        List<INode> nodes;
        String nodesString, realName;

        nodesString = "";
        nodes = graph.getNodes();
        for (INode node : nodes) {
            realName = FileTranslator.simplifyName(node.getId(), removeComplete);
            nodesString += checkIsDummyNode(realName);
        }

        return nodesString;
    }

    private static String writeArcs(Graph graph, Boolean removeComplete) {
        String nodesString;

        nodesString = "";
        Set<Arc> arcs = graph.getArcs();
        for (Arc a : arcs) {
            nodesString += FileTranslator.simplifyName(a.getSource().getId(), removeComplete) +
                    " -> " + FileTranslator.simplifyName(a.getDestination().getId(), removeComplete) + propertiesArc;
        }

        return nodesString;
    }

    private static String checkIsDummyNode(String name) {
        return name.replaceAll("\\s", "") + propertiesNode + "\"" + name + "\"" + CLOSER;
    }

    /****
     * Pintar arcos y nodos
     */

    static String translatePaint(Graph graph, Boolean removeComplete, List<INode> nodesPattern, Set<Arc> arcsPattern,
                                 Integer i, String frequency, String hnName, Boolean infrequent, Double threshold, boolean print) {
        String graphDot;

        graphDot = "";

        graphDot += HEAD;
        graphDot += LABEL + "Frequency : " + frequency + "\\lNº tasks: " + nodesPattern.size() + "\";";
        graphDot += "\n";
        graphDot += "\n";

        graphDot += writeNodesPaint(graph, removeComplete, nodesPattern);
        graphDot += "\n";

        graphDot += writeArcsPaint(graph, removeComplete, arcsPattern);
        graphDot += "\n";
        graphDot += "}";

        if (print) {
            String p = "FP";
            if (infrequent) p = "IP";
            File out = new File(LOG_DIR + hnName + "/" + p + "_" + threshold + "_" + i + ".png");
            gv.writeGraphToFile(gv.getGraph(graphDot, "png", "dot"), out);
        }

        return graphDot;
    }

    static String translateStats(Graph graph, Boolean removeComplete,
                                 HashMap<String, List<String>> nodesToPaint, Double chunksize, boolean time,
                                 List<String> orderColor, List<List<Object>> activityLongerShorterDuration, List<Object> chunk) {
        String graphDot;

        graphDot = "";

        graphDot += HEAD;
        graphDot += "\n";
        graphDot += "\n";

        graphDot += writeNodesSpecific(nodesToPaint);
        graphDot += "\n";

        graphDot += writeArcs(graph, removeComplete);
        graphDot += "\n";

        graphDot += RANKHEAD;
        graphDot += TABLEHEAD;

        for (int i = 0; i<orderColor.size(); i++) {
            if (time) {
                //Select the best format date
                double v;
                long v2;
                if (chunk != null && chunk.size()>=2) {
                    v2 = (long) chunk.get(0);
                    v = (long) chunk.get(1);
                    chunk.remove(0);
                } else {
                    v = chunksize * (i + 1);
                    v2 = (long) (chunksize * i);
                }
                List<String> times = formatTime(v, v2);
                graphDot += "<TD><b>"+ times.get(1) + " - " + times.get(0) +"</b></TD>";
            } else {
                if (chunk != null && chunk.size() > 1) {
                    graphDot += "<TD>" + formatter.format(chunk.get(0)) + " - " + formatter.format(chunk.get(1)) + "</TD>";
                    chunk.remove(0);
                } else {
                    graphDot += "<TD>" + formatter.format(chunksize * i) + " - " + formatter.format(chunksize * (i + 1)) + "</TD>";
                }
            }
            graphDot += "<TD BGCOLOR=\""+ orderColor.get(i) +"\"></TD>\n";
            graphDot += "<TD></TD>\n";
        }

        graphDot += TABLETAIL;
        graphDot += RANKTAIL;

        if (activityLongerShorterDuration != null) {
            graphDot += RANKHEAD2;
            graphDot += TABLEHEAD;

            graphDot += "<TD></TD><TD></TD><TD><B>Activity</B></TD><TD><B>Duration</B></TD></TR>\n";
            graphDot += "<TR>\n";
            Long v = (Long) activityLongerShorterDuration.get(0).get(1);
            List<String> times = formatTime(v, 0l);
            graphDot += "<TD></TD><TD><B>Longest Activity</B></TD><TD>" + activityLongerShorterDuration.get(0).get(0) + "</TD><TD>" +
                    times.get(0) + "</TD></TR>\n";
            Long v2 = (Long) activityLongerShorterDuration.get(1).get(1);
            times = formatTime(v2, 0l);
            graphDot += "<TR>\n";
            graphDot += "<TD></TD><TD><B>Shortest Activity</B></TD><TD>" + activityLongerShorterDuration.get(1).get(0) + "</TD><TD>" +
                    times.get(0) + "</TD>\n";

            graphDot += TABLETAIL;
            graphDot += RANKTAIL;
        }

        graphDot += "}";
        graphDot += "}";

        //System.out.printf(graphDot);
        return graphDot;
    }

    private static List<String> formatTime(double v, long v2) {
        String time1;
        String time2;
        if (v > 31104000000d) {
            long l = TimeUnit.MILLISECONDS.toDays((long) v);
            //years
            long l1 = l / 365;
            //days
            long l2 = l % 365;
            int aux = (int) l1;
            time1 = String.format("%d", aux) + "y ";
            if (l2 > 0) time1 += String.format("%d", l2) + "d ";
            long h = TimeUnit.MILLISECONDS.toHours(l2) % TimeUnit.HOURS.toMinutes(1);
            if (h > 0) time1 += String.format("%d", h) + "h";

            l = TimeUnit.MILLISECONDS.toDays(v2);
            //years
            l1 = l / 365;
            //days
            l2 = l % 365;
            aux = (int) l1;
            time2 = String.format("%d", aux) + "y ";
            if (l2 > 0) time2 += String.format("%d", l2) + "d ";
            h = TimeUnit.MILLISECONDS.toHours(l2) % TimeUnit.HOURS.toMinutes(1);
            if (h > 0) time2 += String.format("%d", h) + "h";
        } else if (v > 86400000d) {
            //Days
            time1 = String.format("%d", TimeUnit.MILLISECONDS.toDays((long) v)) + "d ";
            long l = TimeUnit.MILLISECONDS.toHours((long) v) % TimeUnit.HOURS.toMinutes(1);
            if (l > 0) time1 += String.format("%d", l) + "h";
            time2 = String.format("%d", TimeUnit.MILLISECONDS.toDays(v2)) + "d ";
            long l2 = TimeUnit.MILLISECONDS.toHours(v2) % TimeUnit.HOURS.toMinutes(1);
            if (l2 > 0) time2 += String.format("%d", l2) + "h";
        } else if (v > 3600000d) {
            //Hours
            time1 = String.format("%d", TimeUnit.MILLISECONDS.toHours((long) v)) + "h ";
            long l = TimeUnit.MILLISECONDS.toMinutes((long) v) % TimeUnit.MINUTES.toSeconds(1);
            if (l > 0) time1 += String.format("%d", l) + "m";
            time2 = String.format("%d", TimeUnit.MILLISECONDS.toHours(v2)) + "h ";
            long l2 = TimeUnit.MILLISECONDS.toMinutes(v2) % TimeUnit.MINUTES.toSeconds(1);
            if (l2 > 0) time2 += String.format("%d", l2) + "m";
        } else if (v > 60000d) {
            //Minutes
            time1 = String.format("%d", TimeUnit.MILLISECONDS.toMinutes((long) v)) + "m ";
            long l = TimeUnit.MILLISECONDS.toSeconds((long) v) % TimeUnit.SECONDS.toMillis(1);
            if (l > 0) time1 += String.format("%d", l) + "s";
            time2 = String.format("%d", TimeUnit.MILLISECONDS.toMinutes(v2)) + "m ";
            long l2 = TimeUnit.MILLISECONDS.toSeconds(v2) % TimeUnit.SECONDS.toMillis(1);
            if (l2 > 0) time2 += String.format("%d", l2) + "s";
        } else {
            //Seconds
            time1 = String.format("%d", TimeUnit.MILLISECONDS.toSeconds((long) v)) + "s ";
            long l = TimeUnit.MILLISECONDS.toMillis((long) v) % TimeUnit.MILLISECONDS.toMicros(1);
            if (l > 0) time1 += String.format("%d", l) + "ms";
            time2 = String.format("%d", TimeUnit.MILLISECONDS.toSeconds(v2)) + "s ";
            long l2 = TimeUnit.MILLISECONDS.toMillis(v2) % TimeUnit.MILLISECONDS.toMicros(1);
            if (l2 > 0) time2 += String.format("%d", l2) + "ms";
        }
        List<String> times = new ArrayList<>();
        times.add(time1);
        times.add(time2);
        return times;
    }

    private static String writeNodesPaint(Graph graph, Boolean removeComplete, List<INode> nodesPattern) {
        List<INode> nodes;
        String nodesString, realName;

        nodesString = "";
        nodes = graph.getNodes();
        for (INode node : nodes) {
            realName = FileTranslator.simplifyName(node.getId(), removeComplete);
            if (nodesPattern.contains(node)) {
                nodesString += checkIsDummyNodePaint(realName, NODE);
            } else{
                nodesString += checkIsDummyNodePaint(realName, OPACITY_NODE);
            }
        }

        return nodesString;
    }

    private static String writeNodesSpecific(HashMap<String, List<String>> nodesToPaint) {
        String nodesString;

        nodesString = "";
        nodesString += checkIsDummyNodePaint(S_DUMMY_TASK, OPACITY_NODE);
        nodesString += checkIsDummyNodePaint(E_DUMMY_TASK, OPACITY_NODE);
        for (Map.Entry<String, List<String>> entry : nodesToPaint.entrySet()) {
            List<String> value = entry.getValue();
            for (int j = 0; j < value.size(); j++) {
                nodesString += value.get(j).replaceAll("\\s", "") + NORMAL_NODE + entry.getKey() + "\",label=\"" + value.get(j) + "\"" + CLOSER;
            }
        }
        return nodesString;
    }

    private static String writeArcsPaint(Graph graph, Boolean removeComplete, Set<Arc> arcsPattern) {
        String nameActualTask, nameActualTaskB, nodesString;

        nodesString = "";
        Set<Arc> arcs = graph.getArcs();
        for (Arc a : arcs) {
            nameActualTask = FileTranslator.simplifyName(a.getSource().getId(), removeComplete);
            nameActualTaskB = FileTranslator.simplifyName(a.getDestination().getId(), removeComplete);
            if (arcsPattern.contains(a)) {
                nodesString += nameActualTask + " -> " + nameActualTaskB + propertiesArc;
            } else {
                nodesString += nameActualTask + " -> " + nameActualTaskB + propertiesArcOpacity;
            }
        }

        return nodesString;
    }

    private static String checkIsDummyNodePaint(String name, String node) {
        return name.replaceAll("\\s", "") + node + "\"" + name + "\"" + CLOSER;
    }

    public static String translateArcsStats(Graph graph, boolean removeComplete, HashMap<String, List<bpm.statistics.Arc>> arcsToPaint,
                                            Double chunksize, List<String> orderColor, boolean printNumber, List<Object> chunk) {
        String graphDot;

        graphDot = "";

        graphDot += HEAD;
        graphDot += "\n";
        graphDot += "\n";

        graphDot += writeNodes(graph, removeComplete);
        graphDot += "\n";

        graphDot += writeArcsPaint2(arcsToPaint, printNumber);
        graphDot += "\n";

        graphDot += RANKHEAD;
        graphDot += TABLEHEAD;

        for (int i=0; i<orderColor.size(); i++) {
            if (chunk != null && chunk.size() > 1) {
                graphDot += "<TD>" + formatter.format(chunk.get(0)) + " - " + formatter.format(chunk.get(1)) + "</TD>";
                chunk.remove(0);
            } else {
                graphDot += "<TD>" + formatter.format(chunksize * i) + " - " + formatter.format(chunksize * (i + 1)) + "</TD>";
            }
            graphDot += "<TD BGCOLOR=\"" + orderColor.get(i) + "\"></TD>\n";
            graphDot += "<TD></TD>\n";
        }

        graphDot += TABLETAIL;
        graphDot += RANKTAIL;

        graphDot += "}";
        graphDot += "}";

        return graphDot;
    }

    public static String translateArcsFrequencies(Graph graph, boolean removeComplete, List<bpm.statistics.Arc> arcs) {
        String graphDot;

        graphDot = "";

        graphDot += HEAD;
        graphDot += "\n";
        graphDot += "\n";

        graphDot += writeNodes(graph, removeComplete);
        graphDot += "\n";

        graphDot += writeArcsPaint3(arcs);
        graphDot += "\n";
        graphDot += "}";

        return graphDot;
    }

    public static String translateArcsPrune(boolean removeComplete, String hnName, String name,
                                                List<bpm.statistics.Arc> arcs, Set<String> nodesToPaint) {
        String graphDot;

        graphDot = "";

        graphDot += HEAD;
        graphDot += "\n";
        graphDot += "\n";

        graphDot += writeNodesSpecific2(removeComplete, nodesToPaint);
        graphDot += "\n";

        graphDot += writeArcsPaint3(arcs);
        graphDot += "\n";
        graphDot += "}";

        if (hnName != null) {
            GraphViz gv = new GraphViz();
            File out = new File(LOG_DIR + hnName + "/" + name + ".png");
            gv.writeGraphToFile(gv.getGraph(graphDot, "png", "dot"), out);
        }

        return graphDot;
    }

    private static String writeNodesSpecific2(boolean removeComplete, Set<String> nodesToPaint) {
        String nodesString, realName;

        nodesString = "";
        Iterator<String> iterator = nodesToPaint.iterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            realName = FileTranslator.simplifyName(next, removeComplete);
            nodesString += checkIsDummyNode(realName);
        }
        return nodesString;
    }

    private static String writeArcsPaint2(HashMap<String, List<bpm.statistics.Arc>> arcsToPaint, boolean number) {
        String nodesString;

        nodesString = "";
        for (Map.Entry<String, List<bpm.statistics.Arc>> entry : arcsToPaint.entrySet()) {
            String color = entry.getKey();
            List<bpm.statistics.Arc> value = entry.getValue();
            for (int i = 0; i < value.size(); i++) {
                if (number) {
                    nodesString += value.get(i).getActivityA() + " -> " + value.get(i).getActivityB() + " [label=\"" + formatter.format(value.get(i).getFrequency())  +
                            "\", color=\"" + color + "\"" + CLOSER;
                } else {
                    nodesString += value.get(i).getActivityA() + " -> " + value.get(i).getActivityB() + NORMAL_ARC + color + "\"" + CLOSER;
                }
            }
        }
        return nodesString;
    }

    private static String writeArcsPaint3(List<bpm.statistics.Arc> arcs) {
        String nodesString;

        nodesString = "";
        for (int i= 0; i < arcs.size(); i++) {
            bpm.statistics.Arc arc = arcs.get(i);
            nodesString += arc.getActivityA() + " -> " + arc.getActivityB() + NAMED_ARC + formatter.format(arc.getFrequency()) + "\"" + CLOSER;
        }
        return nodesString;
    }

    static String translateTraceFrequency(Graph graph, boolean removeComplete, Tuple2<List<Object>, List<Object>> tuple,
                                          Object fre, Object fre2, double notC) {

        String graphDot = "";

        graphDot += HEAD;
        DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols();
        otherSymbols.setDecimalSeparator('.');
        //DecimalFormat df = new DecimalFormat("#0.00", otherSymbols);
        fre = (double) fre * 100d;
        fre = formatter.format(fre);
        double number = notC - (int) fre2;
        graphDot += LABEL + "Frequency : " + fre + "%\\lNº compliant traces: " + formatter.format(fre2) +
                "\\lNº not compliant traces: " + formatter.format(number) + "\";";
        graphDot += "\n";
        graphDot += "\n";

        graphDot += writeNodesTrace(graph, removeComplete, tuple._1);
        graphDot += "\n";

        graphDot += writeArcsTrace(graph, removeComplete, tuple._2);
        graphDot += "\n";
        graphDot += "}";

        return graphDot;
    }

    static String translateCriticalTrace(Graph graph, boolean removeComplete, List<Object> nodes, List<Object> arcs, Long duration) {

        String graphDot;

        graphDot = "";

        graphDot += HEAD;
        List<String> strings = formatTime(duration, 0l);
        graphDot += LABEL + "Duration: " + strings.get(0) + "\";";
        graphDot += "\n";
        graphDot += "\n";

        graphDot += writeNodesTrace(graph, removeComplete, nodes);
        graphDot += "\n";

        graphDot += writeArcsTrace(graph, removeComplete, arcs);
        graphDot += "\n";
        graphDot += "}";

        return graphDot;
    }

    private static String writeNodesTrace(Graph graph, Boolean removeComplete, List<Object> names) {
        List<INode> nodes;
        String nodesString, realName;

        nodesString = "";
        nodes = graph.getNodes();
        for (INode node : nodes) {
            realName = FileTranslator.simplifyName(node.getId(), removeComplete);
            if (names.contains(realName)) {
                nodesString += checkIsDummyNodePaint(realName, NODE);
            } else{
                nodesString += checkIsDummyNodePaint(realName, OPACITY_NODE);
            }
        }

        return nodesString;
    }

    private static String writeArcsTrace(Graph graph, Boolean removeComplete, List<Object> arcs) {
        String nameActualTask, nameActualTaskB, nodesString;

        nodesString = "";
        Set<Arc> arcsGraph = graph.getArcs();
        for (Arc a : arcsGraph) {
            nameActualTask = FileTranslator.simplifyName(a.getSource().getId(), removeComplete);
            nameActualTaskB = FileTranslator.simplifyName(a.getDestination().getId(), removeComplete);
            boolean arc = false;
            for (int j = 0; j < arcs.size(); j++) {
                bpm.statistics.Arc next = (bpm.statistics.Arc) arcs.get(j);
                if (next.getActivityA().equals(a.getSource().getId()) && next.getActivityB().equals(a.getDestination().getId())) {
                    arc = true;
                    //TODO poñer para mellorar eficiencia
                    //arcs.remove(j);
                    break;
                }
            }
            if (arc) {
                nodesString += nameActualTask + " -> " + nameActualTaskB + propertiesArc;
            } else {
                nodesString += nameActualTask + " -> " + nameActualTaskB + propertiesArcOpacity;
            }
        }
        return nodesString;
    }

    public static String translateArcsLoops(Graph graph, boolean removeComplete,List<Object> loops,
                                            List<Object> nodes, List<bpm.statistics.Arc> arcsF, boolean sameNode) {
        String graphDot;

        graphDot = "";

        graphDot += HEAD;
        graphDot += "\n";
        graphDot += "\n";

        graphDot += writeNodesLoops(graph, removeComplete, nodes);
        graphDot += "\n";

        graphDot += writeArcsPaint2(graph, removeComplete, loops, arcsF);
        graphDot += "\n";
        graphDot += "}";

        return graphDot;
    }

    private static String writeNodesLoops(Graph graph, Boolean removeComplete, List<Object> nodesPaint) {
        List<INode> nodes;
        String nodesString, realName;

        nodesString = "";
        nodes = graph.getNodes();
        for (INode node : nodes) {
            realName = FileTranslator.simplifyName(node.getId(), removeComplete);
            if (nodesPaint.contains(node.getId())) {
                nodesString += checkIsDummyNodePaint(realName, NODE);
            } else {
                nodesString += checkIsDummyNodePaint(realName, OPACITY_NODE);
            }
        }

        return nodesString;
    }

    private static String writeArcsPaint2(Graph graph, Boolean removeComplete, List<Object> loops, List<bpm.statistics.Arc> arcsF) {
        String nameActualTask, nameActualTaskB, nodesString;

        nodesString = "";
        Set<Arc> arcs = graph.getArcs();
        for (Arc a : arcs) {
            nameActualTask = FileTranslator.simplifyName(a.getSource().getId(), removeComplete);
            nameActualTaskB = FileTranslator.simplifyName(a.getDestination().getId(), removeComplete);
            bpm.statistics.Arc myArc = new bpm.statistics.Arc(a.getSource().getId(), a.getDestination().getId());
            if (loops.contains(myArc)) {
                Double fre = 0d;
                for (bpm.statistics.Arc f : arcsF) {
                    if (f.getActivityA().equals(a.getSource().getId()) && f.getActivityB().equals(a.getDestination().getId())) {
                        fre = f.getFrequency();
                        break;
                    }
                }
                nodesString += nameActualTask + " -> " + nameActualTaskB + " [label=\"" + formatter.format(fre) + "\"];\n";
            } else {
                nodesString += nameActualTask + " -> " + nameActualTaskB + NAMED_ARC + "\"" + CLOSER;
            }
        }
        return nodesString;
    }
}