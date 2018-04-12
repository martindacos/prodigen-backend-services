package bpm.heuristic;

import bpm.log_editor.data_types.Config;
import bpm.log_editor.data_types.MinedLog;
import bpm.log_editor.data_types.Node;
import bpm.log_editor.storage.MongoDAO;
import bpm.statistics.charts.ChartGenerator;
import bpm.statistics.spark.ActivityStatisticsVO;
import bpm.statistics.spark.ArcStatisticsVO;
import bpm.statistics.spark.TraceStatisticsVO;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;
import es.usc.citius.womine.model.Arc;
import es.usc.citius.womine.model.Graph;
import es.usc.citius.womine.model.INode;
import es.usc.citius.womine.model.Pattern;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.XYChart;
import scala.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.stream.Collectors;

import static bpm.log_editor.parser.Constants.LOG_DIR;

public class FileTranslator {
    private final static GraphViz gv = new GraphViz();

    public static String resultsToJSON(Set<Pattern> results, Boolean removeComplete) {
        Integer i, weight, numNodes, numArcs;
        String jsonResults;
        DecimalFormat df;
        DecimalFormatSymbols otherSymbols;

        otherSymbols = new DecimalFormatSymbols();
        otherSymbols.setDecimalSeparator('.');
        df = new DecimalFormat("##0.00", otherSymbols);

        i = 0;
        jsonResults = "[";
        for (Pattern pattern : results) {
            numNodes = pattern.getNodes().size();
            numArcs = pattern.getArcs().size();

            jsonResults += "\n{\n";
            jsonResults += "\"name\": \"Pattern " + i + "\",\n";
            jsonResults += "\"numTasks\": " + numNodes + ",\n";
            jsonResults += "\"frequency\": " + df.format(pattern.getFrequency() * 100) + ",\n";

            weight = numNodes * 2 + numArcs;
            jsonResults += "\"weight\": " + weight + ",\n";
            jsonResults += "\"nodes\": \"";
            for (INode node : pattern.getNodes()) {
                jsonResults += simplifyName(node.getId(), removeComplete) + ",";
            }
            // Delete last comma.
            jsonResults = jsonResults.substring(0, jsonResults.length() - 1);
            jsonResults += "\",\n";
            jsonResults += "\"arcs\": \"";
            for (Arc arc : pattern.getArcs()) {
                jsonResults += simplifyName(arc.getSource().getId(), removeComplete) + "->" + simplifyName(arc.getDestination().getId(), removeComplete) + ",";
            }
            if (pattern.getArcs().size() > 0) {
                // Delete last comma.
                jsonResults = jsonResults.substring(0, jsonResults.length() - 1);
            }
            jsonResults += "\",\n";

            jsonResults += "\"dot\": \"";
            jsonResults += ParserHNGraph.translate(pattern, removeComplete)
                    .replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\t", "\\t");
            jsonResults += "\"\n" +
                    "},";
            i++;
        }
        if (results.size() > 0) {
            // Delete last comma.
            jsonResults = jsonResults.substring(0, jsonResults.length() - 1);
        }
        jsonResults += "\n]\n";

        return jsonResults;
    }

    public static List<String> resultsPatternsToDot(Graph model, Set<Pattern> results, Boolean removeComplete, String hnName,
                                            boolean infrequent, Double threshold, MinedLog r, boolean print) {
        Integer i;
        DecimalFormat df;
        DecimalFormatSymbols otherSymbols;

        otherSymbols = new DecimalFormatSymbols();
        otherSymbols.setDecimalSeparator('.');
        df = new DecimalFormat("##0.00", otherSymbols);

        i = 0;
        List<String> patterns = new ArrayList<>();
        for (Pattern pattern : results) {
            String frequency = df.format(pattern.getFrequency() * 100);
            List<INode> nodesPattern = pattern.getNodes();
            Set<Arc> arcsPattern = pattern.getArcs();
            String p = ParserHNGraph.translatePaint(model, removeComplete, nodesPattern, arcsPattern, i, frequency, hnName, infrequent, threshold, print);
            patterns.add(p);
            i++;
        }

        //Save pattern on DB
        int d = (int) (threshold * 100);
        if (infrequent) {
            r.getIP2().put(d, patterns);
        } else {
            r.getFP2().put(d, patterns);
        }

        return patterns;
    }

    public static byte[] LoadImage(String filePath) throws Exception {
        File file = new File(filePath);
        int size = (int)file.length();
        byte[] buffer = new byte[size];
        FileInputStream in = new FileInputStream(file);
        in.read(buffer);
        in.close();
        return buffer;
    }

    public static void ActivityStatsToDot(Graph model, ActivityStatisticsVO VO, String hnName, Config c, MinedLog r, GridFS fs) {
        String fil = "";
        if (c != null) {
            Node node = r.getNode();
            LinkedHashMap<String, ArrayList<String>> filter = node.getFilter();
            for (Map.Entry<String, ArrayList<String>> entry : filter.entrySet()) {
                fil += entry.getKey() + " " + entry.getValue();
                fil += " > ";
            }
            fil = fil.replaceAll(" > $", "");
        }

        String s = ParserHNGraph.normalTranslate(model, true, hnName, hnName, fil);
        //Save graph on DB
        r.setModel(s);

        List<INode> nodes = model.getNodes();
        List<String> nodesS = new ArrayList<>();
        for (INode n : nodes) {
            nodesS.add(n.getId());
        }
        List<Tuple2<String, Double>> activityFrequency = VO.activityFrequency;
        List<Object> activityAverages = VO.activityAverages;

        heatMapFrequency(model, activityFrequency, hnName, r, c, nodesS);

        List<List<Object>> activityLongerShorterDuration = VO.activityLongerShorterDuration;
        heatMapDuration(model, activityAverages, hnName, activityLongerShorterDuration, r, c, nodesS);

        //Activity frequency
        CategoryChart chart = ChartGenerator.BarChart(
                (ArrayList<String>) VO.activityFrequency.stream().map(tupla -> tupla._1).collect(Collectors.toList()),
                (ArrayList<Double>) VO.activityFrequency.stream().map(tupla -> tupla._2).collect(Collectors.toList())
        , "Executions", "Activities", "Act");

        //Activity Duration
        CategoryChart chart1 = ChartGenerator.BarChart(
                (ArrayList<String>) VO.activityAverages.stream().map(tupla -> (String) ((List) tupla).get(0)).collect(Collectors.toList()),
                (ArrayList<Double>) VO.activityAverages.stream().map(tupla -> (Long) ((List) tupla).get(1) * 1.0d).collect(Collectors.toList())
        , "Duration [ms]", "Activities", "Act");

        String path = LOG_DIR;
        if (c!= null) {
            path = LOG_DIR + hnName;
        }

        try {
            BitmapEncoder.saveBitmapWithDPI(chart, path + "/ActivityFrequencyChart.png", BitmapEncoder.BitmapFormat.PNG,
                    249);

            BitmapEncoder.saveBitmapWithDPI(chart1, path + "/ActivityDurationChart.png", BitmapEncoder.BitmapFormat.PNG,
                    249);

            //Load our image
            byte[] imageBytes = LoadImage(path + "/ActivityFrequencyChart.png");

            //Save image into database
            GridFSInputFile in = fs.createFile(imageBytes);
            in.save();

            r.getActStats().frequencyChart = in.getId();

            //Load our image
            imageBytes = LoadImage(path + "/ActivityDurationChart.png");

            //Save image into database
            in = fs.createFile(imageBytes);
            in.save();

            r.getActStats().durationChart = in.getId();

            if (c==null || !c.isActivity_frequency_heatmap()) {
                File f = new File(path + "/ActivityFrequencyChart.png");
                f.delete();
            }
            if (c==null || !c.isActivity_duration_heatmap()) {
                File f = new File(path + "/ActivityDurationChart.png");
                f.delete();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void heatMapFrequency(Graph model, List<Tuple2<String, Double>> activityFrequency, String hnName, MinedLog r, Config c, List<String> nodesS) {
        Double maxFrequency = activityFrequency.get(0)._2;
        Double chunksize = maxFrequency / 4;
        ArrayList<String> _0 = new ArrayList<>();
        ArrayList<String> _1 = new ArrayList<>();
        ArrayList<String> _2 = new ArrayList<>();
        ArrayList<String> _3 = new ArrayList<>();

        List<String> newCu = new ArrayList<>();
        Map<String, List<String>> repeated = new LinkedHashMap<>();
        double oldF = Double.MIN_VALUE;
        String oldS = "";
        List<String> re = new ArrayList<>();
        Map<String, Double> activityFrequencyMap = new HashMap<>();

        for (int i = 0; i <activityFrequency.size(); i++) {
            if (nodesS.contains(activityFrequency.get(i)._1 + ":complete")) {
                Double f = activityFrequency.get(i)._2;
                if (!f.equals(oldF)) {
                    if (!re.isEmpty()) {
                        repeated.put(oldS, re);
                        re = new ArrayList<>();
                    }
                    oldS = activityFrequency.get(i)._1;
                    newCu.add(activityFrequency.get(i)._1);
                } else {
                    re.add(activityFrequency.get(i)._1);
                }

                oldF = f;
                if (f < chunksize) {
                    _0.add(activityFrequency.get(i)._1);
                } else if (f < chunksize * 2) {
                    _1.add(activityFrequency.get(i)._1);
                } else if (f < chunksize * 3) {
                    _2.add(activityFrequency.get(i)._1);
                } else {
                    _3.add(activityFrequency.get(i)._1);
                }
                activityFrequencyMap.put(activityFrequency.get(i)._1, f);
            }
        }

        if (!re.isEmpty()) {
            repeated.put(oldS, re);
        }

        HashMap<String, List<String>> nodesToPaint = new HashMap<>();
        nodesToPaint.put("#fff3e0", _0);
        nodesToPaint.put("#ffcc80", _1);
        nodesToPaint.put("#ef6c00", _2);
        nodesToPaint.put("#d50000", _3);

        List<String> orderColor = new ArrayList<>();
        orderColor.add("#fff3e0");
        orderColor.add("#ffcc80");
        orderColor.add("#ef6c00");
        orderColor.add("#d50000");

        int size = newCu.size();
        List<List<String>> partitions = new LinkedList<>();

        List<String> first = new ArrayList<>(newCu.subList(0, size / 2));
        List<String> second = new ArrayList<>(newCu.subList(size / 2, size));
        size = first.size();
        List<String> first_1 = new ArrayList<>(first.subList(0, size / 2));
        List<String> second_1 = new ArrayList<>(first.subList(size / 2, size));
        size = second.size();
        List<String> first_2 = new ArrayList<>(second.subList(0, size / 2));
        List<String> second_2 = new ArrayList<>(second.subList(size / 2, size));

        List<Object> chunk = new ArrayList<>();
        partitions.add(second_2);
        if (second_2.size() > 0) {
            chunk.add(activityFrequencyMap.get(second_2.get(second_2.size() - 1)));
            chunk.add(activityFrequencyMap.get(second_2.get(0)));
        }
        partitions.add(first_2);
        if (first_2.size() > 0) {
            chunk.add(activityFrequencyMap.get(first_2.get(0)));
        }
        partitions.add(second_1);
        if (second_1.size() > 0) {
            chunk.add(activityFrequencyMap.get(second_1.get(0)));
        }
        partitions.add(first_1);
        if (first_1.size() > 0) {
            chunk.add(activityFrequencyMap.get(first_1.get(0)));
        }

        ArrayList<String> keysS = new ArrayList<>(repeated.keySet());
        Collections.reverse(keysS);
        String next = "";
        if (keysS.size() > 0) {
            next = keysS.get(0);
            keysS.remove(0);
        }

        HashMap<String, List<String>> nodesToPaint2 = new HashMap<>();
        for (int i=0; i<partitions.size(); i++) {
            List<String> keys = partitions.get(i);
            while (keys.contains(next)) {
                List<String> strings1 = repeated.get(next);
                keys.addAll(strings1);
                if (keysS.size() > 0) {
                    next = keysS.get(0);
                    keysS.remove(0);
                } else {
                    next = "";
                }
            }
            nodesToPaint2.put(orderColor.get(i), keys);
        }

        String activity_frequency_heatMap = ParserHNGraph.translateStats(model, true,  nodesToPaint, chunksize, false, orderColor, null, null);
        String activity_frequency_heatMap2 = ParserHNGraph.translateStats(model, true,  nodesToPaint2, chunksize, false, orderColor, null, chunk);

        if (c!= null && c.isActivity_frequency_heatmap()) {
            File out = new File(LOG_DIR + hnName + "/" + "Activity_Frequency_HeatMap" + ".png");
            if (gv.writeGraphToFile( gv.getGraph(activity_frequency_heatMap, "png", "dot"), out) == -1) {
                System.out.println("ERROR");
            }
            out = new File(LOG_DIR + hnName + "/" + "Activity_Frequency_HeatMap2" + ".png");
            if (gv.writeGraphToFile( gv.getGraph(activity_frequency_heatMap2, "png", "dot"), out) == -1) {
                System.out.println("ERROR");
            }
        }

        //Save graph on DB
        r.getActStats().activityFrequencyHeatMap = activity_frequency_heatMap;
        r.getActStats().activityFrequencyHeatMap2 = activity_frequency_heatMap2;
    }

    public static void heatMapDuration(Graph model, List<Object> activityAverages, String hnName, List<List<Object>> activityLongerShorterDuration,
                                       MinedLog r, Config c, List<String> nodesS) {
        List<Object> list = (List<Object>) activityAverages.get(0);
        Long maxFrequency = (Long) list.get(1);
        Long chunksize = maxFrequency / 4;
        ArrayList<String> _0 = new ArrayList<>();
        ArrayList<String> _1 = new ArrayList<>();
        ArrayList<String> _2 = new ArrayList<>();
        ArrayList<String> _3 = new ArrayList<>();

        List<String> newCu = new ArrayList<>();
        Map<String, List<String>> repeated = new LinkedHashMap<>();
        double oldF = Double.MIN_VALUE;
        String oldS = "";
        List<String> re = new ArrayList<>();
        Map<String, Long> activityLongerShorterDurationMap = new HashMap<>();

        for (int i = 0; i <activityAverages.size(); i++) {
            List<Object> lista = (List<Object>) activityAverages.get(i);
            Long f = (Long) lista.get(1);
            String name = (String) lista.get(0);
            if (nodesS.contains(name + ":complete")) {
                if (!f.equals(oldF)) {
                    if (!re.isEmpty()) {
                        repeated.put(oldS, re);
                        re = new ArrayList<>();
                    }
                    oldS = name;
                    newCu.add(name);
                } else {
                    re.add(name);
                }

                oldF = f;
                if (f < chunksize) {
                    _0.add(name);
                } else if (f < chunksize * 2) {
                    _1.add(name);
                } else if (f < chunksize * 3) {
                    _2.add(name);
                } else {
                    _3.add(name);
                }
                activityLongerShorterDurationMap.put(name, f);
            }
        }

        if (!re.isEmpty()) {
            repeated.put(oldS, re);
        }

        HashMap<String, List<String>> nodesToPaint = new HashMap<>();
        nodesToPaint.put("#fff3e0", _0);
        nodesToPaint.put("#ffcc80", _1);
        nodesToPaint.put("#ef6c00", _2);
        nodesToPaint.put("#d50000", _3);

        List<String> orderColor = new ArrayList<>();
        orderColor.add("#fff3e0");
        orderColor.add("#ffcc80");
        orderColor.add("#ef6c00");
        orderColor.add("#d50000");

        int size = newCu.size();
        List<List<String>> partitions = new LinkedList<>();

        List<String> first = new ArrayList<>(newCu.subList(0, size / 2));
        List<String> second = new ArrayList<>(newCu.subList(size / 2, size));
        size = first.size();
        List<String> first_1 = new ArrayList<>(first.subList(0, size / 2));
        List<String> second_1 = new ArrayList<>(first.subList(size / 2, size));
        size = second.size();
        List<String> first_2 = new ArrayList<>(second.subList(0, size / 2));
        List<String> second_2 = new ArrayList<>(second.subList(size / 2, size));

        List<Object> chunk = new ArrayList<>();
        partitions.add(second_2);
        if (second_2.size() > 0) {
            chunk.add(activityLongerShorterDurationMap.get(second_2.get(second_2.size() - 1)));
            chunk.add(activityLongerShorterDurationMap.get(second_2.get(0)));
        }
        partitions.add(first_2);
        if (first_2.size() > 0) {
            chunk.add(activityLongerShorterDurationMap.get(first_2.get(0)));
        }
        partitions.add(second_1);
        if (second_1.size() > 0) {
            chunk.add(activityLongerShorterDurationMap.get(second_1.get(0)));
        }
        partitions.add(first_1);
        if (first_1.size() > 0) {
            chunk.add(activityLongerShorterDurationMap.get(first_1.get(0)));
        }

        ArrayList<String> keysS = new ArrayList<>(repeated.keySet());
        Collections.reverse(keysS);
        String next = "";
        if (keysS.size() > 0) {
            next = keysS.get(0);
            keysS.remove(0);
        }

        HashMap<String, List<String>> nodesToPaint2 = new HashMap<>();
        for (int i=0; i<partitions.size(); i++) {
            List<String> keys = partitions.get(i);
            while (keys.contains(next)) {
                List<String> strings1 = repeated.get(next);
                keys.addAll(strings1);
                if (keysS.size() > 0) {
                    next = keysS.get(0);
                    keysS.remove(0);
                } else {
                    next = "";
                }
            }
            nodesToPaint2.put(orderColor.get(i), keys);
        }

        String activity_duration_heatMap = ParserHNGraph.translateStats(model, true, nodesToPaint, chunksize.doubleValue(),
                true, orderColor, activityLongerShorterDuration, null);
        String activity_duration_heatMap_2 = ParserHNGraph.translateStats(model, true, nodesToPaint2, chunksize.doubleValue(),
                true, orderColor, activityLongerShorterDuration, chunk);

        if (c!= null && c.isActivity_duration_heatmap()) {
            File out = new File(LOG_DIR + hnName + "/" + "Activity_Duration_HeatMap" + ".png");
            if (gv.writeGraphToFile( gv.getGraph(activity_duration_heatMap, "png", "dot"), out) == -1) {
                System.out.println("ERROR");
            }
            out = new File(LOG_DIR + hnName + "/" + "Activity_Duration_HeatMap2" + ".png");
            if (gv.writeGraphToFile( gv.getGraph(activity_duration_heatMap_2, "png", "dot"), out) == -1) {
                System.out.println("ERROR");
            }
        }

        //Save graph on DB
        r.getActStats().activityDurationHeatMap = activity_duration_heatMap;
        r.getActStats().activityDurationHeatMap2 = activity_duration_heatMap_2;
    }

    public static void ArcStatsToDot(Graph model, ArcStatisticsVO arcStatisticsVO, String hnName, Config c, MinedLog r, GridFS fs) {
        List<bpm.statistics.Arc> arcs = arcStatisticsVO.arcsFrequencyOrdered;

        Double maxFrequency = arcs.get(0).getFrequency();

        String arcs_frequencies_number = ParserHNGraph.translateArcsFrequencies(model, true, arcs);

        //Save on BD
        r.getArcStats().arcsFrequencyNumber = arcs_frequencies_number;

        if (c != null && c.isArcs_frequency_number()) {
            File out = new File(LOG_DIR + hnName + "/" + "Arcs_Frequencies_Number" + ".png");
            gv.writeGraphToFile( gv.getGraph(arcs_frequencies_number, "png", "dot"), out);
        }

        //Arcs Frequencies (HeatMap)
        Double chunksize = maxFrequency / 4;
        ArrayList<bpm.statistics.Arc> _0 = new ArrayList<>();
        ArrayList<bpm.statistics.Arc> _1 = new ArrayList<>();
        ArrayList<bpm.statistics.Arc> _2 = new ArrayList<>();
        ArrayList<bpm.statistics.Arc> _3 = new ArrayList<>();

        List<bpm.statistics.Arc> newCu = new ArrayList<>();
        Map<bpm.statistics.Arc, List<bpm.statistics.Arc>> repeated = new LinkedHashMap<>();
        double oldF = Double.MIN_VALUE;
        bpm.statistics.Arc oldA = null;
        List<bpm.statistics.Arc> re = new ArrayList<>();

        Double totalArcs = 0d;
        for (int i = 0; i <arcs.size(); i++) {
            bpm.statistics.Arc name = arcs.get(i);
            Double f = name.getFrequency();
            totalArcs += f;

            if (!f.equals(oldF)) {
                if (!re.isEmpty()) {
                    repeated.put(oldA, re);
                    re = new ArrayList<>();
                }
                oldA = name;
                newCu.add(name);
            } else {
                re.add(name);
            }

            oldF = f;
            if (f < chunksize) {
                _0.add(name);
            } else if (f < chunksize * 2) {
                _1.add(name);
            } else if (f < chunksize * 3) {
                _2.add(name);
            } else {
                _3.add(name);
            }
        }

        if (!re.isEmpty()) {
            repeated.put(oldA, re);
        }

        HashMap<String, List<bpm.statistics.Arc>> arcsToPaint = new HashMap<>();
        arcsToPaint.put("#c1b29b", _0);
        arcsToPaint.put("#ffcc80", _1);
        arcsToPaint.put("#ef6c00", _2);
        arcsToPaint.put("#d50000", _3);

        List<String> orderColor = new ArrayList<>();
        orderColor.add("#c1b29b");
        orderColor.add("#ffcc80");
        orderColor.add("#ef6c00");
        orderColor.add("#d50000");

        int size = newCu.size();
        List<List<bpm.statistics.Arc>> partitions = new LinkedList<>();

        List<bpm.statistics.Arc> first = new ArrayList<>(newCu.subList(0, size / 2));
        List<bpm.statistics.Arc> second = new ArrayList<>(newCu.subList(size / 2, size));
        size = first.size();
        List<bpm.statistics.Arc> first_1 = new ArrayList<>(first.subList(0, size / 2));
        List<bpm.statistics.Arc> second_1 = new ArrayList<>(first.subList(size / 2, size));
        size = second.size();
        List<bpm.statistics.Arc> first_2 = new ArrayList<>(second.subList(0, size / 2));
        List<bpm.statistics.Arc> second_2 = new ArrayList<>(second.subList(size / 2, size));

        List<Object> chunk = new ArrayList<>();
        partitions.add(second_2);
        if (second_2.size() > 0) {
            chunk.add(second_2.get(second_2.size() - 1).getFrequency());
            chunk.add(second_2.get(0).getFrequency());
        }
        partitions.add(first_2);
        if (first_2.size() > 0) {
            chunk.add(first_2.get(0).getFrequency());
        }
        partitions.add(second_1);
        if (second_1.size() > 0) {
            chunk.add(second_1.get(0).getFrequency());
        }
        partitions.add(first_1);
        if (first_1.size() > 0) {
            chunk.add(first_1.get(0).getFrequency());
        }

        ArrayList<bpm.statistics.Arc> keysS = new ArrayList<>(repeated.keySet());
        Collections.reverse(keysS);
        bpm.statistics.Arc next = null;
        if (keysS.size() > 0) {
            next = keysS.get(0);
            keysS.remove(0);
        }

        HashMap<String, List<bpm.statistics.Arc>> arcsToPaint2 = new HashMap<>();
        for (int i=0; i<partitions.size(); i++) {
            List<bpm.statistics.Arc> keys = partitions.get(i);
            while (next != null && keys.contains(next)) {
                List<bpm.statistics.Arc> strings1 = repeated.get(next);
                keys.addAll(strings1);
                if (keysS.size() > 0) {
                    next = keysS.get(0);
                    keysS.remove(0);
                } else {
                    next = null;
                }
            }
            arcsToPaint2.put(orderColor.get(i), keys);
        }

        String arcs_frequencies = ParserHNGraph.translateArcsStats(model, true, arcsToPaint, chunksize, orderColor, false, null);
        String arcs_frequencies_color_number = ParserHNGraph.translateArcsStats(model, true, arcsToPaint, chunksize, orderColor, true, null);

        List<Object> chunk2 = new ArrayList<>(chunk);
        String arcs_frequencies2 = ParserHNGraph.translateArcsStats(model, true, arcsToPaint2, chunksize, orderColor, false, chunk);
        String arcs_frequencies_color_number2 = ParserHNGraph.translateArcsStats(model, true, arcsToPaint2, chunksize, orderColor, true, chunk2);

        //Save on BD
        r.getArcStats().arcsFrequencies = arcs_frequencies;
        r.getArcStats().arcsFrequencyColorNumber = arcs_frequencies_color_number;
        r.getArcStats().arcsFrequencies2 = arcs_frequencies2;
        r.getArcStats().arcsFrequencyColorNumber2 = arcs_frequencies_color_number2;

        if (c != null && c.isArcs_frequency()) {
            File out = new File(LOG_DIR + hnName + "/" + "Arcs_Frequencies" + ".png");
            gv.writeGraphToFile( gv.getGraph(arcs_frequencies, "png", "dot"), out);

            out = new File(LOG_DIR + hnName + "/" + "Arcs_Frequencies_Color_Number" + ".png");
            gv.writeGraphToFile( gv.getGraph(arcs_frequencies_color_number, "png", "dot"), out);

            out = new File(LOG_DIR + hnName + "/" + "Arcs_Frequencies2" + ".png");
            gv.writeGraphToFile( gv.getGraph(arcs_frequencies2, "png", "dot"), out);

            out = new File(LOG_DIR + hnName + "/" + "Arcs_Frequencies_Color_Number2" + ".png");
            gv.writeGraphToFile( gv.getGraph(arcs_frequencies_color_number2, "png", "dot"), out);
        }

        if (c != null) {
            doPrune(c, arcs, hnName);
        }

        List<List<Object>> arcsLoops = arcStatisticsVO.arcsLoops;
        List<Object> loops = arcsLoops.get(0);
        List<Object> end_loops = arcsLoops.get(1);
        List<Object> nodes = arcsLoops.get(2);
        List<Object> nodes2 = arcsLoops.get(3);

        String arcs_loops = ParserHNGraph.translateArcsLoops(model, true, loops, nodes, arcs, true);
        String arcs_loops_2 = ParserHNGraph.translateArcsLoops(model, true, end_loops, nodes2, arcs, false);

        String path = LOG_DIR;
        if (c!= null) {
            path = LOG_DIR + hnName;
        }

        //TODO Save graphs on BD
        try {
            if (loops.size() > 0) {
                ArrayList tags = new ArrayList();
                ArrayList values = new ArrayList();
                ArrayList values2 = new ArrayList();

                Set<Arc> arcs2 = model.getArcs();
                for (Arc a : arcs2) {
                    String nameActualTask = FileTranslator.simplifyName(a.getSource().getId(), true);
                    bpm.statistics.Arc myArc = new bpm.statistics.Arc(a.getSource().getId(), a.getDestination().getId());
                    if (loops.contains(myArc)) {
                        Double fre = 0d;
                        for (bpm.statistics.Arc f : arcs) {
                            if (f.getActivityA().equals(a.getSource().getId()) && f.getActivityB().equals(a.getDestination().getId())) {
                                fre = f.getFrequency();
                                break;
                            }
                        }
                        values.add(fre);
                        double v = fre / totalArcs;
                        double v1 = v * 100;
                        values2.add(v1);
                        tags.add(nameActualTask);
                    }
                }

                CategoryChart chart = ChartGenerator.BarChart(tags, values, "Executions", "Activities", "Act");

                BitmapEncoder.saveBitmapWithDPI(chart, path + "/Arcs_LoopsChart.png", BitmapEncoder.BitmapFormat.PNG,
                        249);

                //Load our image
                byte[] imageBytes = LoadImage(path + "/Arcs_LoopsChart.png");

                //Save image into database
                GridFSInputFile in = fs.createFile(imageBytes);
                in.save();
                r.getArcStats().arcsLoopsChart = in.getId();

                CategoryChart chart2 = ChartGenerator.BarChart(tags, values2, "% Executions", "Activities", "Act");
                BitmapEncoder.saveBitmapWithDPI(chart2, path + "/Arcs_LoopsChartP.png", BitmapEncoder.BitmapFormat.PNG,
                        249);

                //XYChart area = ChartGenerator.getChart();
                //BitmapEncoder.saveBitmapWithDPI(area, LOG_DIR + hnName + "/Prueba.png", BitmapEncoder.BitmapFormat.PNG, 249);

                //Load our image
                imageBytes = LoadImage(path + "/Arcs_LoopsChartP.png");

                //Save image into database
                in = fs.createFile(imageBytes);
                in.save();
                r.getArcStats().arcsLoopsChartP = in.getId();

                if (c==null || !c.isArcs_loops()) {
                    File f = new File(path+ "/Arcs_LoopsChart.png");
                    f.delete();

                    f = new File(path + "/Arcs_LoopsChartP.png");
                    f.delete();
                }
            }

            if (end_loops.size() > 0) {
                ArrayList tags = new ArrayList();
                ArrayList values = new ArrayList();
                ArrayList values2 = new ArrayList();

                Set<Arc> arcs2 = model.getArcs();
                for (Arc a : arcs2) {
                    String nameActualTask = FileTranslator.simplifyName(a.getSource().getId(), true);
                    bpm.statistics.Arc myArc = new bpm.statistics.Arc(a.getSource().getId(), a.getDestination().getId());
                    if (end_loops.contains(myArc)) {
                        Double fre = 0d;
                        for (bpm.statistics.Arc f : arcs) {
                            if (f.getActivityA().equals(a.getSource().getId()) && f.getActivityB().equals(a.getDestination().getId())) {
                                fre = f.getFrequency();
                                break;
                            }
                        }
                        values.add(fre);
                        double v = fre / totalArcs;
                        double v1 = v * 100;
                        values2.add(v1);
                        tags.add(nameActualTask);
                    }
                }

                CategoryChart chart2 = ChartGenerator.BarChart(tags, values, "Executions","Activities", "Act");
                BitmapEncoder.saveBitmapWithDPI(chart2, path + "/Arcs_LoopsChart_2.png", BitmapEncoder.BitmapFormat.PNG,
                        249);

                //Load our image
                byte[] imageBytes = LoadImage(path + "/Arcs_LoopsChart_2.png");

                //Save image into database
                GridFSInputFile in = fs.createFile(imageBytes);
                in.save();
                r.getArcStats().arcsLoopsChart2 = in.getId();

                CategoryChart chart = ChartGenerator.BarChart(tags, values2, "% Executions", "Activities", "Act");
                BitmapEncoder.saveBitmapWithDPI(chart, path + "/Arcs_LoopsChart_2P.png", BitmapEncoder.BitmapFormat.PNG,
                        249);

                //Load our image
                imageBytes = LoadImage(path + "/Arcs_LoopsChart_2P.png");

                //Save image into database
                in = fs.createFile(imageBytes);
                in.save();
                r.getArcStats().arcsLoopsChartP2 = in.getId();

                if (c==null || !c.isArcs_loops()) {
                    File f = new File(path + "/Arcs_LoopsChart_2.png");
                    f.delete();

                    f = new File(path + "/Arcs_LoopsChart_2P.png");
                    f.delete();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        //Save on BD
        r.getArcStats().arcsLoopsGraph = arcs_loops;
        r.getArcStats().arcsLoopsGraph2 = arcs_loops_2;

        if (c!= null && c.isArcs_loops()) {
            File out = new File(LOG_DIR + hnName + "/" + "Arcs_Loops" + ".png");
            gv.writeGraphToFile(gv.getGraph(arcs_loops, "png", "dot"), out);

            out = new File(LOG_DIR + hnName + "/" + "Arcs_Loops_2" + ".png");
            gv.writeGraphToFile(gv.getGraph(arcs_loops_2, "png", "dot"), out);
        }
    }

    public static void doPrune(Config c, List<bpm.statistics.Arc> arcs, String hnName) {
        Double maxFrequency = arcs.get(0).getFrequency();
        List<String> list1 = Arrays.asList(c.getPrune_Arcs().split(","));
        List<Double> prunado = MongoDAO.listToDoubleList(list1);
        if (c != null && prunado.size() > 0) {
            List<List<bpm.statistics.Arc>> pruneArcs = new ArrayList<>();
            List<Set<String>> pruneNodes = new ArrayList<>();

            for (int j = 0; j < prunado.size(); j++) {
                Double arcFrequencyLimit = maxFrequency * prunado.get(j);
                List<bpm.statistics.Arc> pArcs = new ArrayList<>();
                Set<String> pNodes = new HashSet<>();
                for (int i = 0; i < arcs.size(); i++) {
                    bpm.statistics.Arc name = arcs.get(i);
                    Double f = name.getFrequency();
                    if (f > arcFrequencyLimit) {
                        pArcs.add(name);
                        pNodes.add(name.getActivityA());
                        pNodes.add(name.getActivityB());
                    } else {
                        //No tiene sentido seguir explorando porque estan ordenados
                        break;
                    }
                }
                pruneArcs.add(pArcs);
                pruneNodes.add(pNodes);
            }

            for (int i=0; i<pruneArcs.size(); i++) {
                ParserHNGraph.translateArcsPrune(true, hnName, "Arcs_Prune_" + prunado.get(i), pruneArcs.get(i), pruneNodes.get(i));
            }
        }
    }

    public static String doTHPrune(List<bpm.statistics.Arc> arcs, double th) {
        Double maxFrequency = arcs.get(0).getFrequency();

        List<List<bpm.statistics.Arc>> pruneArcs = new ArrayList<>();
        List<Set<String>> pruneNodes = new ArrayList<>();

        Double arcFrequencyLimit = maxFrequency * th;
        List<bpm.statistics.Arc> pArcs = new ArrayList<>();
        Set<String> pNodes = new HashSet<>();
        for (int i = 0; i < arcs.size(); i++) {
            bpm.statistics.Arc name = arcs.get(i);
            Double f = name.getFrequency();
            if (f > arcFrequencyLimit) {
                pArcs.add(name);
                pNodes.add(name.getActivityA());
                pNodes.add(name.getActivityB());
            } else {
                //No tiene sentido seguir explorando porque estan ordenados
                break;
            }
        }
        pruneArcs.add(pArcs);
        pruneNodes.add(pNodes);

        String s = ParserHNGraph.translateArcsPrune(true, null, null, pruneArcs.get(0), pruneNodes.get(0));
        return s;
    }

    public static void TraceStatsToDot(Graph model, TraceStatisticsVO VO, String hnName, Config c, MinedLog r) {
        List<Tuple2<List<Object>, List<Object>>> traceArcsFrequency = VO.traceArcsFrequency;
        List<Tuple2<List<Object>, Object>> traceRelativeFrequency = VO.traceRelativeFrequency;
        List<Tuple2<List<Object>, Object>> traceFrequency = VO.traceFrequency;
        List<List<Object>> traceLongerShorterDuration = VO.traceLongerShorterDuration;

        int i = 1;
        List<String> mostF = new ArrayList<>();
        for (Tuple2<List<Object>, List<Object>> tuple : traceArcsFrequency) {
            Tuple2<List<Object>, Object> fre = traceRelativeFrequency.get(i - 1);
            Tuple2<List<Object>, Object> fre2 = traceFrequency.get(i - 1);
            String mostFrequentPath = ParserHNGraph.translateTraceFrequency(model, true, tuple, fre._2, fre2._2, VO.tracesCount);

            //Save on the List to save on BD
            mostF.add(mostFrequentPath);

            if (c!= null && c.isMost_frequent_path()) {
                File out = new File(LOG_DIR + hnName + "/" + "MostFrequentPath" + "_"+ i +".png");
                gv.writeGraphToFile( gv.getGraph(mostFrequentPath, "png", "dot"), out);
            }
            i++;
        }

        //Save on BD
        r.getTraceStats().mostFrequentPath = mostF;

        List<Object> objects = traceLongerShorterDuration.get(0);
        List<Object> listaNodes = (List<Object>) objects.get(0);
        List<Object> listaArcs = (List<Object>) objects.get(3);
        Long duration = (Long) traceLongerShorterDuration.get(0).get(1);

        String critical_path = ParserHNGraph.translateCriticalTrace(model, true, listaNodes, listaArcs, duration);

        //Save on BD
        r.getTraceStats().criticalPath = critical_path;

        if (c!= null && c.isCritical_path()) {
            File out = new File(LOG_DIR + hnName + "/" + "CriticalPath" + ".png");
            gv.writeGraphToFile( gv.getGraph(critical_path, "png", "dot"), out);
        }

        Map<Object, Object> tracesActivitiesTimes = VO.tracesActivitiesTimes;
        XYChart area = ChartGenerator.activitiesLine(tracesActivitiesTimes);
        try {
            BitmapEncoder.saveBitmapWithDPI(area, LOG_DIR + hnName + "/activitiesChart.png", BitmapEncoder.BitmapFormat.PNG,
                    249);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String simplifyName(String name, Boolean removeComplete) {
        String simplifiedName = removeComplete
                ? name.replace(":complete", "").replace(":COMPLETE", "")
                : name;

        return simplifiedName.replaceAll("\\s", "_").replaceAll("-", "_").replaceAll(":", "__");
    }
}
