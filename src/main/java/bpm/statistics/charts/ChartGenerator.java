package bpm.statistics.charts;

import bpm.log_editor.data_types.MinedLog;
import org.knowm.xchart.*;
import org.knowm.xchart.internal.chartpart.Chart;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.style.markers.SeriesMarkers;

import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ChartGenerator {

    public static CategoryChart BarChart(ArrayList<String> tags, ArrayList<Double> values, String yAxis, String xAxis, String serie) {

        // Create Chart
        CategoryChart chart = new CategoryChartBuilder().width(1000).height(800).xAxisTitle(xAxis).yAxisTitle(yAxis).theme(Styler.ChartTheme.GGPlot2).build();

        //Customize
        chart.getStyler().setXAxisLabelRotation(45);
        chart.getStyler().setXAxisLabelAlignment(Styler.TextAlignment.Right);
        chart.getStyler().setXAxisLabelAlignmentVertical(Styler.TextAlignment.Right);
        chart.getStyler().setLegendVisible(false);

        // Series
        chart.addSeries(serie, tags, values);

        return chart;
    }

    public static CategoryChart ActivityFrequency(MinedLog model) throws IOException {

          Chart chart =  ChartGenerator.BarChart(
                        (ArrayList<String>) model.getActStats().activityFrequency.stream().map(tupla -> tupla._1).collect(Collectors.toList()),
                        (ArrayList<Double>) model.getActStats().activityFrequency.stream().map(tupla -> tupla._2).collect(Collectors.toList()),
                        "Activities",
                        "Number of executions",
                        "Series");

        BitmapEncoder.saveBitmap(chart, model.getFolderName() + "/ActivityFrequencyChart", BitmapEncoder.BitmapFormat.PNG);

        return null;
    }

    public static PieChart PieChart(ArrayList<String> tags, ArrayList<Double> values) {
        // Create Chart
        PieChart chart = new PieChartBuilder().width(1000).height(800).theme(Styler.ChartTheme.XChart).build();

        for (int i=0; i<tags.size(); i++) {
            chart.addSeries(tags.get(i), values.get(i));
        }

        return chart;
    }

    public static CategoryChart StackedBarChart(String yAxis, String xAxis, Map<String, List<String>> mapC, Map<String, Integer> mapW, List<String> categories) {
        CategoryChart chart = new CategoryChartBuilder().width(1000).height(800).xAxisTitle(xAxis).yAxisTitle(yAxis).theme(Styler.ChartTheme.XChart).build();

        //Stacked bar
        chart.getStyler().setStacked(true);

        for (int i=0; i<categories.size(); i++) {
            String s = categories.get(i);
            List<String> list = mapC.get(s);

            for (String w : list) {
                Integer integer = mapW.get(w);
                List<Integer> values = new ArrayList<>();
                //Añadimos vacío en las categorías que no pertenece
                for (int j=0; j<i; j++) {
                    values.add(null);
                }
                values.add(integer);
                //Añadimos vacío en las categorías que no pertenece
                for (int j=values.size(); j<categories.size(); j++) {
                    values.add(null);
                }
                chart.addSeries(w, categories, values);
            }
        }


        // Series
        /*ArrayList<String> es = new ArrayList<>();
        es.add("c1");
        es.add("c2");
        ArrayList<Integer> values = new ArrayList<>();
        values.add(50);
        values.add(null);
        ArrayList<Integer> values2 = new ArrayList<>();
        values2.add(null);
        values2.add(20);
        ArrayList<Integer> values3 = new ArrayList<>();
        values3.add(10);
        values3.add(10);
        chart.addSeries("w1", es, values);
        chart.addSeries("w2", es, values2);
        chart.addSeries("w3", es, values3);*/

        return chart;
    }

    public static XYChart getChart() {

        // Create Chart
        XYChart chart = new XYChartBuilder().width(800).height(600).title("ScatterChart04").xAxisTitle("X").yAxisTitle("Y").build();

        // Customize Chart
        chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        chart.getStyler().setChartTitleVisible(false);
        chart.getStyler().setLegendVisible(false);
        chart.getStyler().setAxisTitlesVisible(false);
        chart.getStyler().setXAxisDecimalPattern("0.0000000");

        // Series
        int size = 10;
        List<Double> xData = new ArrayList();
        List<Double> yData = new ArrayList();
        List<Double> errorBars = new ArrayList();
        for (int i = 0; i <= size; i++) {
            xData.add(((double) i) / 1000000);
            yData.add(10 * Math.exp(-i));
            errorBars.add(Math.random() + .3);
        }
        XYSeries series = chart.addSeries("10^(-x)", xData, yData, errorBars);
        series.setMarkerColor(Color.RED);
        series.setMarker(SeriesMarkers.SQUARE);

        return chart;
    }

    public static CategoryChart ActivityDuration(MinedLog model) throws IOException {

        //Collect values
        ArrayList<String> tags = (ArrayList<String>) model.getActStats().activityAverages.stream().map(tupla -> (String) ((List) tupla).get(0)).collect(Collectors.toList());
        ArrayList<Double> values = (ArrayList<Double>) model.getActStats().activityAverages.stream().map(
                tupla -> ((Long) ((List) tupla).get(1))*1.0d).collect(Collectors.toList());

        //Get total time
        Double total = 0d;
        for(Double value: values){
            total += value;
        }

        //Get percentages
        ArrayList<Double> percentages = new ArrayList<>();
        for(Double value : values){
            percentages.add(value/total*100);
        }


        // Create Chart
        CategoryChart chart = new CategoryChartBuilder().width(1000).height(800).xAxisTitle("Activities").yAxisTitle("Duration percentage").theme(Styler.ChartTheme.GGPlot2).build();

        //Customize
        chart.getStyler().setXAxisLabelAlignment(Styler.TextAlignment.Right);
        chart.getStyler().setXAxisLabelAlignmentVertical(Styler.TextAlignment.Right);
        chart.getStyler().setXAxisLabelRotation(45);

        // Series
        chart.addSeries("Act", tags, percentages);

        BitmapEncoder.saveBitmap(chart, model.getFolderName() + "/ActivityDurationPercentageChart", BitmapEncoder.BitmapFormat.PNG);

        return chart;
    }

    public static List<org.knowm.xchart.XYChart> Area(Map<String, int[]> map, String xAxis, String yAxis, String areaTitle,
                                                      String yAxis2, String areaTitle2,  List<?> dates) {
        List<org.knowm.xchart.XYChart> list = new ArrayList<>();


        // Create Chart
        final org.knowm.xchart.XYChart chart = new XYChartBuilder().width(1000).height(800).title(areaTitle).xAxisTitle(xAxis).yAxisTitle(yAxis).build();

        // Customize Chart
        //chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNE);
        //XYSeries.XYSeriesRenderStyle.Line
        chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Line);
        chart.getStyler().setXAxisLabelRotation(45);
        chart.getStyler().setXAxisLabelAlignment(Styler.TextAlignment.Right);
        chart.getStyler().setXAxisLabelAlignmentVertical(Styler.TextAlignment.Right);

        // Series
        List<Integer> l;
        List<Integer> t = new ArrayList<>();
        int[] total = null;
        for (Map.Entry entry : map.entrySet()) {
            int[] value = (int[]) entry.getValue();
            l = new ArrayList<>();

            if (total == null) {
                total = new int[value.length];
                for (int i=0; i<value.length; i++) {
                    total[i] = value[i];
                    l.add(value[i]);
                }
            } else {
                for (int i=0; i<value.length; i++) {
                    total[i] = total[i] + value[i];
                    l.add(value[i]);
                }
            }

            chart.addSeries((String) entry.getKey(), dates, l);
        }

        for (int i=0; i<total.length; i++) {
            t.add(total[i]);
        }

        chart.addSeries("Total", dates, t);

        final org.knowm.xchart.XYChart chart2 = new XYChartBuilder().width(1000).height(800).title(areaTitle2).xAxisTitle(xAxis).yAxisTitle(yAxis2).build();
        chart2.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Line);
        chart2.getStyler().setXAxisLabelRotation(45);
        chart2.getStyler().setXAxisLabelAlignment(Styler.TextAlignment.Right);
        chart2.getStyler().setXAxisLabelAlignmentVertical(Styler.TextAlignment.Right);

        // Series
        List<Double> l2;
        for (Map.Entry entry : map.entrySet()) {
            int[] value = (int[]) entry.getValue();
            l2 = new ArrayList<>();
            for (int i =0; i<value.length; i++) {
                double i1 = (double) value[i] / (double) t.get(i);
                l2.add(i1 * 100);
            }
            chart2.addSeries((String) entry.getKey(), dates, l2);
        }

        list.add(chart);
        list.add(chart2);

        return list;
    }

    public static org.knowm.xchart.XYChart activitiesLine(Map<Object, Object> map) {
        // Create Chart
        final org.knowm.xchart.XYChart chart = new XYChartBuilder().width(1000).height(800).title("Title").xAxisTitle("Activities Execution Times").yAxisTitle("").build();
        chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Line);

        // Series
        int j = 1;
        for (Map.Entry entry : map.entrySet()) {
            List<List<Object>> value = (List<List<Object>>) entry.getValue();
            List<Long> times = new ArrayList<>();
            for (List<Object> o : value) {
                times.add((long) o.get(1));
            }
            List<Integer> id = new ArrayList<>();
            for (int i=0; i<times.size(); i++) {
                //id.add(Integer.valueOf((String) entry.getKey()));
                id.add(j);
            }
            j++;
            if (j == 20) break;
            chart.addSeries((String) entry.getKey(), times, id);
        }

        return chart;
    }
}
