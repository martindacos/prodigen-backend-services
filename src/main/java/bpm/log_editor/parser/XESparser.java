package bpm.log_editor.parser;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.in.XesXmlParser;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class XESparser {

    public static String read(File file, String fileName) {
        //Create file writer
        PrintWriter writer = null;
        try {
            writer = new PrintWriter("upload-dir/" + fileName, "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        XesXmlParser reader = new XesXmlParser();
        //Write header
        writer.print("case,event,completeTime\n");

        try {
            List<XLog> parser = reader.parse(file);
            XLog traces = parser.get(0);
            int numCases = traces.size();
            for (int caseIndex = 0; caseIndex < numCases; caseIndex++) {
                XTrace trace = traces.get(caseIndex);
                XAttribute caseName = trace.getAttributes().get("concept:name");
                String oldName = caseName.toString();
                String name = oldName;
                if (oldName.contains("caso")) {
                    String replace = oldName.replace("caso", "");
                    if (replace.contains(".")) {
                        name = replace.replace(".", "");
                    } else {
                        name = replace;
                    }
                } else {
                    if (oldName.contains(".")) {
                        name = oldName.replace(".", "0");
                    }
                }
                int traceSize = trace.size();
                for (int taskIndex = 0; taskIndex < traceSize; taskIndex++) {
                    String taskName = trace.get(taskIndex).getAttributes().get(XConceptExtension.KEY_NAME).toString();
                    String dateText = trace.get(taskIndex).getAttributes().get("time:timestamp").toString();
                    String[] split = dateText.split("T");
                    String[] split2 = split[1].split("\\+");
                    if (split2[0].contains(".")) {
                        String replace = split[0].replace("-", "/");
                        String[] split22 = split2[0].split("\\.");
                        //System.out.println(caseName.toString() + " " +  taskName + " " + split[0] + " " + split22[0]);
                        writer.print(name + "," + taskName + "," + replace + " " + split22[0] + "\n");
                    } else {
                        String replace = split[0].replace("-", "/");
                        //System.out.println(caseName.toString() + " " +  taskName + " " + split[0] + " " + split2[0]);
                        writer.print(name + "," + taskName + "," + replace + " " + split2[0] + "\n");
                    }
                }
            }
            writer.close();
        } catch (Exception ex) {
            System.out.println("Exception XESparser");
        }
        return fileName;
    }
}
