package bpm.log_editor.parser;


import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import bpm.log_editor.data_types.Headers;
import bpm.log_editor.data_types.LogFile;
import bpm.log_editor.storage.MongoDAO;
import bpm.log_editor.storage.StorageService;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.bson.Document;
import bpm.statistics.spark.SparkConnection;

import java.io.*;

import java.text.Normalizer;
import java.util.*;

import static bpm.log_editor.parser.Constants.COMPLETE_TIME;
import static bpm.log_editor.parser.Constants.INITIAL_TIME;

public class CSVparser {


    public static StorageService storageService;

    public static Headers getHeaders(String file) {

        //System.out.println("FILE PATH: " + file);

        String line = "";
        String cvsSplitBy = ",";
        ArrayList<String> result = new ArrayList<>();

        //System.out.println("FILE PATH LOADED:" + storageService.load(file).toString());

        try (BufferedReader br = new BufferedReader(new FileReader(storageService.load(file).toString()))) {

            //If file is not empty
            if ((line = br.readLine()) != null) {
                String[] columns = line.split(cvsSplitBy);

                //Add headers to array and remove quotes
                for (String header : columns)
                    result.add(header.replace("\"", ""));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        Headers h = new Headers();
        h.setData(result);
        return h;
    }

    public static HashMap<String, ArrayList<String>> removeColumns(LogFile logFile, Headers headers) {

        String file = logFile.getPath();
        String name = logFile.getName();

        //Load file and get path
        String filePath = storageService.load(file).toString();
        String archivePath = filePath.replace(file, "");

        //Get filename and create new fileName
        String f = file.substring(0, file.lastIndexOf('.'));
        String fileExt = file.substring(file.lastIndexOf("."), file.length());
        String newFilePath = archivePath + f + "Parsed" + fileExt;
        logFile.setPath(f + "Parsed" + fileExt);

        //Create new file
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(newFilePath, "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        //Create Mongo collection
        DBCollection coll;
        if (MongoDAO.db.collectionExists(name)) {
            coll = MongoDAO.db.getCollection(name);
            coll.drop();
        }


        coll = MongoDAO.db.createCollection(name, null);


        //Get headers to delete indexes
        ArrayList<Integer> indexes = new ArrayList<>();
        List<String> originalHeaders = logFile.getHeaders().getData();
        //ArrayList<String> newHeaders = new ArrayList<>();
        for (String header : headers.getData()) {
            if (originalHeaders.contains(header)) {
                indexes.add(originalHeaders.indexOf(header));
                //newHeaders.add(originalHeaders.get(originalHeaders.indexOf(header)));
            }
        }

        //Rewrite new file
        String line = "";
        ArrayList<String> result = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {

            //Headers
            if ((line = br.readLine()) != null) {

                //For each column of the file
                String[] columns = line.split(",");
                for (int i = 0; i < columns.length; i++) {
                    if (indexes.contains(i)) {

                        //Write to new file
                        writer.print(columns[i]);

                        if (i < columns.length - 1) {
                            writer.print(",");
                        }
                    }
                }

                writer.print("\n");
            }

            //Map<String, Integer> cases = new HashMap<>();
            //Integer indice = 0;

            //For each line of the file
            while ((line = br.readLine()) != null) {

                //Create doc
                BasicDBObject doc = new BasicDBObject();

                //For each column of the file
                String[] columns = line.split(",");
                for (int i = 0; i < columns.length; i++) {
                    if (indexes.contains(i)) {

                        //Write to new file
                        writer.print(columns[i]);

                        //Add to mongo doc and remove quotes
                        String data = columns[i];
                        data = data.replace("\"", "");

                        /*if (i == 0) {
                            if (cases.containsKey(data)) {
                                doc.append("caseIndex" , cases.get(data));
                            } else {
                                doc.append("caseIndex", indice);
                                cases.put(data, indice);
                                indice++;
                            }
                        }*/

                        //If column is empty, add a dash
                        if (!data.equals("")) {
                            doc.append(originalHeaders.get(i), data);
                        } else {
                            doc.append(originalHeaders.get(i), "-");
                        }

                        //Add a colon after each column
                        if (i < columns.length - 1) {
                            writer.print(",");
                        }
                    }
                }

                //Insert Mongo doc
                MongoDAO.insertDocument(coll, doc);

                writer.print("\n");
            }

            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        //Get unique values
        /*HashMap<String, ArrayList<String>> data = new HashMap<>();

        //Create index for each field
        for (String header : headers.getData()) {
            coll.createIndex(header);
            data.put(header, (ArrayList<String>) coll.distinct(header));
        }*/

        //delete old rename new
        storageService.load(file).toFile().delete();

        return null;
    }

    public static void removeColumnsSpark(LogFile logFile, Headers headers) {
        String file = logFile.getPath();
        String name = logFile.getName();
        String filePath = storageService.load(file).toString();

        JavaRDD<String> content = SparkConnection.getContext(name).textFile(filePath);
        //System.out.println("Número particiones archivo: " + content.getNumPartitions());

        //List<String> collect = content.collect();
        /*for (String s: collect) {
            System.out.println(s);
        }*/

        //Get headers to delete indexes
        ArrayList<Integer> indexes = new ArrayList<>();
        List<String> originalHeaders = logFile.getHeaders().getData();
        for (String header : headers.getData()) {
            if (originalHeaders.contains(header)) {
                indexes.add(originalHeaders.indexOf(header));
            }
        }

        List<String> take = content.take(1);
        String fileHeader = take.get(0);

        JavaRDD<Document> map = content.map(new Function<String, Document>() {
            @Override
            public Document call(String v1) throws Exception {
                Document doc = new Document();
                if (!v1.equals(fileHeader)) {

                    String[] columns = v1.split(",");
                    for (int i = 0; i < columns.length; i++) {
                        if (indexes.contains(i)) {

                            //Add to mongo doc and remove quotes
                            String data = columns[i];
                            data = data.replace("\"", "");
                            //data = data.replace(" ", "");
                            data = stripAccents(data);
                            //data = data.toLowerCase();
                            if (i==0 && data.contains(".")) {
                                data = data.replace(".", "0");
                            }

                            //If column is empty, add a dash
                            if (!data.equals("")) {
                                doc.append(originalHeaders.get(i), data);
                            } else {
                                doc.append(originalHeaders.get(i), "-");
                            }
                        }
                    }
                }
                return doc;
            }
        });

        //TODO we need to delete the empty doc
        JavaRDD<Document> finalMap = map.filter(new Function<Document, Boolean>() {
            @Override
            public Boolean call(Document v1) throws Exception {
                return !v1.isEmpty();
            }
        });


        /*List<Document> collect1 = map.collect();
        for (Document s: collect1) {
            System.out.println(s);
        }*/

        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", name);
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(SparkConnection.getContext()).withOptions(writeOverrides);

        MongoSpark.save(finalMap, writeConfig);

        DBCollection coll = MongoDAO.db.getCollection(name);

        //Create index for each field
        for (String header : headers.getData()) {
            coll.createIndex(header);
        }
    }

    public static void savedParsed(String name, String path) {
        JavaRDD<String> content = SparkConnection.getContext(name).textFile(path);

        Headers headers = CSVparser.getHeaders(name);
        List<String> originalHeaders = headers.getData();

        List<String> take = content.take(1);
        String fileHeader = take.get(0);

        JavaRDD<Document> map = content.map(new Function<String, Document>() {
            @Override
            public Document call(String v1) throws Exception {
                Document doc = new Document();
                if (!v1.equals(fileHeader)) {
                    String[] columns = v1.split(",");
                    for (int i = 0; i < columns.length; i++) {
                        //Add to mongo doc and remove quotes
                        String data = columns[i];
                        data = data.replace("\"", "");
                        if (i == 0 && data.contains(".")) {
                            data = data.replace(".", "0");
                        }
                        if (originalHeaders.get(i).equals(INITIAL_TIME) || originalHeaders.get(i).equals(COMPLETE_TIME)) {
                            doc.append(originalHeaders.get(i), Long.parseLong(data));
                        } else {
                            doc.append(originalHeaders.get(i), data);
                        }
                    }
                }
                return doc;
            }
        });

        JavaRDD<Document> finalMap = map.filter(new Function<Document, Boolean>() {
            @Override
            public Boolean call(Document v1) throws Exception {
                return !v1.isEmpty();
            }
        });

        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", name);
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(SparkConnection.getContext()).withOptions(writeOverrides);

        MongoSpark.save(finalMap, writeConfig);
    }

    public static String stripAccents(String s)
    {
        s = Normalizer.normalize(s, Normalizer.Form.NFD);
        s = s.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
        return s;
    }
}