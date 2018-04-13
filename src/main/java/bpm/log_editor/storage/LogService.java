package bpm.log_editor.storage;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import bpm.log_editor.data_types.LogFile;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static bpm.log_editor.parser.Constants.*;
import static bpm.log_editor.storage.MongoDAO.db;

public class LogService {

    private static LogRepository repo;
    public static StorageService storageService;
    private static ArrayList<LogFile> logFiles = new ArrayList<>();

    public static void init(LogRepository repository) {
        //Init repo
        repo = repository;

        //Select all files from collection
        logFiles = (ArrayList<LogFile>) repo.findAll();
    }

    public static LogFile insertLog(String name, String path) {
        //Insert log
        repo.save(new LogFile(null, name, path, name, null, null, null, null, LOADED, null, null));

        //Return created log
        return null;
    }

    public static void deleteLog(LogFile logFile) throws IOException {

        //Delete file
        File file = new File(storageService.load(logFile.getPath()).toString());

        if (file.delete()) {
            System.out.println(file.getName() + " is deleted!");
        } else {
            System.out.println("File not found");
            throw new IOException();
        }

        for (int i=0; i<logFile.getModels().size(); i++) {
            deleteOneLog(logFile, i);

            //Delete csv file
            String nameFile = logFile.getModels().get(i).getModelFile().toString();

            //Delete csv coll
            String[] split2 = nameFile.split("/");
            String[] nameColl2 = split2[1].split(".csv");

            if (i == 0) {
                //Delete initial csv coll
                String[] split = nameColl2[0].split("0");
                DBCollection l = db.getCollection(split[0]);
                l.drop();
            }
        }

        //Delete images folder
        File f = new File(LOG_DIR + logFile.getName());
        if (f.exists()) {
            FileUtils.forceDelete(f);
        }

        //Delete MongoDB Coll
        logFile.dropColl();

        //Delete logFile from coll
        repo.delete(logFile);
    }

    public static void deleteOneLog(LogFile logFile, Integer i) {
        //Delete csv file
        String nameFile = logFile.getModels().get(i).getModelFile().toString();
        File file = new File(nameFile);
        if (file.delete()) {
            System.out.println(file.getName() + " is deleted!");
        }

        //Delete hn file
        String hnFile = logFile.getModels().get(i).getHn().toString();
        file = new File(hnFile);
        if (file.delete()) {
            System.out.println(file.getName() + " is deleted!");
        }

        //Delete images folder
        try {
            if (logFile.getModels().get(i).getFolderName() != null) {
                File f = new File(logFile.getModels().get(i).getFolderName());
                if (f.exists()) {
                    FileUtils.forceDelete(f);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            //Delete last config
            logFile.setLastConfigNull();

            //Delete hn coll
            DBCollection models = db.getCollection("models");
            BasicDBObject query = new BasicDBObject("name", logFile.getName() + i);
            models.remove(query);

            //Delete csv coll
            String[] split2 = nameFile.split("/");
            String[] nameColl2 = split2[1].split(".csv");
            DBCollection logs = db.getCollection(nameColl2[0]);
            logs.drop();

            //Delete traces coll
            DBCollection traces = db.getCollection(nameColl2[0] + TRACES_SUF);
            //Delete cases coll
            DBCollection cases = db.getCollection(nameColl2[0] + CASES_SUF);
            traces.drop();
            cases.drop();

            MongoDAO.deleteImg(logFile.getModels().get(i).getActStats().frequencyChart);
            MongoDAO.deleteImg(logFile.getModels().get(i).getActStats().durationChart);

            MongoDAO.deleteImg(logFile.getModels().get(i).getArcStats().arcsLoopsChart);
            MongoDAO.deleteImg(logFile.getModels().get(i).getArcStats().arcsLoopsChart2);

            MongoDAO.deleteImg(logFile.getModels().get(i).getArcStats().arcsLoopsChartP);
            MongoDAO.deleteImg(logFile.getModels().get(i).getArcStats().arcsLoopsChartP2);
        }
    }

    public static ArrayList<LogFile> getLogFiles() {
        return logFiles = (ArrayList<LogFile>) repo.findAll();
    }

    public static LogFile getLogByName(String name) {
        return repo.findByName(name);
    }

    public static LogFile save(LogFile logFile) {
        return repo.save(logFile);
    }

    public static CommandResult getLogStats() {
        CommandResult resultSet;
        return resultSet = db.getCollection("collectionName").getStats();
    }


}
