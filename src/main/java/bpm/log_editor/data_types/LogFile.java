package bpm.log_editor.data_types;

import com.mongodb.DBCollection;
import es.usc.citius.womine.model.Pattern;
import bpm.log_editor.parser.CSVparser;
import bpm.log_editor.storage.MongoDAO;
import bpm.log_editor.storage.LogService;
import org.springframework.data.annotation.Id;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static bpm.log_editor.parser.Constants.*;

public class LogFile {

    @Id
    public String id;
    Long user;
    String name;
    String path;
    String dbName;
    DBCollection coll;
    Headers headers;
    Headers hierarchyCols;
    HashMap<String, String> pairing;
    String state;
    String date;
    String sampleDate;
    Hierarchy tree;
    ArrayList<MinedLog> models = new ArrayList<>();
    String configName;
    Config lastConfig;

    //BUILDERS
    public LogFile() {
    }

    public LogFile(Long user, String name, String path, String dbName, DBCollection coll, Headers headers, Headers hierarchyCols, HashMap<String, String> pairing,
                   String state, String date, String configName) {

        //Assing data
        this.user = user;
        this.name = name;
        this.path = path;
        this.dbName = dbName;
        this.coll = coll;
        this.headers = headers;
        this.hierarchyCols = hierarchyCols;
        this.pairing = pairing;
        this.state = state;
        this.date = date;
        this.configName = configName;
    }

    //---SETTERS---//
    public void setName(String name) {
        this.name = name;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setDb(String db) {
        this.dbName = db;
        this.coll = MongoDAO.getCollection(this.dbName);
    }

    public void setColl(DBCollection coll) {
        //this.coll = coll;
        LogService.save(this);
    }

    public void setHierarchyCols(Headers h) {
        this.hierarchyCols = h;
        LogService.save(this);
    }

    public void setState(String state) {
        this.state = state;
        LogService.save(this);
    }

    public void setModels(ArrayList<MinedLog> models) {
        this.models = models;
    }

    public void setDate(String date) {
        this.date = date;
        LogService.save(this);
    }

    public void setUser(Long user) {
        this.user = user;
    }

    public void setTree(Hierarchy tree) {
        this.tree = tree;
        LogService.save(this);
    }

    public void setLastConfig(Config lastConfig) {
        Config c = new Config(lastConfig);
        this.lastConfig = c;
        LogService.save(this);
    }

    public void setLastConfigNull() {
        this.lastConfig = null;
        LogService.save(this);
    }

    public void setConfigName(String configName) {
        this.configName = configName;
        LogService.save(this);
    }

    //---GETTERS---//
    public String getName() {
        return this.name;
    }

    public String getPath() {
        return this.path;
    }

    public DBCollection getColl() {
        return this.coll;
    }

    public Headers getHeaders() {

        //Check if headers already fetched
        //if (this.headers == null) {
            this.headers = CSVparser.getHeaders(this.path);
            LogService.save(this);
        //}

        return this.headers;
    }

    public String getState() {
        return state;
    }

    public HashMap<String, String> getPairing() {
        return pairing;
    }

    public Headers getHierarchyCols() {
        return hierarchyCols;
    }

    public Long getUser() {
        return user;
    }

    public ArrayList<MinedLog> getModels() {
        return models;
    }

    public String getSampleDate() {
        return sampleDate;
    }

    public Hierarchy getTree() {
        return tree;
    }

    public String getConfigName() {
        return configName;
    }

    //---CUSTOM FUNCTIONS---//
    public void insertFile(Headers columns) {

        this.setState("Processing");
        LogService.save(this);

        //HashMap<String, ArrayList<String>> r = CSVparser.removeColumns(this, columns);
        CSVparser.removeColumnsSpark(this, columns);
        this.headers = CSVparser.getHeaders(this.path);
        this.setState("loaded");
        LogService.save(this);


        //return r;
    }

    public void setTraceActTime(String trace, String act, String timestamp, String timestampf) {
        this.pairing = new HashMap<>();
        pairing.put(TRACE, trace);
        pairing.put(INITIAL_TIME, timestamp);
        pairing.put(COMPLETE_TIME, timestampf);
        pairing.put(ACTIVITY, act);
        LogService.save(this);
    }

    public HashMap<String, List<String>> UniquesToFilter() {
        return MongoDAO.getContent(this.name, this.hierarchyCols);
    }

    public void replaceNulls(String column, String value) {
        MongoDAO.replaceNulls(this.getName(), column, value);
    }

    public void replaceValues(String column, List<String> values, String replacement) {
        MongoDAO.replaceValues(this.getName(), column, values, replacement);
    }

    private void setState() {
        if (this.getHierarchyCols() != null) {
            this.setState("processing");

        }
    }

    public void dropColl() {
        MongoDAO.dropColl(this.name);
    }

    public void addModel(MinedLog model) {
        System.out.println("ADD MODEL");
        this.getModels().add(model);
        LogService.save(this);
    }

    public LogFile deleteModel(int index) {
        this.models.remove(index);
        LogService.save(this);
        return this;
    }

    public String getDate() {
        return date;
    }

    public List<String> getFrequentPatterns(int i, Double threshold) {
        List<String> strings = MongoDAO.frequentPatters(this, i, threshold);
        LogService.save(this);
        return strings;
    }

    public List<String> getInfrequentPatterns(int i, Double threshold) {
        List<String> strings = MongoDAO.infrequentPatters(this, i, threshold);
        LogService.save(this);
        return strings;
    }

    public String pruneArcs(int i, Double threshold) {
        String string = MongoDAO.pruneArcs(this, i, threshold);
        return string;
    }

    public boolean addPattern(Integer model, Pattern data, String type) {

        if (type.equals("frequent")) {
            this.getModels().get(model).getFP().add(data);
        } else {
            this.getModels().get(model).getIP().add(data);
        }

        LogService.save(this);

        return true;
    }

    public void setSampleDate() {
        ArrayList r = new ArrayList<>();
        r.add(this.getPairing().get(INITIAL_TIME));
        this.sampleDate = MongoDAO.getContent(this.name, new Headers(r)).get(this.getPairing().get(INITIAL_TIME)).get(0);
        LogService.save(this);
    }

    public Config getLastConfig() {
        return lastConfig;
    }
}

