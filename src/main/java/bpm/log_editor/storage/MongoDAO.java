package bpm.log_editor.storage;

import bpm.heuristic.FileTranslator;
import bpm.heuristic.GraphViz;
import bpm.heuristic.parserHN;
import bpm.log_editor.data_types.*;
import bpm.log_editor.parser.CSVparser;
import bpm.log_editor.parser.ConstantsImpl;
import bpm.statistics.Arc;
import bpm.statistics.spark.*;
import com.google.gson.Gson;
import com.mongodb.*;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.util.JSON;
import es.usc.citius.prodigen.domainLogic.exceptions.*;
import es.usc.citius.prodigen.domainLogic.workflow.Log;
import es.usc.citius.prodigen.domainLogic.workflow.LogEntryInterface;
import es.usc.citius.prodigen.domainLogic.workflow.Task.Task;
import es.usc.citius.prodigen.domainLogic.workflow.algorithms.geneticMining.CMTask.CMSet;
import es.usc.citius.prodigen.domainLogic.workflow.algorithms.geneticMining.CMTask.CMTask;
import es.usc.citius.prodigen.domainLogic.workflow.algorithms.geneticMining.individual.CMIndividual;
import es.usc.citius.prodigen.domainLogic.workflow.algorithms.geneticMining.individual.writer.IndividualWriterCN;
import es.usc.citius.prodigen.domainLogic.workflow.algorithms.geneticMining.individual.writer.IndividualWriterHN;
import es.usc.citius.prodigen.domainLogic.workflow.algorithms.heuristic.heuristicsminer.HeuristicsMiner;
import es.usc.citius.prodigen.domainLogic.workflow.algorithms.heuristic.settings.HeuristicsMinerSettings;
import es.usc.citius.prodigen.domainLogic.workflow.logReader.LogReaderCSV;
import es.usc.citius.prodigen.domainLogic.workflow.logReader.LogReaderInterface;
import es.usc.citius.prodigen.spark.FHMSpark;
import es.usc.citius.prodigen.util.IndividualVO;
import es.usc.citius.prodigen.util.TaskVO;
import es.usc.citius.womine.algorithm.FrequentPatterns;
import es.usc.citius.womine.algorithm.IGraphMining;
import es.usc.citius.womine.algorithm.InfrequentPatterns;
import es.usc.citius.womine.algorithm.Settings;
import es.usc.citius.womine.input.InputSpark;
import es.usc.citius.womine.model.Graph;
import es.usc.citius.womine.model.Pattern;
import es.usc.citius.womine.model.Trace;
import es.usc.citius.womine.model.TraceArcSet;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.hash.TIntHashSet;
import main.SparkMain;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;
import org.bson.types.ObjectId;
import scala.Tuple2;

import java.io.*;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static bpm.log_editor.parser.Constants.ACTIVITY;
import static bpm.log_editor.parser.Constants.CASES_SUF;
import static bpm.log_editor.parser.Constants.COMPLETE_TIME;
import static bpm.log_editor.parser.Constants.DB_NAME;
import static bpm.log_editor.parser.Constants.FORMAT;
import static bpm.log_editor.parser.Constants.HOST;
import static bpm.log_editor.parser.Constants.INITIAL_TIME;
import static bpm.log_editor.parser.Constants.LOG_DIR;
import static bpm.log_editor.parser.Constants.PORT;
import static bpm.log_editor.parser.Constants.TRACE;
import static bpm.log_editor.parser.Constants.TRACES_SUF;
import static bpm.log_editor.parser.ConstantsImpl.*;

public class MongoDAO {

    public static MongoClient mongoClient = new MongoClient(HOST, PORT);
    public static DB db = mongoClient.getDB(DB_NAME);
    public static GridFS fs = new GridFS(db);
    private static HashMap<Integer, Task> intToTasksLog;
    private static HashMap<String, Integer> StringToIntLog;
    private static Config c;
    private final static GraphViz gv = new GraphViz();

    //--BASIC MONGO FUNCTIONS--//
    //Creates MongoColl
    public static DBCollection createCollection(String name) {

        DBCollection coll;

        //If coll exists, override
        coll = db.createCollection(name, null);

        return coll;
    }

    //Returns all mongo collection names
    public static Set<String> getCollections() {
        return db.getCollectionNames();
    }

    //Returns a mongo coll with name Name
    public static DBCollection getCollection(String name) {
        return db.getCollection(name);
    }

    //Retrieves all docs from a MongoColl
    public static DBCursor queryCollection(DBCollection coll, BasicDBObject query) {
        return coll.find(query);
    }

    //Inserts a document in a collection
    public static void insertDocument(DBCollection coll, BasicDBObject doc) {
        coll.insert(doc);
    }

    //Drops a collection
    public static void dropColl(String collName) {
        DBCollection coll = db.getCollection(collName);
        coll.drop();
    }

    //Prints a collection to csv
    private static void printToCSV(DBCollection coll) {

        //Create file writer
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(coll.getName(), "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        //Create cursor
        DBCursor cursor = coll.find();

        //Write header
        writer.print("CaseIdentifier, TaskIdentifier\n");

        //Iterate results
        while (cursor.hasNext()) {
            DBObject trace = cursor.next();
            //System.out.println(trace);
            writer.print(trace.get("trace") + ",");
            writer.print(trace.get("activity") + "\n");
            //writer.print(trace.get("timestamp")+ "\n");
            //writer.print(":complete\n");
        }

        writer.print("\n");
        writer.close();

    }


    //--LOG FUNCTIONS--//
    //Inserts a log into mongodb
    public static void insertLog(String filePath, StorageService storageService) {

        //Create new collection
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.length());
        DBCollection coll;

        //If coll exists, override
        if (db.collectionExists(fileName)) {
            coll = db.getCollection(fileName);
            coll.drop();
        }
        coll = db.createCollection(fileName, null);

        String line = "";
        String cvsSplitBy = ",";
        List<String> headers = CSVparser.getHeaders(filePath).getData();
        //ArrayList<String> headers = CSVparser.getHeaders(filePath);

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {

            //Skip headers
            line = br.readLine();

            //If file has content
            //For each line we create a doc
            while ((line = br.readLine()) != null) {

                String[] columns = line.split(cvsSplitBy);

                //Create doc
                BasicDBObject doc = new BasicDBObject();

                //Add content to doc
                for (int i = 0, columnsLength = columns.length; i < columnsLength; i++) {
                    String data = columns[i];

                    doc.append(headers.get(i), data.replace("\"", ""));
                }
                insertDocument(coll, doc);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    //Returns unique values of a collection from columns h
    public static HashMap<String, List<String>> getContent(String collName, Headers h) {

        List<String> headers = h.getData();

        DBCollection coll = db.getCollection(collName);

        //Get unique values
        HashMap<String, List<String>> data = new HashMap<>();

        List<DBObject> indexes = coll.getIndexInfo();

        //Create index for each field
        for (int i = 1; i < indexes.size(); i++) {
            String key = indexes.get(i).get("key").toString();
            String[] tokens = key.split("\"");
            if (headers != null) {
                if (headers.contains(tokens[1])) {
                    data.put(tokens[1], (ArrayList<String>) coll.distinct(tokens[1]));
                }
            } else {
                data.put(tokens[1], (ArrayList<String>) coll.distinct(tokens[1]));
            }
        }

        return data;

    }

    //Queries a mongo collection and mines it
    public static ArrayList<MinedLog> queryLog(LogFile logFile, Hierarchy h) throws EmptyLogException, InvalidFileExtensionException, MalformedFileException, WrongLogEntryException, NonFinishedWorkflowException, ParseException, FileNotFoundException, UnsupportedEncodingException {
        //Set state
        logFile.setState("processing");

        //Check if content in branch
        String configName = logFile.getConfigName();
        BasicDBObject query = new BasicDBObject("name", configName);
        DBCollection config = db.getCollection("config");
        DBCursor cursor = config.find(query);

        try {
            if (cursor.hasNext()) {
                DBObject obj = cursor.next();
                c = new Gson().fromJson(obj.get("config").toString(), Config.class);
            } else {
                c = null;
            }
        } finally {
            cursor.close();
        }

        boolean mine = true;
        if (c != null && logFile.getLastConfig() != null) {
            mine = c.checkDiferences(logFile.getLastConfig());
        }

        //Data to return
        ArrayList<MinedLog> models = new ArrayList<>();
        if (mine) {
            if (c != null) {
                HashMap<String, List<String>> filterValues = LogService.getLogByName(logFile.getName()).UniquesToFilter();
                addBranchs(h, filterValues);
            }

            //Get collection
            String file = logFile.getName();

            AssistantFunctions.setConfig(file);
            JavaRDD<Document> rdd = AssistantFunctions.getInitialRDD();
            rdd.persist(StorageLevel.MEMORY_ONLY());
            setnPartitions(rdd.getNumPartitions());
            //System.out.println("Partitions initial RDD: " + rdd.getNumPartitions());
                /*List<Document> take = rdd.take(1);
                for (Document doc : take) {
                    System.out.println(doc);
                }*/

            JavaRDD<Document> preOrderRDD = AssistantFunctions.standarizeRDD(logFile.getDate(), logFile.getPairing(), rdd);
            rdd.unpersist();

            //Only change the time when we put artificial times
            ConstantsImpl.setcTimes(logFile.getPairing().get("timestamp").equals(logFile.getPairing().get("timestampf")));

            if (c != null && !c.getCategory().equals("")) {
                //Calc log stats
                AssistantFunctions.getAllLogTraces(preOrderRDD, file, c.getCategory(), c.getWorkflow());
            }
            //System.out.println("Partitions after standarize RDD: " + preOrderRDD.getNumPartitions());

            if (h.getNodes().size() == 0) {
                h.addNode(new Node());
            }

            //For each branch
            for (Node b : h.getNodes()) {
                Boolean mine1 = b.getMine();
                if (mine1) {
                    //New Minned log
                    MinedLog r = new MinedLog();

                    //Set log branch
                    r.setNode(b);


                    Map<Object, List<Object>> map = new HashMap<>();
                    //For each column
                    Iterator it = b.getFilter().entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry pair = (Map.Entry) it.next();
                        List<Object> list = new ArrayList<>();
                        //For each value
                        for (String value : b.getFilter().get(pair.getKey())) {
                            list.add(value);
                        }
                        map.put(pair.getKey(), list);
                    }

                    JavaRDD<Document> orderRDD;
                    Map<String, String> pairing = revertMap(logFile.getPairing());
                    // || b.getInitialTime() != 0l
                    if (map.size() > 0) {
                        //Filter RDD by the user values
                        orderRDD = preOrderRDD.filter(new Function<Document, Boolean>() {
                            @Override
                            public Boolean call(Document v1) throws Exception {
                                boolean resul = false;
                                for (Map.Entry<Object, List<Object>> entry : map.entrySet()) {
                                    resul = false;
                                    String key;
                                    List<Object> value = entry.getValue();
                                    if (pairing.get(entry.getKey()) == null) {
                                        key = (String) entry.getKey();
                                    } else {
                                        key = pairing.get(entry.getKey());
                                    }
                                    for (Object r : value) {
                                        if (v1.get(key).equals(r)) {
                                            resul = true;
                                            break;
                                        }
                                    }
                                    if (!resul) {
                                        return false;
                                    }
                                }
                                return resul;
                            }
                        });
                    } else {
                        orderRDD = preOrderRDD;
                    }

                    //Create new coll
                    DBCollection newcoll;
                    DBCollection newcollTraces;
                    DBCollection newcollCases;
                    String newCollName2 = file + logFile.getModels().size();
                    String newCollName2T = file + logFile.getModels().size() + TRACES_SUF;
                    String newCollName3C = file + logFile.getModels().size() + CASES_SUF;
                    if (db.collectionExists(newCollName2)) {
                        newcoll = db.getCollection(newCollName2);
                        newcoll.drop();
                    }
                    if (db.collectionExists(newCollName2T)) {
                        newcollTraces = db.getCollection(newCollName2T);
                        newcollTraces.drop();
                    }
                    if (db.collectionExists(newCollName3C)) {
                        newcollCases = db.getCollection(newCollName3C);
                        newcollCases.drop();
                    }
                    newcoll = db.createCollection(newCollName2, null);
                    newcollTraces = db.createCollection(newCollName2T, null);
                    newcollCases = db.createCollection(newCollName3C, null);

                    //Save activities RDD
                    JavaRDD<Document> activities = AssistantFunctions.getAllActivities(orderRDD, C_TIMES, b.getGroup());
                    AssistantFunctions.setInitialRDD(activities);
                    //TODO try to avoid on large datasets
                    if (FORMAT.equals(es.usc.citius.womine.utils.constants.Constants.CM_EXTENSION)) {
                        writeFile(newCollName2, activities);
                    }
                    writeBD(newcoll, activities);

                    //Save traces RDD
                    JavaRDD<Document> traces = AssistantFunctions.getAllTraces(C_TIMES);
                    AssistantFunctions.setTracesRDD(traces);
                    writeBD(newcollTraces, traces);

                    CMIndividual model;
                    IndividualVO individualVO;
                    long start_Read;
                    long end_Read;
                    long start_Minner;
                    long end_Minner;
                    if (FORMAT.equals(es.usc.citius.womine.utils.constants.Constants.CM_EXTENSION)) {
                        //Mine log
                        start_Read = System.nanoTime();
                        //Read log from the csv
                        LogReaderInterface reader = new LogReaderCSV();
                        //System.out.println(log);
                        File f = new File(LOG_DIR + newCollName2 + ".csv");
                        ArrayList<LogEntryInterface> entries = reader.read(null, null, f);
                        Log log = new Log("log", LOG_DIR + newCollName2 + ".csv", entries);
                        end_Read = System.nanoTime();
                        System.out.println("(Time) Read: " + (end_Read - start_Read) / 1e9);


                        start_Minner = System.nanoTime();
                        model = heuristicsMiner(log);
                        end_Minner = System.nanoTime();
                        individualVO = individualToVO(model, log);
                    } else {
                        start_Minner = System.nanoTime();
                        model = FHMSpark.mine(SparkConnection.getContext(), AssistantFunctions.getTracesRDD());
                        end_Minner = System.nanoTime();

                        intToTasksLog = new HashMap<>();
                        TIntObjectMap<CMTask> tasks = model.getTasks();
                        for (int i = 0; i < tasks.size(); i++) {
                            intToTasksLog.put(i, tasks.get(i).getTask());
                        }

                        individualVO = individualToVO(model, intToTasksLog);
                    }
                    System.out.println("(Time) Minner: " + (end_Minner - start_Minner) / 1e9);

                    DBCollection modelsColl;
                    if (db.getCollection("models") != null) {
                        modelsColl = db.getCollection("models");
                        query = new BasicDBObject();
                        query.put("name", newcoll.getName());
                        modelsColl.remove(query);
                    } else {
                        modelsColl = db.createCollection("models", null);
                    }

                    String path = LOG_DIR + newCollName2 + MODEL_FORMAT + MODEL_EXTENSION_FORMAT;
                    if (MODEL_FORMAT.equals("cn")) {
                        IndividualWriterCN cnWriter = new IndividualWriterCN();
                        cnWriter.write(path, model);
                    } else if (MODEL_FORMAT.equals("hn")) {
                        IndividualWriterHN hnWriter = new IndividualWriterHN();
                        hnWriter.write(path, model);
                    }

                    r.setHn(path);
                    r.setModelFile(LOG_DIR + newCollName2 + ".csv");

                    //Save model and update log
                    r.setModel(parserHN.translate(model));
                    logFile.addModel(r);
                    LogService.save(logFile);

                    //Calculate stats
                    ActivityStatisticsVO actStats = activityStats();
                    r.setActStats(actStats);

                    TraceStatisticsVO traceStats = traceStats();

                    //Spark Arcs calcs
                    Graph graph = new Graph(individualVO);

                    HashMap<Integer, es.usc.citius.womine.model.Node> nodesSpark = graph.getNodesSpark();
                    Map<String, Integer> relationIDs = new HashMap<>();
                    for (Map.Entry<Integer, es.usc.citius.womine.model.Node> node : nodesSpark.entrySet()) {
                        relationIDs.put(node.getValue().getId().replace(":complete", ""), node.getKey());
                    }

                    JavaPairRDD<String, Trace> caseInstances = InputSpark.getCaseInstances(graph, individualVO.getFormat(), AssistantFunctions.getTracesRDD(), relationIDs, SparkConnection.getContext());
                    ArcStatisticsVO arcStatisticsVO = arcStats(caseInstances, null, graph);
                    r.setArcStats(arcStatisticsVO);

                    JavaPairRDD<String, Trace> cis = InputSpark.allCaseInstances.flatMapToPair(new PairFlatMapFunction<Tuple2<List<String>, Trace>, String, Trace>() {
                        @Override
                        public Iterator<Tuple2<String, Trace>> call(Tuple2<List<String>, Trace> t) throws Exception {
                            List<Tuple2<String, Trace>> l = new ArrayList();
                            for (String s : t._1) {
                                Trace traza = new Trace(t._2);
                                l.add(new Tuple2<>(s, traza));
                            }

                            return l.iterator();
                        }
                    });

                    //Transform the paths adding arcs
                    putArcs(traceStats, cis.collectAsMap());
                    r.setTraceStats(traceStats);

                    saveIndivudalVO(modelsColl, newcoll.getName(), model.getNumOfTasks(), individualVO);
                    JavaRDD<Document> ci = preSaveCasesSpark(caseInstances, InputSpark.numInstances);
                    writeBD(newcollCases, ci);

                    r.setSpan(AssistantFunctions.getLogTime());

                    LogService.save(logFile);

                    generateBackGraphs(graph, newCollName2, actStats, arcStatisticsVO, traceStats, caseInstances, r, logFile.getName());

                    //TODO Remove this?
                    r.getTraceStats().tracesActivitiesTimes = null;

                    if (c != null) {
                        logFile.setLastConfig(c);
                    }
                    LogService.save(logFile);
                }
            }
        } else {
            models = logFile.getModels();
            for (MinedLog m : models) {
                generateNewBackGraphs(m, c, logFile);
            }
        }

        //Set state
        logFile.setState("loaded");
        LogService.save(logFile);

        return models;
    }

    private static void saveIndivudalVO(DBCollection modelsColl, String name, Integer numT, IndividualVO individualVO) {
        Gson gson = new Gson();

        BasicDBObject parse = (BasicDBObject) JSON.parse(gson.toJson(individualVO));
        BasicDBObject obj = new BasicDBObject("tasks", parse);
        obj.put("name", name);
        obj.put("numOfTasks", numT);
        modelsColl.save(obj);
    }

    private static void saveCases(DBCollection coll, Map<String, Trace> caseInstances) {
        Gson gson = new Gson();

        for (Map.Entry<String, Trace> entry : caseInstances.entrySet()) {
            BasicDBObject object = new BasicDBObject();
            object.put("id", entry.getKey());
            object.put("trace", JSON.parse(gson.toJson(entry.getValue())));
            coll.save(object);
        }

    }

    private static JavaRDD<Document> preSaveCasesSpark(JavaPairRDD<String, Trace> caseInstances, Integer numInstances) {

        JavaRDD<Document> ci = caseInstances.map(new Function<Tuple2<String, Trace>, Document>() {
            @Override
            public Document call(Tuple2<String, Trace> v1) throws Exception {
                Document d = new Document();
                Gson gson = new Gson();

                d.put("id", v1._1);
                d.put("trace", JSON.parse(gson.toJson(v1._2)));
                //TODO Should be global, not in all documents
                d.put("numInstances", numInstances);

                return d;
            }
        });

        return ci;
    }

    //Convert a CMIndividual into IndividualVO and assign the same tasks "id" as the log
    private static IndividualVO individualToVO(CMIndividual model, Log log) {
        intToTasksLog = log.getIntToTasks();
        //Get de ref of the task names and de int "id" on the log
        StringToIntLog = new HashMap<>();
        for (Map.Entry entry : intToTasksLog.entrySet()) {
            Task value = (Task) entry.getValue();
            StringToIntLog.put(value.getId(), (Integer) entry.getKey());
        }

        //Change de ref of the tasks names with the log "id"
        TIntObjectMap<CMTask> tasks = model.getTasks();
        HashMap<Integer, String> intToStringModel = new HashMap<>();
        for (int i = 0; i < model.getNumOfTasks(); i++) {
            if (!intToStringModel.containsKey(i)) {
                //When the log was read with CN is necessary put :complete on tasks names
                if (MODEL_FORMAT == es.usc.citius.womine.utils.constants.Constants.CN_EXTENSION) {
                    intToStringModel.put(i, model.getTask(i).getTask().getId() + ":complete");
                } else {
                    intToStringModel.put(i, model.getTask(i).getTask().getId());
                }
            }
            tasks.put(i, model.getTask(i));
        }

        List<TaskVO> l = new ArrayList<>();
        boolean s;
        for (int z = 0; z < model.getNumOfTasks(); z++) {
            CMTask task = model.getTask(z);
            s = true;
            //Check tasks without inputs and outputs
            if (!task.getInputs().isEmpty() || !task.getOutputs().isEmpty()) {
                if (task.getInputs().size() == 1 && task.getOutputs().size() == 1) {
                    //Posible bucle a si misma sin otras conexiones
                    TIntHashSet hashSet = task.getInputs().get(0);
                    TIntHashSet hashSet1 = task.getOutputs().get(0);
                    if (hashSet.size() == 1 && hashSet1.size() == 1) {
                        TIntIterator iterator = hashSet.iterator();
                        TIntIterator iterator1 = hashSet1.iterator();
                        int next = iterator.next();
                        int next1 = iterator1.next();
                        if (next == next1 && next == task.getMatrixID()) {
                            s = false;
                        }
                    }
                }
                if (s) {
                    CMSet inputs = task.getInputs();
                    List<List<Integer>> list = new ArrayList<>();
                    for (int j = 0; j < inputs.size(); j++) {
                        List<Integer> subList = new ArrayList<>();
                        TIntIterator ite = inputs.get(j).iterator();
                        while (ite.hasNext()) {
                            String modelA = intToStringModel.get(ite.next());
                            //Get the "id" reference of the log
                            subList.add(StringToIntLog.get(modelA));
                        }
                        list.add(subList);
                    }
                    CMSet outputs = task.getOutputs();
                    List<List<Integer>> list2 = new ArrayList<>();
                    for (int j = 0; j < outputs.size(); j++) {
                        List<Integer> subList = new ArrayList<>();
                        TIntIterator ite = outputs.get(j).iterator();
                        while (ite.hasNext()) {
                            String modelA = intToStringModel.get(ite.next());
                            subList.add(StringToIntLog.get(modelA));
                        }
                        list2.add(subList);
                    }

                    String taskA = intToStringModel.get(task.getTask().getMatrixID());
                    TaskVO t = new TaskVO(task.getTask().getId(), StringToIntLog.get(taskA), list, list2);
                    l.add(t);
                }
            }
        }

        IndividualVO individual = new IndividualVO(l, MODEL_FORMAT);

        return individual;
    }

    //Convert a CMIndividual into IndividualVO
    public static IndividualVO individualToVO(CMIndividual model, HashMap<Integer, Task> intToTasksLo) {
        //Get de ref of the task names and de int "id" on the log
        StringToIntLog = new HashMap<>();
        for (Map.Entry entry : intToTasksLo.entrySet()) {
            Task value = (Task) entry.getValue();
            StringToIntLog.put(value.getId(), (Integer) entry.getKey());
        }

        //Change de ref of the tasks names with a int "id"
        TIntObjectMap<CMTask> tasks = model.getTasks();
        HashMap<Integer, String> intToStringModel = new HashMap<>();
        for (int i = 0; i < model.getNumOfTasks(); i++) {
            if (!intToStringModel.containsKey(i)) {
                //When the log was read with CN is necessary put :complete on tasks names
                if (MODEL_FORMAT == es.usc.citius.womine.utils.constants.Constants.CN_EXTENSION) {
                    //intToStringModel.put(i, model.getTask(i).getTask().getId() + ":complete");
                    intToStringModel.put(i, model.getTask(i).getTask().getId());
                } else {
                    intToStringModel.put(i, model.getTask(i).getTask().getId());
                }
            }
            tasks.put(i, model.getTask(i));
        }

        List<TaskVO> l = new ArrayList<>();
        for (int z = 0; z < model.getNumOfTasks(); z++) {
            CMTask task = model.getTask(z);
            //Check tasks without inputs and outputs
            if (!task.getInputs().isEmpty() || !task.getOutputs().isEmpty()) {
                CMSet inputs = task.getInputs();
                List<List<Integer>> list = new ArrayList<>();
                for (int j = 0; j < inputs.size(); j++) {
                    List<Integer> subList = new ArrayList<>();
                    TIntIterator ite = inputs.get(j).iterator();
                    while (ite.hasNext()) {
                        String modelA = intToStringModel.get(ite.next());
                        //Get the "id" reference of the log
                        subList.add(StringToIntLog.get(modelA));
                    }
                    list.add(subList);
                }
                CMSet outputs = task.getOutputs();
                List<List<Integer>> list2 = new ArrayList<>();
                for (int j = 0; j < outputs.size(); j++) {
                    List<Integer> subList = new ArrayList<>();
                    TIntIterator ite = outputs.get(j).iterator();
                    while (ite.hasNext()) {
                        String modelA = intToStringModel.get(ite.next());
                        subList.add(StringToIntLog.get(modelA));
                    }
                    list2.add(subList);
                }

                String taskA = intToStringModel.get(task.getTask().getMatrixID());
                TaskVO t = new TaskVO(task.getTask().getId(), StringToIntLog.get(taskA), list, list2);
                l.add(t);
            }
        }

        IndividualVO individual = new IndividualVO(l, MODEL_FORMAT);

        return individual;
    }

    private static Map<String, String> revertMap(HashMap<String, String> map) {
        Map<String, String> newMap = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            newMap.put(entry.getValue(), entry.getKey());
        }
        return newMap;
    }

    //Mines with heuristics minner
    private static CMIndividual heuristicsMiner(Log l) throws EmptyLogException, InvalidFileExtensionException, MalformedFileException, WrongLogEntryException, NonFinishedWorkflowException {

        //Mine with ProDiGen
        //GeneticSettings geneticSettings = new GeneticSettings();
        //geneticSettings.setPath("/home/yagouus/prodigen/");
        //geneticSettings.setNumOfCores(1);
        //GeneticMiningThread geneticThread = new GeneticMiningThread(null, l, geneticSettings);
        //(new Thread(geneticThread, "geneticThreadFromDB")).start();
        //Mine with ProDiGen

        //TODO debug files
        /*ArrayList<CaseInstance> arrayCaseInstances = l.getArrayCaseInstances();
        for (CaseInstance a: arrayCaseInstances) {
            System.out.println("");
            System.out.println("ID: " + a.getId());
            TIntIterator it = a.getTaskSequence().iterator();
            while (it.hasNext()) {
                System.out.print(it.next() + " ");
            }
        }*/

        //Mine log with FHM (dont't work)
        //System.out.println("Starting Miner");
        long start_Log = System.nanoTime();
        //Mine log with HeuristicMinner
        HeuristicsMiner hm = new HeuristicsMiner(l, new HeuristicsMinerSettings());
        long end_Log = System.nanoTime();
        System.out.println("(Time) Log: " + (end_Log - start_Log) / 1e9);
        long start_FHM = System.nanoTime();
        CMIndividual mine = hm.mine();
        long end_FHM = System.nanoTime();
        System.out.println("(Time) miner: " + (end_FHM - start_FHM) / 1e9);
        return mine;
    }

    //Gets infrequent pattern
    public static List<String> infrequentPatters(LogFile log, Integer m, Double threshold) {
        MinedLog r = log.getModels().get(m);
        Map<Integer, List<String>> fp2 = r.getIP2();

        Double t = threshold * 100;
        int i = t.intValue();
        if (fp2.containsKey(i)) {
            List<String> strings = fp2.get(i);
            return strings;
        } else {
            String hnName = getName(log, m);

            BasicDBObject query = new BasicDBObject("name", hnName);
            DBCollection models = db.getCollection("models");
            DBCursor cursor = models.find(query);

            String casesName = hnName + CASES_SUF;
            DBCollection cases = db.getCollection(casesName);
            DBCursor casesCursor = cases.find();

            //Individual from DB
            IndividualVO individual = null;
            List<Tuple2<String, Trace>> caseInstances = new ArrayList<>();
            try {
                if (cursor.hasNext()) {
                    DBObject obj = cursor.next();
                    //Converting BasicDBObject to a custom Class(CMIndividual)
                    individual = new Gson().fromJson(obj.get("tasks").toString(), IndividualVO.class);
                }
                Integer numInstances = 0;
                while (casesCursor.hasNext()) {
                    DBObject next = casesCursor.next();
                    String id = (String) next.get("id");
                    Trace trace = new Gson().fromJson(next.get("trace").toString(), Trace.class);
                    caseInstances.add(new Tuple2<>(id, trace));
                    numInstances = (int) next.get("numInstances");
                }
                Settings settings = new Settings();

                String coll = hnName + CASES_SUF;
                JavaPairRDD<String, Trace> ciParaleliced = SparkConnection.getContext(coll).parallelizePairs(caseInstances);
                Graph model = new Graph(individual);
                SparkMain.setSpContext(SparkConnection.getContext());
                IGraphMining womine = new InfrequentPatterns(model, ciParaleliced, threshold, settings, numInstances);

                Set<Pattern> run1 = womine.runSpark();

                List<String> strings = FileTranslator.resultsPatternsToDot(model, run1, true, hnName, true, threshold, r, false);
                return strings;
            } finally {
                cursor.close();
            }
        }
    }

    //Prune arcs
    public static String pruneArcs(LogFile log, Integer m, Double threshold) {
        MinedLog r = log.getModels().get(m);

        List<bpm.statistics.Arc> arcs = r.getArcStats().arcsFrequencyOrdered;
        String s = FileTranslator.doTHPrune(arcs, threshold);

        return s;
    }

    //Gets frequent pattern
    public static List<String> frequentPatters(LogFile log, Integer m, Double threshold) {
        MinedLog r = log.getModels().get(m);
        Map<Integer, List<String>> fp2 = r.getFP2();

        Double t = threshold * 100;
        int i = t.intValue();
        if (fp2.containsKey(i)) {
            List<String> strings = fp2.get(i);
            return strings;
        } else {
            String hnName = getName(log, m);

            BasicDBObject query = new BasicDBObject("name", hnName);
            DBCollection models = db.getCollection("models");
            DBCursor cursor = models.find(query);

            String casesName = hnName + CASES_SUF;
            DBCollection cases = db.getCollection(casesName);
            DBCursor casesCursor = cases.find();

            //Individual from DB
            IndividualVO individual = null;
            List<Tuple2<String, Trace>> caseInstances = new ArrayList<>();
            try {
                if (cursor.hasNext()) {
                    DBObject obj = cursor.next();
                    //Converting BasicDBObject to a custom Class(CMIndividual)
                    individual = new Gson().fromJson(obj.get("tasks").toString(), IndividualVO.class);
                }
                Integer numInstances = 0;
                while (casesCursor.hasNext()) {
                    DBObject next = casesCursor.next();
                    String id = (String) next.get("id");
                    Trace trace = new Gson().fromJson(next.get("trace").toString(), Trace.class);
                    caseInstances.add(new Tuple2<>(id, trace));
                    numInstances = (int) next.get("numInstances");
                }

                Settings settings = new Settings();

                String coll = hnName + CASES_SUF;
                JavaPairRDD<String, Trace> ciParaleliced = SparkConnection.getContext(coll).parallelizePairs(caseInstances);
                Graph model = new Graph(individual);
                SparkMain.setSpContext(SparkConnection.getContext());
                IGraphMining womine = new FrequentPatterns(model, ciParaleliced, threshold, settings, numInstances);

                Set<Pattern> run1 = womine.runSpark();

                List<String> strings = FileTranslator.resultsPatternsToDot(model, run1, true, hnName, false, threshold, r, false);
                return strings;
            } finally {
                cursor.close();
            }
        }
    }

    //Gets frequent pattern
    public static void frequentPattersBack(Graph model, String hnName, String fp_threshold, JavaPairRDD<String, Trace> ciParaleliced
            , Integer numInstances, MinedLog r) {
        Settings settings = new Settings();

        SparkMain.setSpContext(SparkConnection.getContext());

        List<String> list1 = Arrays.asList(fp_threshold.split(","));
        List<Double> thresholds = listToDoubleList(list1);

        for (Double threshold : thresholds) {
            IGraphMining womine = new FrequentPatterns(model, ciParaleliced, threshold, settings, numInstances);

            Set<Pattern> run1 = womine.runSpark();
            FileTranslator.resultsPatternsToDot(model, run1, true, hnName, false, threshold, r, true);
        }
    }

    //Gets infrequent pattern
    public static void infrequentPattersBack(Graph model, String hnName, String ip_threshold, JavaPairRDD<String, Trace> ciParaleliced
            , Integer numInstances, MinedLog r) {
        Settings settings = new Settings();

        SparkMain.setSpContext(SparkConnection.getContext());

        List<String> list1 = Arrays.asList(ip_threshold.split(","));
        List<Double> thresholds = listToDoubleList(list1);

        for (Double threshold : thresholds) {
            IGraphMining womine = new InfrequentPatterns(model, ciParaleliced, threshold, settings, numInstances);
            Set<Pattern> run1 = womine.runSpark();
            FileTranslator.resultsPatternsToDot(model, run1, true, hnName, true, threshold, r, true);
        }
    }

    public static String getName(LogFile log, Integer m) {
        return log.getName() + m;
    }

    //Replaces all null values in a collection
    public static void replaceNulls(String collName, String column, String value) {

        //Create object with the query
        BasicDBObject query = new BasicDBObject();
        query.put(column, "-");

        //Get the collection
        DBCollection coll = db.getCollection(collName);

        //Add new value to the object
        BasicDBObject doc = new BasicDBObject();
        doc.append("$set", new BasicDBObject().append(column, value));

        //Update collection
        coll.updateMulti(query, doc);

    }

    //Replaces an array of values with a new one
    public static void replaceValues(String collName, String column, List<String> values, String replacement) {

        //Get the collection
        DBCollection coll = db.getCollection(collName);

        for (String value : values) {

            //Create object with the query
            BasicDBObject query = new BasicDBObject();
            query.put(column, value);

            //Add new value to the object
            BasicDBObject doc = new BasicDBObject();
            doc.append("$set", new BasicDBObject().append(column, replacement));

            //Update collection
            coll.updateMulti(query, doc);
        }
    }

    public static void writeFile(String name, JavaRDD<Document> orderRDD) throws ParseException, FileNotFoundException, UnsupportedEncodingException {
        //Create file writer
        File theDir = new File(LOG_DIR);
        if (!theDir.exists()) theDir.mkdir();
        PrintWriter writer = new PrintWriter(LOG_DIR + name + ".csv", "UTF-8");
        //Write header
        //writer.print("CaseIdentifier,TaskIdentifier\n");
        writer.print("CaseIdentifier,TaskIdentifier,InitialTime,CompleteTime\n");

        //TODO try to avoid on large datasets
        List<Document> collect = orderRDD.collect();

        Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        for (Document d : collect) {
            //writer.print(d.get(TRACE) + "," + d.get(ACTIVITY)+"\n");
            Date date = new Date((long) d.get(INITIAL_TIME));
            String ini = format.format(date);
            date = new Date((long) d.get(COMPLETE_TIME));
            String fin = format.format(date);
            writer.print(d.get(TRACE) + "," + d.get(ACTIVITY) + "," + ini + "," + fin + "\n");
            //writer.print(d.get(TRACE) + "," + d.get(ACTIVITY) + "," + d.get(INITIAL_TIME) + "," + d.get(COMPLETE_TIME) + "\n");
        }

        //Close writer
        writer.print("\n");
        writer.close();
    }

    private static void writeBD(DBCollection newcoll, JavaRDD<Document> rdd) {
        //Save on BD (lost order?)
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", newcoll.getName());
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(SparkConnection.getContext()).withOptions(writeOverrides);

        MongoSpark.save(rdd, writeConfig);
    }

    //Activity stats
    public static ActivityStatisticsVO activityStats() {

        //Create new return object
        ActivityStatisticsVO result = new ActivityStatisticsVO();

        //Activity frequencies
        result.activityFrequency = ActivityStatistics.activityFrequency();
        result.activityRelativeFrequency = ActivityStatistics.activityRelativeFrequency();
        result.activityFrequencyCuartil = ActivityStatistics.activityFrequencyCuartil();
        result.activityAverages = ActivityStatistics.activityAverages();
        result.activityLongerShorterDuration = ActivityStatistics.activityLongerShorterDuration();

        return result;
    }

    //Arc stats
    public static ArcStatisticsVO arcStats(JavaPairRDD<String, Trace> caseInstances, Map<String, Trace> ci, Graph model) throws EmptyLogException, WrongLogEntryException, MalformedFileException, NonFinishedWorkflowException, InvalidFileExtensionException {

        //return ArcsStatistics.arcsFrequencyOrdered(ci, intToTasksLog);

        //Create new return object
        ArcStatisticsVO result = new ArcStatisticsVO();

        //Activity frequencies
        result.arcsFrequencyOrdered = ArcsStatistics.arcsFrequencyOrderedSpark(caseInstances, intToTasksLog);
        result.arcsLoops = ArcsStatistics.arcsLoops(model);

        return result;
    }

    //Trace stats
    public static TraceStatisticsVO traceStats() {

        //Create new return object
        TraceStatisticsVO result = new TraceStatisticsVO();

        //Trace frequencies
        result.traceFrequency = TracesStatistics.traceFrequency();
        result.traceRelativeFrequency = TracesStatistics.traceFrequencyRelative();
        result.tracesAverages = TracesStatistics.tracesAverages();
        result.tracesActivitiesTimes = TracesStatistics.tracesActivitiesTimes();
        result.traceLongerShorterDuration = TracesStatistics.traceLongerShorterDuration();
        result.traceFrequency2 = TracesStatistics.traceFrequency2();
        result.firstLastTimeTraceActivity = TracesStatistics.firstLastTimeTraceActivity();
        result.tracesCuartil = TracesStatistics.timeTracesCuartil();
        result.tracesCount = TracesStatistics.tracesCount;

        return result;
    }

    public static void putArcs(TraceStatisticsVO traceStats, Map<String, Trace> caseInstances) {
        List<Tuple2<Tuple2<List<Object>, List<Object>>, Object>> traceFrequency = traceStats.traceFrequency2;
        List<Tuple2<List<Object>, List<Object>>> newTraceFrequency = new ArrayList<>();

        List<Object> arcs;
        for (Tuple2<Tuple2<List<Object>, List<Object>>, Object> t : traceFrequency) {
            arcs = new ArrayList<>();
            if (!t._1._1.isEmpty()) {
                String traceID = null;
                for (Object o : t._1._1) {
                    traceID = (String) o;
                    if (caseInstances.get(traceID) != null) {
                        break;
                    }
                }
                Trace trace = caseInstances.get(traceID);
                completeArcs(trace, arcs);
            }
            Tuple2<List<Object>, List<Object>> tupla = new Tuple2<>(t._1._2, arcs);
            newTraceFrequency.add(tupla);
        }

        traceStats.traceFrequency2 = null;
        traceStats.traceArcsFrequency = newTraceFrequency;

        if (traceStats.traceLongerShorterDuration.size() == 2) {
            List<Object> longer = traceStats.traceLongerShorterDuration.get(0);
            String traceID = (String) longer.get(2);
            Trace trace = caseInstances.get(traceID);
            List<Object> oArcs = new ArrayList<>();
            completeArcs(trace, oArcs);
            traceStats.traceLongerShorterDuration.get(0).add(oArcs);

            oArcs = new ArrayList<>();
            List<Object> shorter = traceStats.traceLongerShorterDuration.get(1);
            traceID = (String) shorter.get(2);
            trace = caseInstances.get(traceID);
            completeArcs(trace, oArcs);
            traceStats.traceLongerShorterDuration.get(1).add(oArcs);
        }
    }

    private static void completeArcs(Trace trace, List<Object> arcs) {
        List<TraceArcSet> arcSequence = trace.getArcSequence();
        //Get all arcs of the trace
        for (int i = 0; i < arcSequence.size(); i++) {
            //Get the destination task
            Integer destinationTask = arcSequence.get(i).getDestinationId();
            //Get the from tasks
            TIntArrayList sourceIds = arcSequence.get(i).getSourceIds();
            TIntIterator it = sourceIds.iterator();
            while (it.hasNext()) {
                Integer from = it.next();
                Arc a = new Arc(intToTasksLog.get(from).getId(), intToTasksLog.get(destinationTask).getId());
                arcs.add(a);
            }
        }
    }

    private static void generateBackGraphs(Graph graph, String hnName, ActivityStatisticsVO actStats, ArcStatisticsVO arcStats,
                                           TraceStatisticsVO traceStats, JavaPairRDD<String, Trace> caseInstances, MinedLog r, String fName) {
        //Creamos la carpeta donde guardaremos todas las imágenes
        if (c != null) {
            //Filtros aplicados
            Node node = r.getNode();
            String output = "";
            String filters = "";
            LinkedHashMap<String, ArrayList<String>> filter = node.getFilter();
            if (filter.size() > 0) {
                output += "Filtros sobre el log: ";
            } else {
                output += "Log minado sin filtros.";
            }
            for (Map.Entry<String, ArrayList<String>> entry : filter.entrySet()) {
                output += entry.getKey() + " " + entry.getValue();
                output += " > ";
                filters += entry.getKey() + "_" + entry.getValue();
                filters += "_";
            }
            output = output.replaceAll(" > $", "");
            filters = filters.replaceAll("_$", "");

            String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
            hnName = fName + "/" + timeStamp + "_" + filters;
            File dir = new File(LOG_DIR + hnName);

            if (dir.mkdirs()) {
                System.out.println("Directory is created!");
                r.setFolderName(LOG_DIR + hnName);
                //Write file with Info
                BufferedWriter writer = null;
                File infoFile = new File(LOG_DIR + hnName + "/README.txt");
                try {
                    writer = new BufferedWriter(new FileWriter(infoFile));
                    writer.write(output);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                System.out.println("Failed to create directory!");
            }
        }

        FileTranslator.ActivityStatsToDot(graph, actStats, hnName, c, r, fs);

        FileTranslator.ArcStatsToDot(graph, arcStats, hnName, c, r, fs);

        FileTranslator.TraceStatsToDot(graph, traceStats, hnName, c, r);

        if (c != null) {
            frequentPattersBack(graph, hnName, c.getFp_Threshold(), caseInstances, InputSpark.numInstances, r);
            infrequentPattersBack(graph, hnName, c.getIp_Threshold(), caseInstances, InputSpark.numInstances, r);
        }
    }

    private static void generateNewBackGraphs(MinedLog r, Config newConfig, LogFile logFile) {
        //Borramos la anterior carpeta de imágenes
        if (r.getFolderName() != null) {
            File f = new File(r.getFolderName());
            if (f.exists()) {
                try {
                    FileUtils.forceDelete(f);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        //Creamos la carpeta donde guardaremos todas las imágenes
        String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
        String hn = r.getModelFile();
        String[] split = hn.split("/");
        String[] split1 = split[1].split("\\.");
        String iniName = split1[0];
        String hnName = logFile.getName() + "/" + timeStamp + "_" + iniName;
        File dir = new File(LOG_DIR + hnName);

        if (dir.mkdirs()) {
            System.out.println("Directory is created!");
            r.setFolderName(LOG_DIR + hnName);
            //Write file with Info
            BufferedWriter writer = null;
            File infoFile = new File(LOG_DIR + hnName + "/README.txt");

            Node node = r.getNode();
            String output = "";
            LinkedHashMap<String, ArrayList<String>> filter = node.getFilter();
            if (filter.size() > 0) {
                output += "Filtros sobre el log: ";
            } else {
                output += "Log minado sin filtros.";
            }
            for (Map.Entry<String, ArrayList<String>> entry : filter.entrySet()) {
                output += entry.getKey() + " " + entry.getValue();
                output += " > ";
            }
            output = output.replaceAll(" > $", "");
            try {
                writer = new BufferedWriter(new FileWriter(infoFile));
                writer.write(output);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println("Failed to create directory!");
        }

        File normal = new File(LOG_DIR + hnName + "/" + "0_ModelClean.png");
        gv.writeGraphToFile(gv.getGraph(r.getModel(), "png", "dot"), normal);

        if (newConfig.isActivity_frequency_heatmap()) {
            String activityFrequencyHeatMap = r.getActStats().activityFrequencyHeatMap;
            File out = new File(LOG_DIR + hnName + "/" + "Activity_Frequency_HeatMap" + ".png");
            gv.writeGraphToFile(gv.getGraph(activityFrequencyHeatMap, "png", "dot"), out);

            activityFrequencyHeatMap = r.getActStats().activityFrequencyHeatMap2;
            out = new File(LOG_DIR + hnName + "/" + "Activity_Frequency_HeatMap2" + ".png");
            gv.writeGraphToFile(gv.getGraph(activityFrequencyHeatMap, "png", "dot"), out);

            //Find saved image
            GridFSDBFile out2 = fs.findOne(new BasicDBObject("_id", r.getActStats().frequencyChart));

            try {
                FileOutputStream outputImage = new FileOutputStream(LOG_DIR + hnName + "/ActivityFrequencyChart.png");
                out2.writeTo(outputImage);
                outputImage.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (newConfig.isActivity_duration_heatmap()) {
            String activity_duration_heatMap = r.getActStats().activityDurationHeatMap;
            File out = new File(LOG_DIR + hnName + "/" + "Activity_Duration_HeatMap" + ".png");
            gv.writeGraphToFile(gv.getGraph(activity_duration_heatMap, "png", "dot"), out);

            activity_duration_heatMap = r.getActStats().activityDurationHeatMap2;
            out = new File(LOG_DIR + hnName + "/" + "Activity_Duration_HeatMap2" + ".png");
            gv.writeGraphToFile(gv.getGraph(activity_duration_heatMap, "png", "dot"), out);

            //Find saved image
            GridFSDBFile out2 = fs.findOne(new BasicDBObject("_id", r.getActStats().durationChart));

            try {
                FileOutputStream outputImage = new FileOutputStream(LOG_DIR + hnName + "/ActivityDurationChart.png");
                out2.writeTo(outputImage);
                outputImage.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (newConfig.isArcs_frequency_number()) {
            String arcs_frequencies_number = r.getArcStats().arcsFrequencyNumber;
            File out = new File(LOG_DIR + hnName + "/" + "Arcs_Frequencies_Number" + ".png");
            gv.writeGraphToFile(gv.getGraph(arcs_frequencies_number, "png", "dot"), out);
        }

        if (newConfig.isArcs_frequency()) {
            String arcs_frequencies = r.getArcStats().arcsFrequencies;
            String arcs_frequencies_color_number = r.getArcStats().arcsFrequencyColorNumber;

            File out = new File(LOG_DIR + hnName + "/" + "Arcs_Frequencies" + ".png");
            gv.writeGraphToFile(gv.getGraph(arcs_frequencies, "png", "dot"), out);

            out = new File(LOG_DIR + hnName + "/" + "Arcs_Frequencies_Color_Number" + ".png");
            gv.writeGraphToFile(gv.getGraph(arcs_frequencies_color_number, "png", "dot"), out);

            arcs_frequencies = r.getArcStats().arcsFrequencies2;
            arcs_frequencies_color_number = r.getArcStats().arcsFrequencyColorNumber2;

            out = new File(LOG_DIR + hnName + "/" + "Arcs_Frequencies2" + ".png");
            gv.writeGraphToFile(gv.getGraph(arcs_frequencies, "png", "dot"), out);

            out = new File(LOG_DIR + hnName + "/" + "Arcs_Frequencies_Color_Number2" + ".png");
            gv.writeGraphToFile(gv.getGraph(arcs_frequencies_color_number, "png", "dot"), out);
        }

        if (newConfig.isArcs_loops()) {
            String arcs_loops = r.getArcStats().arcsLoopsGraph;
            File out = new File(LOG_DIR + hnName + "/" + "Arcs_Loops" + ".png");
            gv.writeGraphToFile(gv.getGraph(arcs_loops, "png", "dot"), out);
        }

        if (newConfig.isMost_frequent_path()) {
            int i = 1;
            List<String> mostF = r.getTraceStats().mostFrequentPath;
            for (String m : mostF) {
                File out = new File(LOG_DIR + hnName + "/" + "MostFrequentPath" + "_" + i + ".png");
                gv.writeGraphToFile(gv.getGraph(m, "png", "dot"), out);
                i++;
            }
        }

        if (newConfig.isCritical_path()) {
            String critical_path = r.getTraceStats().criticalPath;
            File out = new File(LOG_DIR + hnName + "/" + "CriticalPath" + ".png");
            gv.writeGraphToFile(gv.getGraph(critical_path, "png", "dot"), out);
        }

        List<bpm.statistics.Arc> arcs = r.getArcStats().arcsFrequencyOrdered;
        FileTranslator.doPrune(newConfig, arcs, hnName);

        //Check FP already calculated
        String fp_threshold = newConfig.getFp_Threshold();
        List<String> list1 = Arrays.asList(fp_threshold.split(","));
        List<Double> thresholds = listToDoubleList(list1);
        Map<Integer, List<String>> fp2 = r.getFP2();
        String th = newPatterns(thresholds, fp2, hnName, "FP");
        newConfig.setFp_Threshold(th);

        //Check IP already calculated
        String ip_threshold = newConfig.getIp_Threshold();
        List<String> list2 = Arrays.asList(ip_threshold.split(","));
        thresholds = listToDoubleList(list2);
        fp2 = r.getIP2();
        String th2 = newPatterns(thresholds, fp2, hnName, "IP");
        newConfig.setIp_Threshold(th2);

        if (!th.equals("") || !th2.equals("")) {
            BasicDBObject query = new BasicDBObject("name", iniName);
            DBCollection models = db.getCollection("models");
            DBCursor cursor = models.find(query);

            String casesName = iniName + CASES_SUF;
            DBCollection cases = db.getCollection(casesName);
            DBCursor casesCursor = cases.find();

            //Individual from DB
            IndividualVO individual = null;
            List<Tuple2<String, Trace>> caseInstances = new ArrayList<>();
            try {
                if (cursor.hasNext()) {
                    DBObject obj = cursor.next();
                    //Converting BasicDBObject to a custom Class(CMIndividual)
                    individual = new Gson().fromJson(obj.get("tasks").toString(), IndividualVO.class);
                }
                Integer numInstances = 0;
                while (casesCursor.hasNext()) {
                    DBObject next = casesCursor.next();
                    String id = (String) next.get("id");
                    Trace trace = new Gson().fromJson(next.get("trace").toString(), Trace.class);
                    caseInstances.add(new Tuple2<>(id, trace));
                    numInstances = (int) next.get("numInstances");
                }

                String coll = iniName + CASES_SUF;
                JavaPairRDD<String, Trace> ciParaleliced = SparkConnection.getContext(coll).parallelizePairs(caseInstances);
                Graph model = new Graph(individual);
                SparkMain.setSpContext(SparkConnection.getContext());

                frequentPattersBack(model, hnName, newConfig.getFp_Threshold(), ciParaleliced, numInstances, r);
                infrequentPattersBack(model, hnName, newConfig.getIp_Threshold(), ciParaleliced, numInstances, r);
            } finally {
                cursor.close();
            }
        }
    }

    private static void addBranchs(Hierarchy h, HashMap<String, List<String>> fValues) {
        h.setNodes(new ArrayList<>());
        boolean mine = true;
        if (c.isOnlyLeafs() && !c.getFilter_by().equals("")) {
            mine = false;
        }
        //Add ROOT node
        Node root = new Node(0, 0, "ROOT", mine, null, null, null, new LinkedHashMap<>(), null, 0l, 0l);
        root.setGroup(c.isGroup_activities());
        h.addNode(root);

        String f_by = c.getFilter_by();
        List<String> list4 = Arrays.asList(f_by.split(";"));
        List<List<String>> filter_by = new ArrayList<>();
        for (int i = 0; i < list4.size(); i++) {
            String s = list4.get(i);
            if (!s.equals("")) {
                filter_by.add(Arrays.asList(s.split(",")));
            }
        }

        if (filter_by.size() > 0) {
            int id = 1;
            for (int j = 0; j < filter_by.size(); j++) {
                List<String> filtros = filter_by.get(j);
                LinkedHashMap<String, ArrayList<String>> filter = new LinkedHashMap<>();
                int z = 0, allCount = 0;
                mine = true;
                while (z < filtros.size()) {
                    if (c.isOnlyLeafs() && z > 0 && z != filtros.size() - 1) {
                        mine = false;
                    }
                    String s1 = filtros.get(z);
                    List<String> splitFilters = Arrays.asList(s1.split("\\|\\|"));
                    List<String> values = new ArrayList<>();
                    LinkedHashMap<String, ArrayList<String>> filter2 = new LinkedHashMap<>();
                    for (String s : splitFilters) {
                        values = Arrays.asList(s.split(":"));

                        ArrayList<String> filterValues = new ArrayList<>();
                        List<String> split = Arrays.asList(values.get(1).split("&"));
                        if (split.size() == 1 && split.get(0).equals("SPLIT")) {
                            List<String> strings = fValues.get(values.get(0));
                            String s2 = strings.get(allCount);
                            filterValues.add(s2);
                            if (allCount < (strings.size() - 1)) {
                                allCount++;
                                z--;
                            } else {
                                allCount = 0;
                            }
                        } else {
                            filterValues.addAll(split);
                        }
                        filter.put(values.get(0), filterValues);
                        filter2 = new LinkedHashMap<>(filter);
                    }
                    Node node = new Node(id, (j + 1), values.get(0), mine, z, null, null, filter2, null, 0l, 0l);
                    node.setGroup(c.isGroup_activities());
                    h.addNode(node);
                    id++;
                    z++;
                }
            }
        }
    }

    public static List<Double> listToDoubleList(List<String> list) {
        List<Double> to = new ArrayList<>();
        for (String s : list) {
            if (!s.isEmpty()) {
                to.add(Double.valueOf(s));
            }
        }

        return to;
    }

    public static String newPatterns(List<Double> thresholds, Map<Integer, List<String>> fp2, String hnName, String p) {
        List<Double> notCalculatedFP = new ArrayList<>();
        for (Double d : thresholds) {
            int key = (int) (d * 100);
            if (fp2.containsKey(key)) {
                List<String> strings = fp2.get(key);
                int i = 0;
                for (String s : strings) {
                    //Print pattern
                    File out = new File(LOG_DIR + hnName + "/" + p + "_" + d + "_" + i + ".png");
                    gv.writeGraphToFile(gv.getGraph(s, "png", "dot"), out);
                    i++;
                }
            } else {
                notCalculatedFP.add(d);
            }
        }
        String th = "";
        for (Double d : notCalculatedFP) {
            th += d + ",";
        }
        th = th.replaceAll(",$", "");
        return th;
    }

    public static void deleteImg(Object id) {
        if (id != null) {
            fs.remove((ObjectId) id);
        }
    }
}


