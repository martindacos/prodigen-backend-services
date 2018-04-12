package bpm.services;

import bpm.log_editor.data_types.*;
import bpm.log_editor.parser.CSVparser;
import bpm.log_editor.parser.XESparser;
import bpm.log_editor.storage.LogService;
import bpm.log_editor.storage.MongoDAO;
import bpm.log_editor.storage.StorageService;
import bpm.statistics.charts.ChartGenerator;
import com.google.gson.Gson;
import com.mongodb.*;
import com.mongodb.util.JSON;
import es.usc.citius.prodigen.domainLogic.exceptions.*;
import io.swagger.annotations.*;
import org.knowm.xchart.CategoryChart;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.zeroturnaround.zip.ZipUtil;

import java.io.*;
import java.text.ParseException;
import java.util.*;

import static bpm.log_editor.parser.Constants.*;

@RestController
@RequestMapping("")
@Api(tags = "Log management API", description="Log management services")
public class LogController implements Serializable {

    private final StorageService storageService;
    public static MongoClient mongoClient = new MongoClient(HOST, PORT);
    public static DB db = mongoClient.getDB(DB_NAME);

    //public static Rengine engine = new Rengine(new String[]{"--no-save"}, false, null);

    @Autowired
    public LogController(StorageService storageService) {
        this.storageService = storageService;
        CSVparser.storageService = storageService;
        LogService.storageService = storageService;
    }

    //----BASIC LOG FUNCTIONS----//
    //Lists all files in the server
//    @Deprecated
//    @CrossOrigin
//    @GetMapping("/archivos")
//    public ArrayList<LogFile> listUploadedFiles(Model model) throws IOException {
//        return LogService.getLogFiles();
//    }

    @CrossOrigin
    @GetMapping("/logs")
    @ApiOperation(value = "List all files present in the server")
    @ApiResponses({
            @ApiResponse(code = 201, message = "Logs", response = LogFile.class, responseContainer = "List")
    })
    public ArrayList<LogFile> listUploadedLogs() {
        return LogService.getLogFiles();
    }

    @CrossOrigin
    @GetMapping(value = "/logs/{id:.+}")
    @ApiOperation(value = "Get a log by its id")
    @ApiResponses({
            @ApiResponse(code = 201, message = "Log", response = LogFile.class)
    })
    public LogFile getLog(@ApiParam("The log ID") @PathVariable("id") String id) {
        return LogService.getLogByName(id);
    }

    //Lists all mongo collections in the server
/*    @CrossOrigin
    @GetMapping("/dbs")
    @ApiOperation(value = "Lists all mongo collections in the server")
    @ApiResponses({
            @ApiResponse(code = 201, message = "The collections names", response = String.class, responseContainer = "List")
    })
    public Set<String> listDataBases() {
        return MongoDAO.getCollections();
    }*/

    //Get a log by its id
//    @CrossOrigin
//    @RequestMapping(value = "/db/{id:.+}", method = RequestMethod.GET)
//    public LogFile getLogContent(@PathVariable("id") String id) throws IOException {
//        return LogService.getLogByName(id);
//    }

    @CrossOrigin
    @PostMapping("/fileUpload")
    @ApiOperation(value = "Saves file to the server and registers LogFile in collection")
    @ApiResponses({
            @ApiResponse(code = 200, message = "File uploaded correctly", response = ResponseEntity.class)
    })
    public ResponseEntity insertLog(@ApiParam("The file to upload")  @RequestParam("file") MultipartFile file, @ApiParam("The log name") String name, @ApiParam("Has configuration") String state) {
        //Save file
        name = storageService.store(file, name);

        String newname = name;
        //Convert to .csv
        if (name.contains(".xes")) {
            File file1 = new File(storageService.load(name).toString());
            String[] split = file1.getName().split("\\.");
            String fileName = split[0] + ".csv";
            fileName = storageService.checkName(fileName);
            newname = XESparser.read(file1, fileName);
            File oldFile = new File(storageService.load(name).toString());
            oldFile.delete();
        }

        //Insert in db
        String[] parseName = newname.split(".csv");
        String r = parseName[0];
        LogService.insertLog(r, newname, state);
        return ResponseEntity.status(HttpStatus.OK).contentType(MediaType.TEXT_PLAIN).body(r);
    }

    @CrossOrigin
    @PostMapping("/uploadConfig")
    @ApiOperation(value = "Upload one configuration file")
    @ApiResponses({
            @ApiResponse(code = 200, message = "File uploaded correctly", response = ResponseEntity.class),
            @ApiResponse(code = 500, message = "Internal server error", response = ResponseEntity.class)
    })
    public ResponseEntity uploadConfig(@ApiParam("The configuration name") String name, @ApiParam("The file to upload") @RequestParam("config") MultipartFile config) {
        //Insert config to db
        name = storageService.store(config, name + ".ini");

        DBCollection configColl;
        if (db.getCollection("config") != null) {
            configColl = db.getCollection("config");
        } else {
            configColl = db.createCollection("config", null);
        }

        //Read Config File
        Config c;
        try {
            c = new Config(name);
        } catch (Exception e) {
            e.printStackTrace();
            //Delete
            File f = new File("upload-dir/" + name);
            if (f.exists()) {
                f.delete();
            }
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
        Gson gson = new Gson();
        BasicDBObject parse = (BasicDBObject) JSON.parse(gson.toJson(c));
        BasicDBObject obj = new BasicDBObject("name", name);
        obj.put("dir", "upload-dir/" + name);
        obj.put("config", parse);
        obj.put("editable", true);
        configColl.save(obj);

        return ResponseEntity.status(HttpStatus.OK).build();
    }

    @CrossOrigin
    @PostMapping("/loadConfig")
    @ApiOperation(value = "Assign a config to a uploaded file")
    @ApiResponses({
            @ApiResponse(code = 200, message = "Config assigned correctly", response = ResponseEntity.class),
            @ApiResponse(code = 500, message = "Internal server error", response = ResponseEntity.class)
    })
    public ResponseEntity loadConfig(@ApiParam("The log ID") String logId, @ApiParam("The config name") String name) {
        //Read Config File
        Config c;
        try {
            c = new Config(name);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

        LogFile log = LogService.getLogByName(logId);

        log.setConfigName(name);

        Headers headers = new Headers();
        List<String> columns = Arrays.asList(c.getColumns().split(","));
        headers.setData(columns);
        log.insertFile(headers);

        log.setHierarchyCols(headers);

        log.setTraceActTime(c.getTrace_id(), c.getActivity_id(),
                c.getTimestamp(), c.getFinal_timestamp());

        log.setDate(c.getDate_format());
        log.setState(LOADED);
        return ResponseEntity.status(HttpStatus.OK).build();
    }

    @CrossOrigin
    @DeleteMapping(value = "/logs/{id:.+}")
    @ApiOperation(value = "Deletes a log file")
    @ApiResponses({
            @ApiResponse(code = 200, message = "Log deleted correctly", response = ResponseEntity.class),
            @ApiResponse(code = 404, message = "Log not found", response = ResponseEntity.class),
            @ApiResponse(code = 500, message = "Internal server error", response = ResponseEntity.class)
    })
    public ResponseEntity deleteLog(@ApiParam("The log ID") @PathVariable("id") String id){
        LogFile logByName = LogService.getLogByName(id);
        if (logByName == null) {
            return ResponseEntity.notFound().build();
        }
        try {
            LogService.deleteLog(logByName);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }

        return ResponseEntity.ok().build();
    }


    //----COMPLEX LOG FUNCTIONS----//
    @CrossOrigin
    @GetMapping("/headers")
    @ApiOperation(value = "Returns log column headers")
    @ApiResponses({
            @ApiResponse(code = 200, message = "Log column headers returned correctly", response = ResponseEntity.class),
            @ApiResponse(code = 404, message = "Log not found", response = ResponseEntity.class)
    })
    public ResponseEntity<Headers> getFileHeaders(@ApiParam("The log ID") @RequestParam("file") String file) {
        LogFile logByName = LogService.getLogByName(file);

        if (logByName == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(logByName.getHeaders());
    }

    @CrossOrigin
    @PostMapping(value = "/hierarchyCols")
    @ApiOperation(value = "Set the columns used to create hierarchies")
    @ApiResponses({
            @ApiResponse(code = 200, message = "Log columns set correctly", response = ResponseEntity.class),
            @ApiResponse(code = 404, message = "Log not found", response = ResponseEntity.class)
    })
    public ResponseEntity setHierarchyCols(@ApiParam("The log ID") @RequestParam("file") String file, @ApiParam("The log headers") Headers headers) {
        LogFile logByName = LogService.getLogByName(file);

        if (logByName == null) {
            return ResponseEntity.notFound().build();
        }

        logByName.setHierarchyCols(headers);

        return ResponseEntity.ok().build();
    }

    @CrossOrigin
    @PostMapping(value = "/activityCol")
    @ApiOperation(value = "Sets the trace, activity and timestamp columns")
    @ApiResponses({
            @ApiResponse(code = 204, message = "Trace, activity and timestamp columns set correctly", response = ResponseEntity.class),
            @ApiResponse(code = 404, message = "Log not found", response = ResponseEntity.class)
    })
    public ResponseEntity setActIdTime(@ApiParam("The log ID") @RequestParam("file") String file, @ApiParam("The name of the traces column") String trace, @ApiParam("The name of the activities column") String act, @ApiParam("The name of the initial activity time column") String timestamp, @ApiParam("The name of the final activity time column") String timestampf) {
        LogFile logByName = LogService.getLogByName(file);

        if (logByName == null) {
            return ResponseEntity.notFound().build();
        }

        logByName.setTraceActTime(trace, act, timestamp, timestampf);
        logByName.setSampleDate();

        return ResponseEntity.ok().build();
    }

    @CrossOrigin
    @PostMapping("/date")
    @ApiOperation(value = "Set format of the date column")
    @ApiResponses({
            @ApiResponse(code = 204, message = "Format date set correctly", response = ResponseEntity.class),
            @ApiResponse(code = 404, message = "Log not found", response = ResponseEntity.class)
    })
    public ResponseEntity setDateFormat(@ApiParam("The log ID") @RequestParam("file") String file, @ApiParam("The format of the file dates") String data) {
        LogFile logByName = LogService.getLogByName(file);

        if (logByName == null) {
            return ResponseEntity.notFound().build();
        }

        logByName.setDate(data);

        return ResponseEntity.ok().build();
    }

    @CrossOrigin
    @PostMapping(value = "/filterLog")
    @ApiOperation(value = "Removes the non selected columns from a log")
    @ApiResponses({
            @ApiResponse(code = 201, message = "Final headers of the file", response = ResponseEntity.class),
            @ApiResponse(code = 404, message = "Log not found", response = ResponseEntity.class)
    })
    public ResponseEntity<Headers> insertLogToDB(@ApiParam("The log ID") @RequestParam("file") String file,@ApiParam("The log headers")  Headers headers) {
        LogFile logByName = LogService.getLogByName(file);

        if (logByName == null) {
            return ResponseEntity.notFound().build();
        }

        logByName.insertFile(headers);

        return ResponseEntity.ok(logByName.getHeaders());
    }

    //Returns the uniques of the columns to filter a log
    /*@CrossOrigin
    @PostMapping(value = "/nulls")
    public Headers deleteNulls(@RequestParam("file") String file, String column, String value) {
        LogService.getLogByName(file).replaceNulls(column, value);
        return LogService.getLogByName(file).getHeaders();
    }*/

    @CrossOrigin
    @GetMapping(value = "/db")
    @ApiOperation(value = "Return the unique values of each column of the file")
    @ApiResponses({
            @ApiResponse(code = 201, message = "Map with the column and the possible values", response = String.class, responseContainer = "Map")
    })
    public HashMap<String, List<String>> db(@ApiParam("The log ID") @RequestParam("db") String db) {
        return LogService.getLogByName(db).UniquesToFilter();
    }

    @CrossOrigin
    @PostMapping(value = "/replaceValues")
    @ApiOperation(value = "Returns the unique values of each column in the file by replacing some value")
    @ApiResponses({
            @ApiResponse(code = 201, message = "Map with the column and the possible values", response = String.class, responseContainer = "Map")
    })
    public HashMap<String, List<String>> replaceValues(@ApiParam("The log ID") @RequestParam("file") String file, @ApiParam("The name of the column to replace values") String column, @ApiParam("The headers of the file") Headers values, @ApiParam("The value to replace") String replacement) {
        LogService.getLogByName(file).replaceValues(column, values.getData(), replacement);
        return LogService.getLogByName(file).UniquesToFilter();
    }

    @CrossOrigin
    @PostMapping(value = "/hierarchy")
    @ApiOperation(value = "Mine a log with a determined hierarchy")
    @ApiResponses({
            @ApiResponse(code = 204, message = "Log mined correctly", response = ResponseEntity.class),
            @ApiResponse(code = 404, message = "Log not found", response = ResponseEntity.class),
            @ApiResponse(code = 500, message = "Internal server error", response = ResponseEntity.class)
    })
    public ResponseEntity mineLog(@ApiParam("The log ID") @RequestParam("file") String file, @ApiParam("The nodes to mine") @RequestBody ArrayList<Node> nodes) throws EmptyLogException, InvalidFileExtensionException, MalformedFileException, WrongLogEntryException, NonFinishedWorkflowException, ParseException, FileNotFoundException, UnsupportedEncodingException {
        LogFile logByName = LogService.getLogByName(file);

        if (logByName == null) {
            return ResponseEntity.notFound().build();
        }

        Hierarchy h = new Hierarchy();
        h.setNodes(nodes);
        logByName.setTree(h);

        try {
            MongoDAO.queryLog(logByName, h);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }

        return ResponseEntity.ok().build();
    }

    @CrossOrigin
    @PostMapping(value = "/hierarchyConfig")
    @ApiOperation(value = "Mine a log with a determined configuration")
    @ApiResponses({
            @ApiResponse(code = 204, message = "Log mined correctly"),
            @ApiResponse(code = 404, message = "Log not found", response = ResponseEntity.class),
            @ApiResponse(code = 500, message = "Internal server error", response = ResponseEntity.class)
    })
    public ResponseEntity mineLogConfig(@ApiParam("The log ID") @RequestParam("file") String file) throws EmptyLogException, InvalidFileExtensionException, MalformedFileException, WrongLogEntryException, NonFinishedWorkflowException, ParseException, IOException {
        LogFile logByName = LogService.getLogByName(file);

        if (logByName == null) {
            return ResponseEntity.notFound().build();
        }

        Hierarchy h = new Hierarchy();
        logByName.setTree(h);

        try {
            MongoDAO.queryLog(logByName, h);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }

        /*int index = Integer.parseInt(data);
        MinedLog model = logByName.getModels().get(index);*/

        //Create directory
        //File theDir = new File("download-dir/" + file);
        //theDir.mkdir();

        //File opt = new File("/opt/files/" + file);
        //opt.mkdirs();

        String path = null;


        for (MinedLog model : LogService.getLogByName(file).getModels()) {

            //Activity frequency
            //ChartGenerator.ActivityFrequency(model);

            //Activity Duration
            ChartGenerator.ActivityDuration(model);

            //Flux duration by time

            //Flux executions by time

            //Flux duration boxplot

            // R plot
            //path = engine.eval("setwd(\""+ model.getFolderName() + "\")\n").asString();
            //System.out.println(engine.eval("png(\"" + model.getFolderName() + "/BoxPlot.png\")"));
            //System.out.println(engine.eval("boxplot(mpg~cyl,data=mtcars, main=\"Car Milage Data\", \n" +
             //       "  \txlab=\"Number of Cylinders\", ylab=\"Miles Per Gallon\")"));
            //System.out.println(engine.eval("dev.off();"));

            //CREATE ZIP
            ZipUtil.pack(new File("log-dir/" + file + "/"), new File("/opt/files/" + file + ".zip"));
        }

        return ResponseEntity.ok().build();
    }

    @CrossOrigin
    @DeleteMapping("/model")
    @ApiOperation(value = "Delete a model from a log")
    @ApiResponses({
            @ApiResponse(code = 201, message = "Model deleted correctly", response = LogFile.class),
            @ApiResponse(code = 404, message = "Log or model not found", response = ResponseEntity.class),
            @ApiResponse(code = 500, message = "Internal server error", response = ResponseEntity.class)
    })
    public ResponseEntity<LogFile> deleteModel(@ApiParam("The log ID") @RequestParam("file") String file, @ApiParam("The model ID") @RequestParam("data") String data) {
        LogFile logByName = LogService.getLogByName(file);

        if (logByName == null) {
            return ResponseEntity.notFound().build();
        }

        try {
            int index = Integer.parseInt(data);
            if (logByName.getModels().size() <= index) {
                return ResponseEntity.notFound().build();
            }
            LogService.deleteOneLog(logByName, index);
            return ResponseEntity.ok(logByName.deleteModel(index));
        } catch (Exception e) {
            return ResponseEntity.status(500).build();
        }
    }

    @CrossOrigin
    @GetMapping("/download")
    @ApiOperation(value = "Download a log file")
    @ApiResponses({
            @ApiResponse(code = 201, message = "Log file", response = ResponseEntity.class)
    })
    public ResponseEntity<Resource> downloadModel(@ApiParam("The log ID") @RequestParam("file") String file) throws IOException {
        File fi = new File(file);
        InputStreamResource resource = new InputStreamResource(new FileInputStream(fi));
        HttpHeaders h = new HttpHeaders();
        h.add("Content-Disposition", "attachment; filename=\"" + fi.getName() + "\"");
        return ResponseEntity.ok()
                .headers(h)
                .contentLength(fi.length())
                .contentType(MediaType.parseMediaType("application/octet-stream"))
                .body(resource);
    }

    @CrossOrigin
    @PostMapping(value = "/report", produces = "application/zip")
    @ApiOperation(value = "Generate report")
    @ApiResponses({
            @ApiResponse(code = 204, message = "Report", response = ResponseEntity.class)
    })
    public void generateReport(@ApiParam("The log ID") @RequestParam("file") String file, @ApiParam("The model ID") String data) throws IOException {

        //Get lo
        LogFile logByName = LogService.getLogByName(file);
        int index = Integer.parseInt(data);
        MinedLog model = logByName.getModels().get(index);

        //Create directory
        File theDir = new File("download-dir/" + file);
        theDir.mkdir();


        //CREATE ZIP
        ZipUtil.pack(new File("download-dir/" + file + "/"), new File("/opt/files/" + file + ".zip"));
    }

    @CrossOrigin
    @PostMapping(value = "/editReportConfig")
    @ApiOperation(value = "Edit configuration for generate a report")
    @ApiResponses({
            @ApiResponse(code = 200, message = "New configuration", response = ResponseEntity.class),
            @ApiResponse(code = 204, message = "Not acceptable configuration", response = ResponseEntity.class),
            @ApiResponse(code = 304, message = "Not modified configuration", response = ResponseEntity.class)
    })
    public ResponseEntity editReportConfig(@ApiParam("The log ID") @RequestParam("file") String file, @ApiParam("The config ID") @RequestBody Config config) {
        //Initialize null values well & check fails
        if (config.checkNulls()) {
            return ResponseEntity.status(HttpStatus.NOT_ACCEPTABLE).build();
        }

        String configName = config.getConfigName();
        boolean b = !config.getConfigName().equals(config.getOldName());

        BasicDBObject query = new BasicDBObject("name", configName);
        DBCollection con = db.getCollection("config");
        DBCursor cursor = con.find(query);
        DBObject confBD = new BasicDBObject();
        Gson gson = new Gson();

        if (b) {
            //We would overwrite another configuration file
            if (cursor.hasNext()) {
                //Update name
                configName = storageService.checkName(configName);
                //Update names on config
                config.setConfigName(configName);
            }
            //Updates old name
            config.setOldName(configName);
            BasicDBObject parse = (BasicDBObject) JSON.parse(gson.toJson(config));
            confBD.put("config", parse);
            confBD.put("name", configName);
            confBD.put("dir", "upload-dir/" + configName);
            confBD.put("editable", true);
            con.save(confBD);
        } else {
            //Update old name
            config.setOldName(configName);
            BasicDBObject parse = (BasicDBObject) JSON.parse(gson.toJson(config));
            confBD.put("config", parse);
            if (cursor.hasNext()) {
                DBObject next = cursor.next();
                Boolean editable = (Boolean) next.get("editable");
                if (editable == false) {
                    return ResponseEntity.status(HttpStatus.NOT_MODIFIED).build();
                }
                BasicDBObject set = new BasicDBObject("$set", confBD);
                //Update document
                con.update(query, set);
            } else {
                confBD.put("name", configName);
                confBD.put("dir", "upload-dir/" + configName);
                confBD.put("editable", true);
                con.save(confBD);
            }
        }

        config.saveConfigFile(configName);

        LogFile log = LogService.getLogByName(file);
        //Update config name if changes
        if (log != null) {
            log.setConfigName(configName);
        }
        return ResponseEntity.status(HttpStatus.OK).contentType(MediaType.TEXT_PLAIN).body(configName);
    }

    @CrossOrigin
    @GetMapping("/configs")
    @ApiOperation(value = "Lists all configs in the server")
    @ApiResponses({
            @ApiResponse(code = 201, message = "The config names", response = String.class, responseContainer = "List")
    })
    public List<String> listConfig() throws IOException {
        List<String> configs = new ArrayList<>();
        DBCollection con = db.getCollection("config");
        BasicDBObject query = new BasicDBObject();
        DBCursor cursor = con.find(query);
        while (cursor.hasNext()) {
            DBObject next = cursor.next();
            String name = (String) next.get("name");
            configs.add(name);
        }
        return configs;
    }

    @CrossOrigin
    @GetMapping(value = "/config/{id}")
    @ApiOperation(value = "Get a config by its id")
    @ApiResponses({
            @ApiResponse(code = 201, message = "Config", response = ResponseEntity.class),
            @ApiResponse(code = 404, message = "Config not found", response = ResponseEntity.class)
    })
    public ResponseEntity<Config> getConfig(@ApiParam("The config ID") @PathVariable("id") String id) throws IOException {
        BasicDBObject query = new BasicDBObject("name", id + ".ini");
        DBCollection con = db.getCollection("config");
        DBCursor cursor = con.find(query);
        if (cursor.hasNext()) {
            DBObject next = cursor.next();
            Config c = new Gson().fromJson(next.get("config").toString(), Config.class);
            return ResponseEntity.ok(c);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @CrossOrigin
    @DeleteMapping(value = "/config/{id}")
    @ApiOperation(value = "Deletes a config")
    @ApiResponses({
            @ApiResponse(code = 200, message = "Config deleted correctly", response = ResponseEntity.class),
            @ApiResponse(code = 404, message = "Config not found", response = ResponseEntity.class)
    })
    public ResponseEntity deleteConfig(@ApiParam("The config ID") @PathVariable("id") String id) throws IOException {
        //Remove form BD
        BasicDBObject query = new BasicDBObject("name", id + ".ini");
        query.append("editable", true);
        DBCollection con = db.getCollection("config");
        DBObject andRemove = con.findAndRemove(query);
        if (andRemove == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }

        //Remove File
        File f = new File("upload-dir/" + id + ".ini");
        if (f.exists()) {
            f.delete();
        }

        //Delete Config of Logs
        query = new BasicDBObject("configName", id + ".ini");
        con = db.getCollection("logFile");
        BasicDBObject update = new BasicDBObject();
        update.put("$unset", new BasicDBObject("configName", ""));
        con.updateMulti(query, update);

        return ResponseEntity.status(HttpStatus.OK).build();
    }
}

