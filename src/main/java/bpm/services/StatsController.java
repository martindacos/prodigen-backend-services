package bpm.services;

import bpm.log_editor.data_types.LogFile;
import bpm.log_editor.parser.CSVparser;
import bpm.log_editor.parser.ConstantsImpl;
import bpm.log_editor.storage.LogService;
import bpm.log_editor.storage.StorageProperties;
import bpm.log_editor.storage.StorageService;
import bpm.statistics.Arc;
import bpm.statistics.spark.*;
import com.mongodb.DBCollection;
import es.usc.citius.prodigen.domainLogic.exceptions.*;
import es.usc.citius.prodigen.domainLogic.workflow.Task.Task;
import es.usc.citius.prodigen.domainLogic.workflow.algorithms.geneticMining.CMTask.CMTask;
import es.usc.citius.prodigen.domainLogic.workflow.algorithms.geneticMining.individual.CMIndividual;
import es.usc.citius.prodigen.spark.FHMSpark;
import es.usc.citius.prodigen.util.IndividualVO;
import es.usc.citius.womine.input.InputSpark;
import es.usc.citius.womine.model.Graph;
import es.usc.citius.womine.model.Pattern;
import es.usc.citius.womine.model.Trace;
import gnu.trove.map.TIntObjectMap;
import io.swagger.annotations.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static bpm.log_editor.parser.CSVparser.savedParsed;
import static bpm.log_editor.storage.MongoDAO.db;
import static bpm.log_editor.storage.MongoDAO.individualToVO;

@RestController
@RequestMapping("")
@Api(tags = "Stats management API", description="Stat management services")
public class StatsController implements Serializable{

    private final StorageService storageService;

    @Autowired
    public StatsController(StorageService storageService) {
        this.storageService = storageService;
        CSVparser.storageService = storageService;
        LogService.storageService = storageService;
    }

    //---PATTERNS---//
    /*@CrossOrigin
    @GetMapping(value = "/frequentPatterns")
    @ApiOperation(value = "Get the frequent patterns of a log with a determined threshold")
    @ApiResponses({
            @ApiResponse(code = 201, message = "Frequent patterns", response = String.class, responseContainer = "List")
    })
    public List<String> frequentPatters(@ApiParam("The log ID") @RequestParam("file") String file, @ApiParam("The model ID") String data, @ApiParam("The threshold") Double threshold) {
        return LogService.getLogByName(file).getFrequentPatterns(Integer.parseInt(data), threshold);
    }

    //Queries a log with a determined hierarchy
    @CrossOrigin
    @GetMapping(value = "/infrequentPatterns")
    @ApiOperation(value = "Get the infrequent patterns of a log with a determined threshold")
    @ApiResponses({
            @ApiResponse(code = 201, message = "Infrequent patterns", response = String.class, responseContainer = "List")
    })
    public List<String> infrequentPatters(@ApiParam("The log ID") @RequestParam("file") String file, @ApiParam("The model ID") String data, @ApiParam("The threshold") Double threshold) {
        return LogService.getLogByName(file).getInfrequentPatterns(Integer.parseInt(data), threshold);
    }

    //Prune arcs with a determined threshold
    @CrossOrigin
    @GetMapping(value = "/pruneArcs")
    @ApiOperation(value = "Prune arcs with a determined threshold")
    @ApiResponses({
            @ApiResponse(code = 201, message = "Prune arcs", response = String.class, responseContainer = "List")
    })
    public List<String>  pruneArcs(@ApiParam("The log ID") @RequestParam("file") String file, @ApiParam("The model ID") String data, @ApiParam("The threshold") Double threshold) {
        List<String> list = new ArrayList<>();
        String s = LogService.getLogByName(file).pruneArcs(Integer.parseInt(data), threshold);
        list.add(s);
        return list;
    }

    /*@CrossOrigin
    @PostMapping(value = "/frequentPatterns")
    @ApiOperation(value = "Saves a frequent pattern")
    @ApiResponses({
            @ApiResponse(code = 201, message = "The log with all stats", response = LogFile.class)
    })
    public LogFile saveFrequentPattern(@ApiParam("The log ID") @RequestParam("file") String file, @ApiParam("The model ID") Integer model, @ApiParam("The pattern") Pattern data) {
        LogService.getLogByName(file).addPattern(model, data, "frequent");
        return LogService.getLogByName(file);
    }

    //Saves an  infrequent log
    @CrossOrigin
    @PostMapping(value = "/infrequentPatterns")
    @ApiOperation(value = "Saves a infrequent pattern")
    @ApiResponses({
            @ApiResponse(code = 201, message = "The log with all stats", response = LogFile.class)
    })
    public LogFile saveInfrequentPattern(@ApiParam("The log ID") @RequestParam("file") String file, @ApiParam("The model ID") Integer model, @ApiParam("The pattern") Pattern data) {
        LogService.getLogByName(file).addPattern(model, data, "infrequent");
        return LogService.getLogByName(file);
    }*/

    //---STATS FUNCTIONS---//
    //Activity stats
    /*@CrossOrigin
    @GetMapping("logsStats/{id}/activity")
    @ApiOperation(value = "Get the activity stats of a uploaded log")
    @ApiResponses({
            @ApiResponse(code = 201, message = "The activity stats", response = ActivityStatisticsVO.class)
    })
    public ActivityStatisticsVO activityStats(@ApiParam("The log ID") @PathVariable String id) {

        AssistantFunctions.setConfig(id);

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

    @CrossOrigin
    @GetMapping("logsStats/{id}/arc")
    @ApiOperation(value = "Get the arc stats of a uploaded log")
    @ApiResponses({
            @ApiResponse(code = 201, message = "The arc stats", response = Arc.class, responseContainer = "List")
    })
    public List<Arc> arcStats(@ApiParam("The log ID") @PathVariable String id) throws EmptyLogException, InvalidFileExtensionException, MalformedFileException, WrongLogEntryException, NonFinishedWorkflowException {
        JavaRDD<Document> orderRDD = AssistantFunctions.setConfig(id);

        //Save activities RDD
        JavaRDD<Document> activities = AssistantFunctions.getAllActivities(orderRDD, false, false);
        AssistantFunctions.setInitialRDD(activities);
        //Save traces RDD
        JavaRDD<Document> traces = AssistantFunctions.getAllTraces(false);
        AssistantFunctions.setTracesRDD(traces);

        //Necesitamos modelo y log
        CMIndividual model = FHMSpark.mine(SparkConnection.getContext(), AssistantFunctions.getTracesRDD());
        ConstantsImpl.setModelFormat(es.usc.citius.womine.utils.constants.Constants.CN_EXTENSION);

        HashMap<Integer, Task> intToTaskLo = new HashMap<>();
        TIntObjectMap<CMTask> tasks = model.getTasks();
        for (int i = 0; i < tasks.size(); i++) {
            CMTask cmTask = tasks.get(i);
            intToTaskLo.put(cmTask.getTask().getMatrixID(), cmTask.getTask());
        }

        IndividualVO individualVO = individualToVO(model, intToTaskLo);

        Graph graph = new Graph(individualVO);

        HashMap<Integer, es.usc.citius.womine.model.Node> nodesSpark = graph.getNodesSpark();
        Map<String, Integer> relationIDs = new HashMap<>();
        for (Map.Entry<Integer, es.usc.citius.womine.model.Node> node : nodesSpark.entrySet()) {
            relationIDs.put(node.getValue().getId().replace(":complete", ""), node.getKey());
        }

        JavaPairRDD<String, Trace> caseInstances = InputSpark.getCaseInstances(graph, individualVO.getFormat(), AssistantFunctions.getTracesRDD(), relationIDs, SparkConnection.getContext());

        List<Arc> arcs = ArcsStatistics.arcsFrequencyOrderedSpark(caseInstances, intToTaskLo);

        ConstantsImpl.setModelFormat(es.usc.citius.womine.utils.constants.Constants.CM_EXTENSION);
        return arcs;
    }

    @CrossOrigin
    @GetMapping("logsStats/{id}/trace")
    @ApiOperation(value = "Get the trace stats of a uploaded log")
    @ApiResponses({
            @ApiResponse(code = 201, message = "The trace stats", response = TraceStatisticsVO.class)
    })
    public TraceStatisticsVO traceStats(@ApiParam("The log ID") @PathVariable String id) {
        JavaRDD<Document> orderRDD = AssistantFunctions.setConfig(id);

        //Save activities RDD
        JavaRDD<Document> activities = AssistantFunctions.getAllActivities(orderRDD, false, false);
        AssistantFunctions.setInitialRDD(activities);
        //Save traces RDD
        JavaRDD<Document> traces = AssistantFunctions.getAllTraces(false);
        AssistantFunctions.setTracesRDD(traces);

        //Create new return object
        TraceStatisticsVO result = new TraceStatisticsVO();

        //Trace frequencies
        result.traceFrequency = TracesStatistics.traceFrequency();
        result.traceRelativeFrequency = TracesStatistics.traceFrequencyRelative();
        result.tracesAverages = TracesStatistics.tracesAverages();
        result.traceLongerShorterDuration = TracesStatistics.traceLongerShorterDuration();
        result.traceFrequency2 = TracesStatistics.traceFrequency2();
        result.firstLastTimeTraceActivity = TracesStatistics.firstLastTimeTraceActivity();
        result.tracesCuartil = TracesStatistics.timeTracesCuartil();

        return result;
    }

    //Upload a log with a specific format for Stats
    @CrossOrigin
    @PostMapping("/uploadLogStats")
    @ApiOperation(value = "Upload a file with a specific format")
    @ApiResponses({
            @ApiResponse(code = 201, message = "The log ID", response = String.class)
    })
    public String insertLog(@ApiParam("The file") @RequestParam("file") MultipartFile file) {
        int randomNum = ThreadLocalRandom.current().nextInt(1, 99999998 + 1);
        Path path = Paths.get(StorageProperties.getLocation());
        File f = new File(path.toString() + "/" +  randomNum);
        while (f.exists()) {
            randomNum = ThreadLocalRandom.current().nextInt(1, 99999998 + 1);
            f = new File(path.toString() + "/" + randomNum);
        }
        storageService.store(file, String.valueOf(randomNum));
        storageService.load(String.valueOf(randomNum)).toString();

        savedParsed(String.valueOf(randomNum), f.getPath());

        return String.valueOf(randomNum);
    }

    //Delete a file for Stats
    @CrossOrigin
    @DeleteMapping(value = "logsStats/{id}")
    @ApiOperation(value = "Deletes a log")
    @ApiResponses({
            @ApiResponse(code = 200, message = "The log was deleted correctly", response = ResponseEntity.class),
            @ApiResponse(code = 500, message = "Internal server error", response = ResponseEntity.class),
            @ApiResponse(code = 404, message = "Log not found", response = ResponseEntity.class)
    })
    public ResponseEntity deLog(@ApiParam("The log ID") @PathVariable("id") String id) throws IOException {
        Path path = Paths.get(StorageProperties.getLocation());
        File f = new File(path.toString() + "/" +  id);
        if (f.exists()) {
            if (f.delete()) {
                DBCollection lo = db.getCollection(id);
                lo.drop();
                return ResponseEntity.status(HttpStatus.OK).build();
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
    }*/
}

