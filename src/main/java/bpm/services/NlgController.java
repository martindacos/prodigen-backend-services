package bpm.services;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
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
import bpm.log_editor.data_types.*;
import bpm.log_editor.parser.CSVparser;
import bpm.log_editor.parser.ConstantsImpl;
import bpm.log_editor.parser.XESparser;
import bpm.log_editor.storage.MongoDAO;
import bpm.log_editor.storage.LogService;
import bpm.log_editor.storage.StorageProperties;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import bpm.log_editor.storage.StorageService;
import bpm.statistics.Arc;
import bpm.statistics.spark.*;

import static bpm.log_editor.parser.CSVparser.savedParsed;
import static bpm.log_editor.storage.MongoDAO.db;
import static bpm.log_editor.storage.MongoDAO.individualToVO;

@RestController
public class NlgController implements Serializable{

    private final StorageService storageService;

    @Autowired
    public NlgController(StorageService storageService) {
        this.storageService = storageService;
        CSVparser.storageService = storageService;
        LogService.storageService = storageService;
    }

}

