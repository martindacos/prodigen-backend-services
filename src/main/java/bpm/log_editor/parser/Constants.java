package bpm.log_editor.parser;

import static es.usc.citius.prodigen.config.NameConstants.*;

/**
 * All the constants of the app
 */

public interface Constants {
    //MongoDB column names
    String TRACE = "trace";
    String ACTIVITY = "activity";
    String INITIAL_TIME = "timestamp";
    String COMPLETE_TIME = "timestampf";

    //SPARK
    Integer NUM_PARTITIONS = 6;
    String DATABASE = "mongodb://127.0.0.1/test.";
    String MASTER = "local[*]";

    //MongoDB connection
    String HOST = "localhost";
    Integer PORT = 27017;
    String DB_NAME = "test";

    //Add Start and End tasks
    Boolean DUMMY_TASKS = true;
    String S_DUMMY_TASK = START_DUMMY_TASK;
    String E_DUMMY_TASK = END_DUMMY_TASK;
    Boolean CHANGE_TIMES = true;

    String TRACES_SUF = "Traces";
    String CASES_SUF = "CI";
    String ACTIVITIES = "activities";
    String FIRST_ACTIVITY_TIME = "firstTime";
    String LAST_ACTIVITY_TIME = "lastTime";
    String ACTIVITIES_TIME = "activities_time";

    String FORMAT = es.usc.citius.womine.utils.constants.Constants.CM_EXTENSION;
    //String FORMAT = es.usc.citius.womine.utils.constants.Constants.CN_EXTENSION;
    String EXTENSION_FORMAT = "." + FORMAT;

    String LOG_DIR = "log-dir/";
    String PROCESSING = "processing";
    String LOADED = "loaded";
}
