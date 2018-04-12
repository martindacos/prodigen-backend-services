package bpm.log_editor.parser;

/***
 Modifiable constants
 **/
public class ConstantsImpl implements Constants{
    public static Boolean D_TASKS = DUMMY_TASKS;
    public static Boolean C_TIMES = CHANGE_TIMES;
    public static Integer N_PARTITIONS = NUM_PARTITIONS;
    public static String MODEL_FORMAT= FORMAT;
    public static String MODEL_EXTENSION_FORMAT= EXTENSION_FORMAT;

    public static void setD_TASKS(Boolean d_TASKS) {
        D_TASKS = d_TASKS;
    }

    public static void setcTimes(Boolean cTimes) {
        C_TIMES = cTimes;
    }

    public static void setnPartitions(Integer nPartitions) {
        N_PARTITIONS = nPartitions;
    }

    public static void setModelFormat(String modelFormat) {
        MODEL_FORMAT = modelFormat;
    }

    public static void setModelExtensionFormat(String modelExtensionFormat) {
        MODEL_EXTENSION_FORMAT = modelExtensionFormat;
    }
}
