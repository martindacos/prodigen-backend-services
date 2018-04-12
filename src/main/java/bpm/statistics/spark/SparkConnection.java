package bpm.statistics.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import static bpm.log_editor.parser.Constants.*;

public class SparkConnection {

    private static String appName = "ProDiGen Spark";
    private static String sparkMaster = MASTER;
    //Collection name
    private static String coll = null;

    private static JavaSparkContext spContext = null;
    private static SparkSession sparkSession = null;

    public static JavaSparkContext getContext(String colle) {
        if (coll == null || !colle.equals(coll)) {
            coll = colle;
            getSession();
        }
        return spContext;
    }

    public static JavaSparkContext getContext() {
        return spContext;
    }

    public static SparkSession getSession() {
        if (sparkSession != null) {
            sparkSession.stop();
        }

        sparkSession = SparkSession
                .builder()
                .appName(appName)
                .master(sparkMaster)
                .config("spark.mongodb.input.uri", DATABASE + coll)
                .config("spark.mongodb.output.uri", DATABASE + coll)
                .getOrCreate();

        SparkContext sc = sparkSession.sparkContext();
        spContext = new JavaSparkContext(sc);
        spContext.setLogLevel("ERROR");

        return sparkSession;
    }
}
