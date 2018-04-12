package bpm.log_editor.data_types;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Config {
    private String columns;
    private String trace_id;
    private String activity_id;
    private String timestamp;
    private String final_timestamp;
    private String category;
    private String workflow;
    private String date_format;

    private String fp_Threshold;
    private String ip_Threshold;
    private boolean activity_duration_heatmap;
    private boolean activity_frequency_heatmap;
    private boolean arcs_frequency;
    private boolean arcs_frequency_number;
    private String prune_Arcs;
    private boolean critical_path;
    private boolean most_frequent_path;
    private boolean arcs_loops;
    private String filter_by;
    private boolean group_activities;
    private String configName;
    private boolean mineAgain;
    private String oldName;
    private boolean onlyLeafs;

    //Constructor por defecto
    public Config() {
    }

    //Constructor que lee desde fichero
    public Config(String filename) throws Exception {
        Properties p = new Properties();
        try {
            p.load(new FileReader("upload-dir/" + filename));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (p.getProperty("fp_Threshold") == null || p.getProperty("ip_Threshold") == null || p.getProperty("activity_duration_heatmap") == null || p.getProperty("activity_frequency_heatmap") == null ||
                p.getProperty("arcs_frequency") == null || p.getProperty("arcs_frequency_number") == null || p.getProperty("prune_Arcs") == null || p.getProperty("critical_path") == null ||
                p.getProperty("most_frequent_path") == null || p.getProperty("arcs_loops") == null || p.getProperty("group_activities") == null || p.getProperty("filter_by") == null ||
                p.getProperty("Columns") == null || p.getProperty("Trace_ID") == null || p.getProperty("Activity_ID") == null || p.getProperty("Timestamp") == null ||
                p.getProperty("Final_Timestamp") == null || p.getProperty("Date_Format") == null || p.getProperty("Category") == null || p.getProperty("Workflow") == null) {

            throw new Exception();
        }
        this.fp_Threshold = p.getProperty("fp_Threshold");
        this.ip_Threshold = p.getProperty("ip_Threshold");
        this.activity_duration_heatmap = Boolean.parseBoolean(p.getProperty("activity_duration_heatmap"));
        this.activity_frequency_heatmap = Boolean.parseBoolean(p.getProperty("activity_frequency_heatmap"));
        this.arcs_frequency = Boolean.parseBoolean(p.getProperty("arcs_frequency"));
        this.arcs_frequency_number = Boolean.parseBoolean(p.getProperty("arcs_frequency_number"));
        this.prune_Arcs = p.getProperty("prune_Arcs");
        this.critical_path = Boolean.parseBoolean(p.getProperty("critical_path"));
        this.most_frequent_path = Boolean.parseBoolean(p.getProperty("most_frequent_path"));
        this.arcs_loops = Boolean.parseBoolean(p.getProperty("arcs_loops"));
        this.group_activities = Boolean.parseBoolean(p.getProperty("group_activities"));
        this.filter_by = p.getProperty("filter_by");
        this.columns = p.getProperty("Columns");
        this.trace_id = p.getProperty("Trace_ID");
        this.activity_id = p.getProperty("Activity_ID");
        this.timestamp = p.getProperty("Timestamp");
        this.final_timestamp = p.getProperty("Final_Timestamp");
        this.category = p.getProperty("Category");
        this.workflow = p.getProperty("Workflow");
        this.date_format = p.getProperty("Date_Format");
        this.onlyLeafs = Boolean.parseBoolean(p.getProperty("onlyLeafs"));

        this.configName = filename;
        this.oldName = filename;
        this.mineAgain = false;
    }

    public Config(Config config) {
        this.columns = config.getColumns();
        this.trace_id = config.getTrace_id();
        this.activity_id = config.getActivity_id();
        this.timestamp = config.getTimestamp();
        this.final_timestamp = config.getFinal_timestamp();
        this.category = config.getCategory();
        this.workflow = config.getWorkflow();
        this.date_format = config.getDate_format();

        this.fp_Threshold = config.getFp_Threshold();
        this.ip_Threshold = config.getIp_Threshold();
        this.activity_duration_heatmap = config.isActivity_duration_heatmap();
        this.activity_frequency_heatmap = config.isActivity_frequency_heatmap();
        this.arcs_frequency = config.isArcs_frequency();
        this.arcs_frequency_number = config.isArcs_frequency_number();
        this.prune_Arcs = config.getPrune_Arcs();
        this.critical_path = config.isCritical_path();
        this.most_frequent_path = config.isMost_frequent_path();
        this.arcs_loops = config.isArcs_loops();
        this.filter_by = config.getFilter_by();
        this.group_activities = config.isGroup_activities();
        this.configName = config.getConfigName();
        this.oldName = config.getOldName();
        this.onlyLeafs = config.isOnlyLeafs();
    }

    public String getFp_Threshold() {
        return fp_Threshold;
    }

    public String getIp_Threshold() {
        return ip_Threshold;
    }

    public String getPrune_Arcs() {
        return prune_Arcs;
    }

    public void setFp_Threshold(String fp_Threshold) {
        this.fp_Threshold = fp_Threshold;
    }

    public void setIp_Threshold(String ip_Threshold) {
        this.ip_Threshold = ip_Threshold;
    }

    public boolean isActivity_duration_heatmap() {
        return activity_duration_heatmap;
    }

    public void setActivity_duration_heatmap(boolean activity_duration_heatmap) {
        this.activity_duration_heatmap = activity_duration_heatmap;
    }

    public boolean isActivity_frequency_heatmap() {
        return activity_frequency_heatmap;
    }

    public void setActivity_frequency_heatmap(boolean activity_frequency_heatmap) {
        this.activity_frequency_heatmap = activity_frequency_heatmap;
    }

    public boolean isArcs_frequency() {
        return arcs_frequency;
    }

    public void setArcs_frequency(boolean arcs_frequency) {
        this.arcs_frequency = arcs_frequency;
    }

    public boolean isArcs_frequency_number() {
        return arcs_frequency_number;
    }

    public void setArcs_frequency_number(boolean arcs_frequency_number) {
        this.arcs_frequency_number = arcs_frequency_number;
    }

    public void setPrune_Arcs(String prune_Arcs) {
        this.prune_Arcs = prune_Arcs;
    }

    public boolean isCritical_path() {
        return critical_path;
    }

    public void setCritical_path(boolean critical_path) {
        this.critical_path = critical_path;
    }

    public boolean isMost_frequent_path() {
        return most_frequent_path;
    }

    public void setMost_frequent_path(boolean most_frequent_path) {
        this.most_frequent_path = most_frequent_path;
    }

    public String getCategory() {
        return category;
    }

    public String getWorkflow() {
        return workflow;
    }

    public void saveConfigFile(String filename) {
        Properties p = new Properties();

        p.setProperty("fp_Threshold", this.fp_Threshold);
        p.setProperty("ip_Threshold", this.ip_Threshold);
        p.setProperty("activity_duration_heatmap", String.valueOf(this.activity_duration_heatmap));
        p.setProperty("activity_frequency_heatmap", String.valueOf(this.activity_frequency_heatmap));
        p.setProperty("arcs_frequency", String.valueOf(this.arcs_frequency));
        p.setProperty("arcs_frequency_number", String.valueOf(this.arcs_frequency_number));
        p.setProperty("prune_Arcs", this.prune_Arcs);
        p.setProperty("critical_path", String.valueOf(this.critical_path));
        p.setProperty("most_frequent_path", String.valueOf(this.most_frequent_path));
        p.setProperty("arcs_loops", String.valueOf(this.arcs_loops));
        p.setProperty("group_activities", String.valueOf(this.group_activities));

        p.setProperty("Trace_ID", String.valueOf(this.trace_id));
        p.setProperty("Activity_ID", String.valueOf(this.activity_id));
        p.setProperty("Timestamp", String.valueOf(this.timestamp));
        p.setProperty("Final_Timestamp", String.valueOf(this.final_timestamp));
        p.setProperty("Category", String.valueOf(this.category));
        p.setProperty("Workflow", String.valueOf(this.workflow));
        p.setProperty("Date_Format", String.valueOf(this.date_format));
        p.setProperty("Columns", this.columns);
        p.setProperty("filter_by", this.filter_by);
        p.setProperty("onlyLeafs", String.valueOf(this.onlyLeafs));

        try {
            p.store(new FileOutputStream("upload-dir/" + filename), null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isArcs_loops() {
        return arcs_loops;
    }

    public void setArcs_loops(boolean arcs_loops) {
        this.arcs_loops = arcs_loops;
    }

    public String getFilter_by() {
        return filter_by;
    }

    public void setFilter_by(String filter_by) {
        this.filter_by = filter_by;
    }

    public boolean isGroup_activities() {
        return group_activities;
    }

    public void setGroup_activities(boolean group_activities) {
        this.group_activities = group_activities;
    }

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public String getTrace_id() {
        return trace_id;
    }

    public void setTrace_id(String trace_id) {
        this.trace_id = trace_id;
    }

    public String getActivity_id() {
        return activity_id;
    }

    public void setActivity_id(String activity_id) {
        this.activity_id = activity_id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getFinal_timestamp() {
        return final_timestamp;
    }

    public void setFinal_timestamp(String final_timestamp) {
        this.final_timestamp = final_timestamp;
    }

    public String getDate_format() {
        return date_format;
    }

    public void setDate_format(String date_format) {
        this.date_format = date_format;
    }

    public void setConfigName(String configName) {
        this.configName = configName;
    }

    public String getConfigName() {
        return configName;
    }

    public boolean isMineAgain() {
        return mineAgain;
    }

    public void setMineAgain(boolean mineAgain) {
        this.mineAgain = mineAgain;
    }

    public String getOldName() {
        return oldName;
    }

    public void setOldName(String oldName) {
        this.oldName = oldName;
    }

    public boolean isOnlyLeafs() {
        return onlyLeafs;
    }

    public void setOnlyLeafs(boolean onlyLeafs) {
        this.onlyLeafs = onlyLeafs;
    }

    public boolean checkNulls() {
        if (this.columns == null || this.trace_id == null || this.activity_id == null || this.timestamp == null || this.final_timestamp == null
                || this.date_format == null || this.configName == null) {
            return true;
        }

        if (this.fp_Threshold == null) {
            this.fp_Threshold = "";
        }
        if (this.ip_Threshold == null) {
            this.ip_Threshold = "";
        }
        if (this.prune_Arcs == null) {
            this.prune_Arcs = "";
        }
        if (this.filter_by == null) {
            this.filter_by = "";
        }
        return false;
    }

    //Check differences on filter log to mine again
    public Boolean checkDiferences(Config other) {
        if (!this.filter_by.equals(other.filter_by)) {
            return true;
        } else if (this.group_activities != other.group_activities) {
            return true;
        } else if (this.onlyLeafs != other.onlyLeafs) {
            return true;
        }else {
            return false;
        }
    }
}
