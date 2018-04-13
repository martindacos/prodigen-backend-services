package bpm;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModelProperty;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.Checksum;

public class Operation{
    private String file;
    private OperationsType operationsType;
    private String model;
    private Double threshold;
    private String id;
    private OperationStatus status;
    @ApiModelProperty(hidden = true)
    private Thread thread;

    public Operation(String file, OperationsType operationsType, String model, Double threshold) {
        this.file = file;
        this.operationsType = operationsType;
        this.model = model;
        this.threshold = threshold;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public OperationsType getOperationsType() {
        return operationsType;
    }

    public void setOperationsType(OperationsType operationsType) {
        this.operationsType = operationsType;
    }

    public Double getThreshold() {
        return threshold;
    }

    public void setThreshold(Double threshold) {
        this.threshold = threshold;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Operation operation = (Operation) o;

        if (!file.equals(operation.file)) return false;
        if (operationsType != operation.operationsType) return false;
        if (model != null ? !model.equals(operation.model) : operation.model != null) return false;
        return threshold != null ? threshold.equals(operation.threshold) : operation.threshold == null;
    }

    @Override
    public int hashCode() {
        int result = file.hashCode();
        result = 31 * result + operationsType.hashCode();
        result = 31 * result + (model != null ? model.hashCode() : 0);
        result = 31 * result + (threshold != null ? threshold.hashCode() : 0);
        return result;
    }

    public OperationStatus getStatus() {
        return status;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setStatus(OperationStatus status) {
        this.status = status;
    }

    public Thread getThread() {
        return thread;
    }

    public void setThread(Thread thread) {
        this.thread = thread;
    }

    public String getId(){
        return String.format("%s:%s@%s/%s", this.operationsType, this.file, this.model, this.threshold);
    }

    public String getCleanId(){
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            return new String(md5.digest(this.getId().getBytes()));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }
}
