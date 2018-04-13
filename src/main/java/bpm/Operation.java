package bpm;

public class Operation {
    private String file;
    private OperationsType operationsType;
    private String model;
    private Double threshold;
private String id;

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
}
