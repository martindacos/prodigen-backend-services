package bpm;

import bpm.services.OperationResult;

import java.util.*;
import java.util.stream.Collectors;

import static bpm.OperationStatus.*;

public class OperationManager {
    private List<Operation> operations;
    private Map<String, OperationResult> results;
    private static OperationManager _instance;

    private OperationManager(){
        results = new HashMap<>();
        operations = new ArrayList<>();
    }

    public static OperationManager getInstance(){
        if(_instance == null)
            _instance = new OperationManager();
        return _instance;
    }

    public boolean saveResult(String operation, OperationResult result){
        return !results.containsKey(operation) && results.put(operation, result) == null;
    }

    public OperationResult getResult(String operation){
        return results.get(operation);
    }

    public List<Operation> getPendingOperations(){
        return operations.stream().filter(x -> x.getStatus() == WAITING).collect(Collectors.toList());
    }

    public List<Operation> getRuningOperations(){
        return operations.stream().filter(x -> x.getStatus() == RUNNING).collect(Collectors.toList());
    }

    public List<Operation> getFinishedOperations(){
        return operations.stream().filter(x -> x.getStatus() == FINISHED).collect(Collectors.toList());
    }

    public List<Operation> getFailedOperations(){
        return operations.stream().filter(x -> x.getStatus() == FAILED).collect(Collectors.toList());
    }

    public List<Operation> getAllOperations(){
        return operations;
    }

    public boolean addOperation(Operation operation){
        if(operations.stream().anyMatch(x -> Objects.equals(x.getId(), operation.getId())))
            return false;

        operations.add(operation);
        return true;
    }

    public Operation getOperation(String id) {
        List<Operation> op =  operations.stream().filter(x -> Objects.equals(x.getCleanId(), id)).collect(Collectors.toList());

        if(op.isEmpty()) return null;
        else return op.get(0);
    }
}
