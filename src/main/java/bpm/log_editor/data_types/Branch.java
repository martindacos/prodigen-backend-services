package bpm.log_editor.data_types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Branch {

    private Integer id;
    private Node[] nodes;
    private LinkedHashMap<String, ArrayList<String>> data;

    public Branch(){
        this.data = new LinkedHashMap<>();
    }

    public Branch(LinkedHashMap<String, ArrayList<String>> data){
        this.data = data;
    }

    public void setData(LinkedHashMap<String, ArrayList<String>> data) {
        this.data = data;
    }

    public HashMap<String, ArrayList<String>> getData() {
        return data;
    }

    public Node[] getNodes() {
        return nodes;
    }

    public void setNodes(Node[] nodes) {
        this.nodes = nodes;
    }
}
