package bpm.log_editor.data_types;

import java.util.ArrayList;

public class Hierarchy {

    //Nodes
    private ArrayList<Node> nodes;

    public Hierarchy() {
    }



    public ArrayList<Node> getNodes() {
        return nodes;
    }

    public void setNodes(ArrayList<Node> nodes) {
        this.nodes = nodes;
    }

    public void addNode(Node node) {
        this.nodes.add(node);
    }
}
