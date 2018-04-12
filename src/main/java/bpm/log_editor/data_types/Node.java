package bpm.log_editor.data_types;

import java.util.*;

public class Node {
    Integer id;
    Integer branch;
    String label;
    Boolean mine;
    Boolean group;
    Integer parent;
    ArrayList<Integer> sons;
    ArrayList<String> hierarchy;
    LinkedHashMap<String, ArrayList<String>> filter;
    LinkedHashMap<String, ArrayList<String>> values;

    public Node(){
        this.sons = new ArrayList<>();
        this.hierarchy = new ArrayList<>();
        this.filter = new LinkedHashMap<>();
        this.values = new LinkedHashMap<>();
        this.group = false;
    }

    public Node(Long initialTime, Long finalTime){
        this.sons = new ArrayList<>();
        this.hierarchy = new ArrayList<>();
        this.filter = new LinkedHashMap<>();
        this.values = new LinkedHashMap<>();
        this.group = false;
    }

    public Node(Integer id, Integer branch, String label, Boolean mine, Integer parent, ArrayList<Integer> sons, ArrayList<String> hierarchy,
                LinkedHashMap<String, ArrayList<String>> filter, LinkedHashMap<String, ArrayList<String>> values, Long initialTime, Long finalTime) {
        this.id = id;
        this.branch = branch;
        this.label = label;
        this.mine = mine;
        this.parent = parent;
        this.sons = sons;
        this.hierarchy = hierarchy;
        this.values = values;
        this.filter = filter;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getBranch() {
        return branch;
    }

    public void setBranch(Integer branch) {
        this.branch = branch;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Boolean getMine() {
        return mine;
    }

    public void setMine(Boolean mine) {
        this.mine = mine;
    }

    public Integer getParent() {
        return parent;
    }

    public void setParent(Integer parent) {
        this.parent = parent;
    }

    public ArrayList<Integer> getSons() {
        return sons;
    }

    public void setSons(ArrayList<Integer> sons) {
        this.sons = sons;
    }

    public ArrayList<String> getHierarchy() {
        return hierarchy;
    }

    public void setHierarchy(ArrayList<String> hierarchy) {
        this.hierarchy = hierarchy;
    }

    public LinkedHashMap<String, ArrayList<String>> getValues() {
        return values;
    }

    public void setValues(LinkedHashMap<String, ArrayList<String>> values) {
        this.values = values;
    }

    public LinkedHashMap<String, ArrayList<String>> getFilter() {
        return filter;
    }

    public void setFilter(LinkedHashMap<String, ArrayList<String>> filter) {
        this.filter = filter;
    }

    public Boolean getGroup() {
        return group;
    }

    public void setGroup(Boolean group) {
        this.group = group;
    }
}
