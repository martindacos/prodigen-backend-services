package bpm.log_editor.data_types;


import java.util.List;

public class Headers {

    private List<String> data;

    public Headers(){}

    public Headers(List<String> data){
        this.data = data;
    }

    public List<String> getData() {
        return data;
    }

    public void setData(List<String> data) {
        this.data = data;
    }
}
