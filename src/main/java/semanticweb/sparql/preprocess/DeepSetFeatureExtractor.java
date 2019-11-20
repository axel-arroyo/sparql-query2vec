package semanticweb.sparql.preprocess;

import java.util.ArrayList;


public class DeepSetFeatureExtractor {

    public ArrayList<String> tablesOrder;
    public ArrayList<String> joinsOrder;
    public ArrayList<String> predicatesOrder;
    public ArrayList<String> predicatesUrisOrder;

    public DeepSetFeatureExtractor() {
        this.tablesOrder = new ArrayList<>();
        this.joinsOrder = new ArrayList<>();
        this.predicatesOrder = new ArrayList<>();
        this.predicatesUrisOrder = new ArrayList<>();
    }
    public static void main(String[] args) {

    }
}
