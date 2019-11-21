package endpoint;

import semanticweb.sparql.KmedoidsGenerator;
import semanticweb.sparql.SparqlUtils;
import semanticweb.sparql.preprocess.DeepSetFeatureExtractor;
import semanticweb.sparql.preprocess.TDBExecutionAndFeature;

import java.util.ArrayList;
import java.util.Map;

public class Endpoint {

    public static void main(String[] args) throws Exception {
        //Getting parameters
        if(args.length == 0){
            System.out.println("Try with some of this parameters:");
            System.out.println("java -jar file.jar kmedoids /path/to/input.csv /path/to/output.csv /path/to/ids_time.csv #-of-centroids");
            System.out.println("java -jar file.jar edit-distance /path/to/input.csv /path/to/output.csv /path/to/prefixes #-of-cores");
            System.out.println("java -jar file.jar deepset-features /path/to/input.csv /path/to/output.csv tables,joins,predicates /path/to/prefixes");
            return;
        }
        String[] params = new String[args.length-1];
        System.arraycopy(args, 1, params, 0, args.length - 1);
        try {
            String task = args[0];
            if(task.equals("kmedoids")){
                System.out.println("Entering to kmedoids class");
                KmedoidsGenerator.main(params);
            }
            else if (task.equals("edit-distance")){
                SparqlUtils.main(params);
            }
            else if (task.equals("algebra-features")){
                TDBExecutionAndFeature.main(params);
            }
            else if (task.equals("predicate-features")){
                ArrayList<String[]> notused = SparqlUtils.getArrayFeaturesVector(params[0], params[1], params[2], params[3]);
            }
            else if (task.equals("deepset-features")){
                ArrayList<Map<String, Object>> notused = DeepSetFeatureExtractor.getArrayFeaturesVector(params[0], params[1], params[2], params[3]);
            }
            else {
                System.out.println("The task not found. Pleas use one of them: 'kmedoids, edit-distance, algebra-features, predicate-features'");
            }
        }catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("You need to specify a task as first parameter");
        }

    }
}
