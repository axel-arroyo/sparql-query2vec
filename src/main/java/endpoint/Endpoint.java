package endpoint;

import semanticweb.sparql.KmedoidsGenerator;
import semanticweb.sparql.SparqlUtils;
import semanticweb.sparql.preprocess.DeepSetFeatureExtractor;
import semanticweb.sparql.preprocess.TDBExecutionAndFeature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Endpoint {

    private static Map<String, String> map;
    private static void makeMap(String[] args) {
        map = new HashMap<>();
        for (String arg : args) {
            if (arg.contains("=")) {
                //works only if the key doesn't have any '='
                map.put(arg.substring(0, arg.indexOf('=')),
                        arg.substring(arg.indexOf('=') + 1));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        makeMap(args);

        //Getting parameters
        if(args.length == 0){
            System.out.println("Try with some of this parameters:");
            System.out.println("java -jar file.jar kmedoids /path/to/input.csv /path/to/output.csv /path/to/ids_time.csv #-of-centroids");
            System.out.println("java -jar file.jar edit-distance /path/to/input.csv /path/to/output.csv /path/to/prefixes #-of-cores");
            System.out.println("java -jar file.jar deepset-features /path/to/input.csv /path/to/output.csv tables,joins,predicates /path/to/prefixes [--cores=numOfCores] [--length=numOfTuples] [--output-delimiter=symbolToDelimitColumns]");
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
            else if (task.equals("deepset-features")) {

                int cores =  map.get("--cores")  != null ? Integer.parseInt(map.get("--cores")) : 0;
                int length = map.get("--length") != null ? Integer.parseInt(map.get("--length")): 0;
                String output_delimiter = map.get("--output-delimiter") != null ? map.get("--output-delimiter"): "~";

                if(cores > 0) {
                    DeepSetFeatureExtractor.getArrayFeaturesVectorParallel(params[0], params[1], params[2], params[3], length, cores, output_delimiter);
                }
                else {
                    // Case with a restrictive length of queries tu process
                    DeepSetFeatureExtractor.getArrayFeaturesVector(params[0], params[1], params[2], params[3], length, output_delimiter);
                }
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
