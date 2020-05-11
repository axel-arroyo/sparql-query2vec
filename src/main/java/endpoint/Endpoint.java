package endpoint;

import semanticweb.sparql.KmedoidsGenerator;
import semanticweb.sparql.SparqlUtils;
import semanticweb.sparql.preprocess.DeepSetFeatureExtractor;
import semanticweb.sparql.preprocess.RecurrentFeaturesExtractor;
import semanticweb.sparql.preprocess.ReinforcementLearningExtractor;
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

    public static void main(String[] args){
        makeMap(args);

        //Getting parameters
        if(args.length == 0){
            System.out.println("Try with some of this parameters:");
            System.out.println("java -jar file.jar kmedoids /path/to/input.csv /path/to/output.csv /path/to/ids_time.csv #-of-centroids");
            System.out.println("java -jar file.jar edit-distance /path/to/input.csv /path/to/output.csv /path/to/prefixes #-of-cores");
            System.out.println("java -jar file.jar deepset-features /path/to/input.csv /path/to/output.csv tables,joins,predicates /path/to/prefixes [--cores=numOfCores] [--length=numOfTuples] [--output-delimiter=symbolToDelimitColumns]");
            System.out.println("java -jar file.jar rlearning\n" +
                    "/path/to/input.csv\n" +
                    "/path/to/output.csv\n" +
                    "/path/to/prefix_file\n" +
                    "[--input-delimiter=symbolToDelimitColumns]\n" +
                    "[--output-delimiter=symbolToDelimitColumns]");
            System.out.println("java -jar file.jar execute-sampling-hist\n" +
                    "/path/to/input.csv\n" +
                    "/path/to/output.csv\n" +
                    "[--input-delimiter=symbolToDelimitColumns]\n" +
                    "[--output-element-delimiter=symbolToDel elem inside columns]\n" +
                    "[--output-delimiter=symbolToDelimitColumns]");
            return;
        }
        String[] params = new String[args.length-1];
        System.arraycopy(args, 1, params, 0, args.length - 1);
        try {
            String task = args[0];
            //Define parameters from args
            String configFile =  map.get("--config-file")  != null ? map.get("--config-file") : "";
            String urlTFPMap =  map.get("--tpf-map-file")  != null ? map.get("--tpf-map-file") : "";
            String prefixFile =  map.get("--prefix-file")  != null ? map.get("--prefix-file") : "";
            int idColumn =  map.get("--idColumn")  != null ? Integer.parseInt(map.get("--idColumn")) : 0;
            int cardinalityColumn =  map.get("--cardinalityColumn")  != null ? Integer.parseInt(map.get("--cardinalityColumn")) : 8;
            int queryColumn =  map.get("--queryColumn")  != null ? Integer.parseInt(map.get("--queryColumn")) : 1;
            int cores =  map.get("--cores")  != null ? Integer.parseInt(map.get("--cores")) : 0;
            int length = map.get("--length") != null ? Integer.parseInt(map.get("--length")): 0;
            String output_delimiter = map.get("--output-delimiter") != null ? map.get("--output-delimiter"): ",";
            //Delimitador de elementos dentro de una columna, la coma no es buena debido a que existen uris con coma.
            String output_element_delimiter = map.get("--output-element-delimiter") != null ? map.get("--output-element-delimiter"): "ᶷ";
            String input_delimiter = map.get("--input-delimiter") != null ? map.get("--input-delimiter"): "~";
            int execTimeColumn =  map.get("--execTimeColumn")  != null ? Integer.parseInt(map.get("--execTimeColumn")) : 7;


            switch (task){
                case "kmedoids": {
                    System.out.println("Entering to kmedoids class");
                    KmedoidsGenerator kmedoidsGenerator = new KmedoidsGenerator();
                    kmedoidsGenerator.proccessQueries(params, input_delimiter.toCharArray()[0], output_delimiter.toCharArray()[0]);
                    break;
                }
                case "edit-distance": {
                    SparqlUtils sparqlUtils = new SparqlUtils();
                    String input, output;
                    try {
                        input = params[0];
                        output = params[1];
                    } catch (Exception ex) {
                        System.out.println("args[0] : Input csv \n args[1] : Output path \n");
                        return;
                    }
                    sparqlUtils.calculateEditDistance(input, output, prefixFile, cores, input_delimiter.toCharArray()[0], output_delimiter.toCharArray()[0], idColumn, queryColumn, execTimeColumn);
                    break;
                }
                case "algebra-features": {
                    String inputFile = params[0];
                    String outputFile = params[1];
                    TDBExecutionAndFeature.produceALgebraFeatures(inputFile, outputFile, configFile, input_delimiter, output_delimiter, idColumn, queryColumn, execTimeColumn);
                    break;
                }
                case "predicate-features": {
                    ArrayList<String[]> notused = SparqlUtils.getArrayFeaturesVector(params[0], params[1], params[2], params[3]);
                    break;
                }
                case "deepset-features": {
                    DeepSetFeatureExtractor dsfv = new DeepSetFeatureExtractor();
                    if(cores > 0) {

                        dsfv.getArrayFeaturesVectorParallel(params[0], params[1], params[2], params[3], length, cores, output_delimiter, input_delimiter, urlTFPMap);
                    }
                    else {
                        dsfv.getArrayFeaturesVector(params[0], params[1], params[2], params[3], length, output_delimiter, input_delimiter, urlTFPMap);
                    }
                    break;
                }
                case "deepset-watdiv-features": {
                    DeepSetFeatureExtractor dsfv = new DeepSetFeatureExtractor();
                    if(cores > 0) {
                        dsfv.getArrayFeaturesVectorParallel(params[0], params[1], params[2], params[3], length, cores, output_delimiter, input_delimiter, urlTFPMap);
                    }
                    else {
                        dsfv.getArrayFeaturesVector(params[0], params[1], params[2], params[3],queryColumn, idColumn,cardinalityColumn, length, output_delimiter, input_delimiter, urlTFPMap);
                    }
                    break;
                }
                case "recurrent-features": {
                    RecurrentFeaturesExtractor rfv = new RecurrentFeaturesExtractor();
                    rfv.getArrayFeaturesVector(params[0], params[1], params[2], queryColumn, idColumn, execTimeColumn, length, output_delimiter, input_delimiter);
                    break;
                }
                case "rlearning": {
                    ReinforcementLearningExtractor rpfv = new ReinforcementLearningExtractor();
                    rpfv.getArrayFeaturesVector(params[0], params[1], params[2], queryColumn, idColumn, execTimeColumn, length, output_delimiter, input_delimiter, output_element_delimiter);
                    break;
                }
                case "execute-sampling-hist": {
                    ReinforcementLearningExtractor rpfv = new ReinforcementLearningExtractor();
                    rpfv.executeSamplingHist(params[0], params[1], output_delimiter, input_delimiter, output_element_delimiter);
                    break;
                }
                default: {
                    System.out.println("The task not found. Pleas use one of them: 'kmedoids, edit-distance, algebra-features, predicate-features'");
                }
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("You need to specify a task as first parameter");
        }
    }
}
