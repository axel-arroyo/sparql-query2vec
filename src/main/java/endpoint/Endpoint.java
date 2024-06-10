package endpoint;

import semanticweb.sparql.KmedoidsGenerator;
import semanticweb.sparql.SparqlUtils;
import semanticweb.sparql.preprocess.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Endpoint {
    private static Map<String, String> map;

    private static void makeMap(String[] args) {
        map = new HashMap<>();
        for (String arg : args) {
            if (arg.contains("=")) {
                // works only if the key doesn't have any '='
                map.put(arg.substring(0, arg.indexOf('=')),
                        arg.substring(arg.indexOf('=') + 1));
            }
        }
    }

    public static void main(String[] args) {
        makeMap(args);

        // Getting parameters
        if (args.length == 0) {
            System.out.println("Try with some of this parameters:");
            System.out.println(
                    "java -jar file.jar kmedoids /path/to/input.csv /path/to/output.csv /path/to/ids_time.csv #-of-centroids");
            System.out.println(
                    "java -jar file.jar edit-distance /path/to/input.csv /path/to/output.csv /path/to/prefixes #-of-cores");
            System.out.println(
                    "java -jar file.jar deepset-features /path/to/input.csv /path/to/output.csv tables,joins,predicates /path/to/prefixes [--cores=numOfCores] [--length=numOfTuples] [--output-delimiter=symbolToDelimitColumns]");
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
        String[] params = new String[args.length - 1];
        System.arraycopy(args, 1, params, 0, args.length - 1);
        try {
            String task = args[0];
            // Define parameters from args
            boolean withHeader = map.get("--with-header") != null && !map.get("--with-header").equals("false");
            boolean withTimeFeatures = map.get("--with-timefeatures") != null
                    && !map.get("--with-timefeatures").equals("false");
            int medoids = map.get("--medoids") != null ? Integer.parseInt(map.get("--medoids")) : 25;
            String configFile = map.get("--config-file") != null ? map.get("--config-file") : "";
            String urlTFPMap = map.get("--tpf-map-file") != null ? map.get("--tpf-map-file") : "";
            String prefixFile = map.get("--prefix-file") != null ? map.get("--prefix-file") : "";
            int idColumn = map.get("--idColumn") != null ? Integer.parseInt(map.get("--idColumn")) : 0;
            int cardinalityColumn = map.get("--cardinalityColumn") != null
                    ? Integer.parseInt(map.get("--cardinalityColumn"))
                    : 8;
            int queryColumn = map.get("--queryColumn") != null ? Integer.parseInt(map.get("--queryColumn")) : 1;
            int cores = map.get("--cores") != null ? Integer.parseInt(map.get("--cores")) : 0;
            int length = map.get("--length") != null ? Integer.parseInt(map.get("--length")) : 0;
            int elemtByCore = map.get("--elements-by-core") != null ? Integer.parseInt(map.get("--elements-by-core"))
                    : 500;
            String output_delimiter = map.get("--output-delimiter") != null ? map.get("--output-delimiter") : ",";
            // Delimitador de elementos dentro de una columna, la coma no es buena debido a
            // que existen uris con coma.
            String output_element_delimiter = map.get("--output-element-delimiter") != null
                    ? map.get("--output-element-delimiter")
                    : "ᶷ";
            String input_delimiter = map.get("--input-delimiter") != null ? map.get("--input-delimiter") : "~";
            int execTimeColumn = map.get("--execTimeColumn") != null ? Integer.parseInt(map.get("--execTimeColumn"))
                    : 7;

            switch (task) {
                case "create-dataset": {
                    // test init espaciado top elementosByEspacio
                    // 0 1 2 3 4
                    int init = Integer.parseInt(params[1]);
                    int pad = Integer.parseInt(params[2]);
                    int top = Integer.parseInt(params[3]);
                    int limit = Integer.parseInt(params[4]);
                    String defaultGragh = params.length == 6 ? params[5] : null;
                    int count = 0;
                    while (init < top) {
                        System.out.println("Procesando rango: "
                                .concat(String.valueOf(init).concat("-").concat(String.valueOf(init + pad))));
                        SparqlUtils.extractDataFromLsq(defaultGragh, params[0], init,
                                init + Integer.parseInt(params[2]), limit);
                        init += Integer.parseInt(params[2]);
                        count++;
                        if (count == 5) {
                            count = 0;
                            Thread.sleep(2000);
                        }
                    }
                    break;
                }
                // case "tsv2csv": {
                // String inputLogsFile = params[0];
                // String inputHdtFile = params[1];
                // String outputFile = params[2];
                // RunQueriesParallel runQueriesParallel = new RunQueriesParallel(inputLogsFile,
                // inputHdtFile, outputFile, prefixFile, output_delimiter, cores);
                //// "/home/daniel/Documentos/ML/rhassan/graph-edit-distance/wikidata_prefixes.csv"
                // ArrayList<String[]> queries =
                // runQueriesParallel.createCsvFromTsv(0,'ᶶ','\t');
                //// runQueriesParallel.proccessData();
                // break;
                // }
                // case "create-query-dataset-parallel": {
                // String inputLogsFile = params[0];
                // String inputHdtFile = params[1];
                // String outputFile = params[2];
                // int init_row = Integer.parseInt(params[3]);
                // int len_exec_results = Integer.parseInt(params[4]);
                // String endpoint = params[5];
                // RunQueriesParallel runQueriesParallel = new RunQueriesParallel(inputLogsFile,
                // inputHdtFile, outputFile, prefixFile, init_row, len_exec_results,
                // output_delimiter, cores);
                //// "/home/daniel/Documentos/ML/rhassan/graph-edit-distance/wikidata_prefixes.csv"
                // runQueriesParallel.proccessData(endpoint);
                //// runQueriesParallel.proccessData();
                // break;
                // }
                case "execute-queries": {
                    // java -jar inputTDBUrl inputLogsFile outputFile
                    String inputTDBUrl = params[0];
                    String inputLogsFile = params[1];
                    String outputFile = params[2];
                    int init_row = Integer.parseInt(params[3]);
                    int len_exec_results = Integer.parseInt(params[4]);
                    RunQueriesSequential runner = new RunQueriesSequential(inputLogsFile, inputTDBUrl, outputFile,
                            prefixFile, init_row, len_exec_results, output_delimiter, idColumn, queryColumn);
                    runner.proccessData2("ᶶ", input_delimiter.charAt(0));
                    break;
                }
                // case "create-query-dataset": {
                // String inputLogsFile = params[0];
                // String inputHdtFile = params[1];
                // String outputFile = params[2];
                // int init_row = Integer.parseInt(params[3]);
                // int len_exec_results = Integer.parseInt(params[4]);
                // String endpoint = params[5];
                // RunQueriesParallel runQueriesParallel = new RunQueriesParallel(inputLogsFile,
                // inputHdtFile, outputFile, prefixFile, init_row, len_exec_results,
                // output_delimiter, cores);
                //// runQueriesParallel.proccessData2(endpoint, "ᶶ",'\t');
                // break;
                // }
                case "kmedoids": {
                    System.out.println("Entering to kmedoids class");
                    KmedoidsGenerator kmedoidsGenerator = new KmedoidsGenerator();
                    kmedoidsGenerator.proccessQueries(params, medoids, input_delimiter,
                            output_delimiter.toCharArray()[0], idColumn, execTimeColumn, withHeader);
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
                    sparqlUtils.calculateEditDistance(input, output, prefixFile.equals("") ? null : prefixFile, cores,
                            input_delimiter.toCharArray()[0], output_delimiter.toCharArray()[0], idColumn, queryColumn,
                            execTimeColumn, elemtByCore);
                    break;
                }
                case "graph-patterns": {
                    // Compute edit distances only with a list of medoids ids file and output the
                    // graph pattern.
                    SparqlUtils sparqlUtils = new SparqlUtils();
                    String input, output, modeidsFile;
                    try {
                        input = params[0];
                        output = params[1];
                        modeidsFile = params[2];

                    } catch (Exception ex) {
                        System.out.println("args[0] : Input csv \n args[1] : Output path \n");
                        return;
                    }
                    sparqlUtils.getGraphPatterns(input, output, modeidsFile, prefixFile.equals("") ? null : prefixFile,
                            input_delimiter.toCharArray()[0], output_delimiter.toCharArray()[0], idColumn, queryColumn,
                            execTimeColumn);
                    break;
                }
                case "algebra-features": {
                    String inputFile = params[0];
                    String outputFile = params[1];
                    TDBExecutionAndFeature.produceALgebraFeatures(inputFile, outputFile, configFile, prefixFile,
                            input_delimiter, output_delimiter, idColumn, queryColumn, execTimeColumn);
                    break;
                }
                case "custom-deepset-features": {
                    String inputFile = params[0];
                    String outputFile = params[1];
                    TDBExecutionAndFeature.produceLSQFeatures(inputFile, outputFile, configFile, prefixFile,
                            input_delimiter, output_delimiter, idColumn, queryColumn, execTimeColumn,
                            cardinalityColumn, withHeader);
                    break;
                }
                case "predicate-features": {
                    ArrayList<String[]> notused = SparqlUtils.getArrayFeaturesVector(params[0], params[1], params[2],
                            params[3]);
                    break;
                }
                case "deepset-features": {
                    DeepSetFeatureExtractor dsfv = new DeepSetFeatureExtractor();
                    if (cores > 0) {

                        dsfv.getArrayFeaturesVectorParallel(params[0], params[1], params[2], params[3], length, cores,
                                output_delimiter, input_delimiter, urlTFPMap);
                    } else {
                        dsfv.getArrayFeaturesVector(params[0], params[1], params[2], params[3], length,
                                output_delimiter, input_delimiter, urlTFPMap);
                    }
                    break;
                }
                case "deepset-watdiv-features": {
                    DeepSetFeatureExtractor dsfv = new DeepSetFeatureExtractor();
                    if (cores > 0) {
                        dsfv.getArrayFeaturesVectorParallel(params[0], params[1], params[2], params[3], length, cores,
                                output_delimiter, input_delimiter, urlTFPMap);
                    } else {
                        dsfv.getArrayFeaturesVector(params[0], params[1], params[2], params[3], queryColumn, idColumn,
                                cardinalityColumn, length, output_delimiter, input_delimiter, urlTFPMap);
                    }
                    break;
                }
                case "recurrent-features": {
                    RecurrentFeaturesExtractor rfv = new RecurrentFeaturesExtractor();
                    rfv.getArrayFeaturesVector(params[0], params[1], params[2], queryColumn, idColumn, execTimeColumn,
                            length, output_delimiter, input_delimiter);
                    break;
                }
                case "rlearning": {
                    ReinforcementLearningExtractor rpfv = new ReinforcementLearningExtractor();
                    rpfv.getArrayFeaturesVector(params[0], params[1], prefixFile, queryColumn, idColumn, execTimeColumn,
                            length, output_delimiter, input_delimiter, output_element_delimiter);
                    break;
                }
                case "execute-sampling-hist": {
                    ReinforcementLearningExtractor rpfv = new ReinforcementLearningExtractor();
                    rpfv.executeSamplingHist(params[0], params[1], output_delimiter, input_delimiter,
                            output_element_delimiter, prefixFile);
                    break;
                }
                case "query-features": {
                    SparqlUtils sparqlUtils = new SparqlUtils();
                    String input, output, medoids_queries;
                    try {
                        input = params[0];
                        output = params[1];
                        medoids_queries = params[2];

                    } catch (Exception ex) {
                        System.out.println("args[0] : Input csv \n args[1] : Output path \n");
                        return;
                    }

                    TDBExecutionAndFeature.produceALgebraFeatures(
                            input,
                            output.concat("algebra_features"),
                            configFile,
                            prefixFile,
                            input_delimiter,
                            output_delimiter,
                            idColumn,
                            queryColumn,
                            execTimeColumn);

                    sparqlUtils.getQueryGraphPatterns(
                            input,
                            output.concat("graph_pattern"),
                            medoids_queries,
                            prefixFile.equals("") ? null : prefixFile,
                            input_delimiter.toCharArray()[0],
                            output_delimiter.toCharArray()[0],
                            idColumn,
                            queryColumn,
                            execTimeColumn);
                    break;
                }
                default: {
                    System.out.println(
                            "The task not found. Pleas use one of them: 'kmedoids, edit-distance, algebra-features, predicate-features'");
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("You need to specify a task as first parameter");
        }
    }
}
