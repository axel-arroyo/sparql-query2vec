package semanticweb.sparql.preprocess;

import com.hp.hpl.jena.rdf.model.Model;
import semanticweb.RecursiveDeepSetFeaturizeAction;
import semanticweb.sparql.SparqlUtils;
import semanticweb.sparql.utils.DBPediaUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;


public class DeepSetFeatureExtractor {
    public static Model model;
    public static String prefixes = "";

    public static ArrayList<String> default_sets = new ArrayList<>(Arrays.asList("tables", "joins", "predicates_v2int", "predicates_v2uri", "pred_v2uri_cardinality"));
    public ArrayList<String> tablesOrder;
    public ArrayList<String> joinsOrder;
    public ArrayList<String> predicatesOrder;
    public ArrayList<String> predicatesUrisOrder;
    public HashMap<String,Integer> predUrisCardinality;
    public DeepSetFeatureExtractor() {
        this.tablesOrder = new ArrayList<>();
        this.joinsOrder = new ArrayList<>();
        this.predicatesOrder = new ArrayList<>();
        this.predicatesUrisOrder = new ArrayList<>();
        this.predUrisCardinality = new HashMap<>();
//        default_sets.addAll(Arrays.asList("tables","joins","predicates_v2int", "predicates_v2uri"));
    }

    /**
     * Retrieve list of queries tuples  in csv dataset file.
     *
     * @param url   Url fil csv with queries info.
     * @param header      If csv include header
     * @param queryColumn Csv column that contain query string( Csv must contain other data)
     * @param idColumn    Csv column that contain the query id
     * @param length    Length of the queries with cardinality > zero
     * @return a list of Queries in format [idQuery,Query,Cardinality]
     */
    public static ArrayList<ArrayList<String>> getArrayQueriesMetaFromCsv(String url, boolean header, int queryColumn, int idColumn, int cardinalityColumns, int length) {
        String row;
        ArrayList<ArrayList<String>> arrayList = new ArrayList<ArrayList<String>>();
        int countQueryInProcess = 0;
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(url));
            if (header) {
                //Ignore first read that corresponde with header
                csvReader.readLine();
            }
            // if length is equal to zero not restrict the length of queries.
            while ((row = csvReader.readLine()) != null && (countQueryInProcess < length || length == 0)) {
                String[] rowArray = row.split(",");
                row = rowArray[queryColumn];
                //Remove quotes in init and end of the string...
                row = row.replaceAll("^\"|\"$", "");
                ArrayList<String> predicatesAndId = new ArrayList<>();
                if (idColumn >= 0)
                    predicatesAndId.add(rowArray[idColumn]);
                predicatesAndId.add(row);
                predicatesAndId.add(rowArray[cardinalityColumns]);
                try {
                    if (Integer.parseInt(rowArray[cardinalityColumns]) > 0) {
                        countQueryInProcess++;
                    }
                    else {
                        // If cardinality not > 0 not add the query to list.
                        continue;
                    }
                }
                catch (Exception ex){
                    ex.printStackTrace();
                    continue;
                }

                arrayList.add(predicatesAndId);
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return arrayList;
    }

    /**
     * Retrieve vectors for DeepSet architecture.
     *
     * @param urlQueries Url for queries file.
     * @param output Url for output file.
     * @param namespaces Url for Sparql prefixes file.
     * @param length Length of queries to get from the csv queries file.
     * @param output_delimiter Delimiter for csv file column.
     * @return ArrayList with Map of queries data generated, see @{@link QueryFeatureExtractor}
     */
    public static ArrayList<Map<String, Object>> getArrayFeaturesVector(String urlQueries, String output, String sets, String namespaces, int length,String output_delimiter) {

        model = SparqlUtils.getNamespacesDBPed(namespaces);

        ArrayList<Map<String, Object>> vectors = new ArrayList<>();

        ArrayList<String> featuresArray = new ArrayList<>();
        String[] arraySets = sets.split(",");
        // Add trusted features set to list.
        if (arraySets.length > 1) {
            for (String arraySet : arraySets) {
                if (default_sets.contains(arraySet)) {
                    featuresArray.add(arraySet);
                }
            }
        }

        ArrayList<ArrayList<String>> featInQueryList = getArrayQueriesMetaFromCsv(urlQueries, true, 1, 0, 8,length);
        //we use the size of array intead of -1(csv header) because we use extra column called others.
        boolean initializeHeaders = true;
        for (ArrayList<String> queryArr : featInQueryList) {
            try {
                QueryFeatureExtractor qfe = new QueryFeatureExtractor(queryArr);
                Map<String, Object> queryVecData = qfe.getProcessedData();
                vectors.add(queryVecData);
            } catch (Exception ex) {
                ex.printStackTrace();
            }

        }
        ArrayList<String> vectorHeader = new ArrayList<>();
        vectorHeader.add("id");
        vectorHeader.addAll(featuresArray);
        vectorHeader.add("cardinality");
        produceCsvArrayVectors(vectorHeader, vectors, output, output_delimiter);
        return vectors;
    }


    /**
     * Retrieve vectors for DeepSet architecture processing in parallel
     *
     * @param urlQueries Url for queries file.
     * @param output Url for output file.
     * @param namespaces Url for Sparql prefixes file.
     * @param length Length of queries to get from the csv queries file.
     * @param cores The count of cores to use in parallel process.
     * @param output_delimiter Delimiter for csv file column.
     * @return ArrayList with Map of queries data generated, see @{@link QueryFeatureExtractor}
     */
    public static ArrayList<Map<String, Object>> getArrayFeaturesVectorParallel(String urlQueries, String output, String sets, String namespaces, int length, int cores, String output_delimiter) {

        model = SparqlUtils.getNamespacesDBPed(namespaces);
        ArrayList<String> featuresArray = new ArrayList<>();
        String[] arraySets = sets.split(",");
        // Add trusted features set to list.
        if (arraySets.length > 1) {
            for (String arraySet : arraySets) {
                if (default_sets.contains(arraySet)) {
                    featuresArray.add(arraySet);
                }
            }
        }
        ArrayList<ArrayList<String>> featInQueryList = getArrayQueriesMetaFromCsv(urlQueries, true, 1, 0, 8, length);
        ForkJoinPool pool = new ForkJoinPool();

        RecursiveDeepSetFeaturizeAction task = new RecursiveDeepSetFeaturizeAction(featInQueryList, featuresArray, cores, output,output_delimiter, 0, featInQueryList.size());
        return pool.invoke(task);
    }

    /**
     * Create a csv with array data passed as parameters.
     * @param headers {@link ArrayList} With header for output file to generate
     * @param list  Queries data list.
     * @param filepath Output file path.
     * @param indexStart Index start tuple for build the output filename with de format filename_indexStart_indexLast.csv
     * @param indexLast Index last tuple for build the output filename with de format filename_indexStart_indexLast.csv
     * @param output_delimiter Delimiter for csv data columns.
     */
    public static void produceCsvArrayVectors(ArrayList<String> headers, ArrayList<Map<String, Object>> list, String filepath, int indexStart, int indexLast, String output_delimiter) {
        String extension = filepath.substring(filepath.length()-4);
        produceCsvArrayVectors(
                headers,
                list,
                filepath.substring(0,filepath.length()-4).concat(String.valueOf(indexStart)).concat("_").concat(String.valueOf(indexLast)).concat(extension),
                output_delimiter
        );
    }

    /**
     * Create a csv with array data passed as parameters.
     * @param headers {@link ArrayList} With header for output file to generate
     * @param list  Queries data list.
     * @param filepath Output file path.
     * @param output_delimiter Delimiter for csv data columns.
     */
    public static void produceCsvArrayVectors(ArrayList<String> headers, ArrayList<Map<String, Object>> list, String filepath, String output_delimiter) {
        BufferedWriter br;
        BufferedWriter brpred_uri;
        try {
            br = new BufferedWriter(new FileWriter(filepath));

            StringBuilder sb = new StringBuilder();
            StringBuilder sbbrpred_uri = new StringBuilder();
            // Append strings from array
            for (String element : headers) {
                sb.append(element);
                sb.append(output_delimiter);
            }
            brpred_uri = new BufferedWriter(new FileWriter(filepath.concat(".preduri.txt")));
            sb.append("\n");
            HashMap<String, String> namespases = SparqlUtils.getNamespacesStr("/home/daniel/Documentos/Web_Semantica/Work/Sparql2vec/prefixes.txt");
            HashMap<String,Integer> tpfCardinalities = new HashMap<>();
            String ENDPOINT = "https://dbpedia.org/sparql";
            for (Map<String, Object> queryData : list) {
                for (String header : headers) {
                    switch (header) {
                        case "tables": {
                            ArrayList<String> qTables = (ArrayList<String>) queryData.get("queryTables");
                            if (qTables.size() > 0) {
                                for (String element : qTables) {
                                    sb.append(element);
                                    sb.append(",");
                                }
                            } else {
                                sb.append("EMPTY_VALUE");
                            }
                            break;
                        }
                        case "joins": {
                            ArrayList<String> qTables = (ArrayList<String>) queryData.get("queryJoins");
                            if (qTables.size() > 0) {
                                for (String element : qTables) {
                                    sb.append(element);
                                    sb.append(",");
                                }
                            } else {
                                sb.append("EMPTY_VALUE");
                            }
                            break;
                        }
                        case "predicates_v2int": {
                            ArrayList<HashMap<String, Object>> qPredicates = (ArrayList<HashMap<String, Object>>) queryData.get("queryPredicates");
                            if (qPredicates.size() > 0) {
                                for (HashMap<String, Object> element : qPredicates) {
                                    sb.append(element.get("col"));
                                    sb.append(",");
                                    sb.append(element.get("operator"));
                                    sb.append(",");
                                    sb.append(element.get("value"));
                                    sb.append(",");
                                }
                            }
                            else {
                                sb.append("EMPTY_VALUE");
                            }
                            break;
                        }
                        case "predicates_v2uri": {
                            ArrayList<HashMap<String, Object>> qPredicates = (ArrayList<HashMap<String, Object>>) queryData.get("queryPredicatesUris");
                            if (qPredicates.size() > 0) {
                                for (HashMap<String, Object> element : qPredicates) {
                                    sb.append(element.get("col"));
                                    sb.append(",");
                                    sb.append(element.get("operator"));
                                    sb.append(",");
                                    sb.append(element.get("object"));
                                    sb.append(",");
                                }
                            } else {
                                sb.append("EMPTY_VALUE");
                            }
                            break;
                        }
                        case "pred_v2uri_cardinality": {
                            ArrayList<HashMap<String, Object>> qPredicates = (ArrayList<HashMap<String, Object>>) queryData.get("queryPredicatesUris");
                            if (qPredicates.size() > 0) {
                                for (HashMap<String, Object> element : qPredicates) {

                                    sb.append(element.get("col"));
                                    sb.append(",");
                                    sb.append(element.get("operator"));
                                    sb.append(",");
                                    sb.append(element.get("object"));
                                    sb.append(",");
                                    String hash = String.valueOf(element.get("sampling_query_id"));

                                    if(tpfCardinalities.get(hash) != null) {
                                        int card = tpfCardinalities.get(hash);
                                        sb.append(card);
                                        sb.append(",");
                                    }
                                    else {
                                        String query = String.valueOf(element.get("sampling_query"));
                                        int result = DBPediaUtils.execQuery(query, ENDPOINT,  namespases);
                                        sb.append(result);
                                        sb.append(",");
                                        tpfCardinalities.put(hash,result);
                                        sbbrpred_uri.append(hash.concat(" , ".concat(String.valueOf(result))));
                                        sbbrpred_uri.append("\n");
                                    }
                                }
                            } else {
                                sb.append("EMPTY_VALUE");
                            }
                            break;
                        }
                        case "id":
                        case "cardinality": {
                            sb.append(queryData.get(header));
                        }
                    }
                    // Add separator
                    sb.append(output_delimiter);
                }
                sb.append("\n");
            }

            br.write(sb.toString());
            brpred_uri.write(sbbrpred_uri.toString());
            br.close();
            brpred_uri.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
