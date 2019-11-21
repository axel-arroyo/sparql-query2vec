package semanticweb.sparql.preprocess;

import com.hp.hpl.jena.rdf.model.Model;
import semanticweb.sparql.SparqlUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class DeepSetFeatureExtractor {
    public static Model model;
    public static String prefixes = "";

    public static ArrayList<String> default_sets = new ArrayList<>(Arrays.asList("tables","joins","predicates_v2int", "predicates_v2uri"));
    public ArrayList<String> tablesOrder;
    public ArrayList<String> joinsOrder;
    public ArrayList<String> predicatesOrder;
    public ArrayList<String> predicatesUrisOrder;

    public DeepSetFeatureExtractor() {
        this.tablesOrder = new ArrayList<>();
        this.joinsOrder = new ArrayList<>();
        this.predicatesOrder = new ArrayList<>();
        this.predicatesUrisOrder = new ArrayList<>();
//        default_sets.addAll(Arrays.asList("tables","joins","predicates_v2int", "predicates_v2uri"));
    }

    /**
     * Retrieve list of vectors data set from queries in csv file.
     * @param url
     * @param header If csv include header
     * @param queryColumn Csv column that contain query string( Csv must contain other data)
     * @param idColumn   Csv column that contain the query id
     * @return
     */
    public static ArrayList<ArrayList<String>> getArrayQueriesMetaFromCsv(String url,boolean header, int queryColumn,int idColumn,int cardinalityColumns) {
        String row;
        ArrayList<ArrayList<String>> arrayList = new ArrayList<ArrayList<String>>();
        int countQueryInProcess = 0;
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(url));
            if (header){
                //Ignore first read that corresponde with header
                csvReader.readLine();
            }
            while ((row = csvReader.readLine()) != null && countQueryInProcess < 10) {
                countQueryInProcess++;
                String[] rowArray = row.split(",");
                row = rowArray[queryColumn];
                //Remove quotes in init and end of the string...
                row = row.replaceAll("^\"|\"$", "");
                ArrayList<String> predicatesAndId = new ArrayList<>();
                if(idColumn >= 0)
                    predicatesAndId.add(rowArray[idColumn]);
                predicatesAndId.add(row);
                predicatesAndId.add(rowArray[cardinalityColumns]);
                arrayList.add(predicatesAndId);
            }
            csvReader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return arrayList;
    }

    /**
     * Retrieve vectors for DeepSet architecture.
     * @param urlQueries
     * @param namespaces
     * @param output
     * @return
     */
    public static ArrayList<Map<String,Object>> getArrayFeaturesVector(String urlQueries, String output, String sets, String namespaces) {

        model = SparqlUtils.getNamespacesDBPed(namespaces);

        ArrayList<Map<String,Object>> vectors = new ArrayList<>();

        ArrayList<String> featuresArray = new ArrayList<>();
        String[] arraySets = sets.split(",");
        // Add trusted features set to list.
        if(arraySets.length > 1){
            for (String arraySet : arraySets) {
                if (default_sets.contains(arraySet)) {
                    featuresArray.add(arraySet);
                }
            }
        }

        ArrayList<ArrayList<String>> featInQueryList = getArrayQueriesMetaFromCsv(urlQueries,true,1,0,8);
        //we use the size of array intead of -1(csv header) because we use extra column called others.
        boolean initializeHeaders = true;
        for (ArrayList<String> queryArr : featInQueryList) {
            try{
                QueryFeatureExtractor qfe = new QueryFeatureExtractor(queryArr);
                Map<String, Object> queryVecData = qfe.getProcessedData();
//                if (initializeHeaders){
//                    headers = (String[]) queryVecData.keySet().toArray();
//                    initializeHeaders = false;
//                }
                vectors.add(queryVecData);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }

        }
        ArrayList<String> vectorHeader = new ArrayList<>();
        vectorHeader.add("id");
        vectorHeader.addAll(featuresArray);
        vectorHeader.add("cardinality");
        produceCsvArrayVectors(vectorHeader,vectors, output);
        return vectors;
    }

    /**
     * Create a csv with array data passed as parameters.
     * @param list
     * @param filepath
     */
    public static void produceCsvArrayVectors(ArrayList<String> headers, ArrayList<Map<String,Object>> list, String filepath) {
        BufferedWriter br = null;
        try {
            br = new BufferedWriter(new FileWriter(filepath));

            StringBuilder sb = new StringBuilder();
            // Append strings from array
            for (String element : headers) {
                sb.append(element);
                sb.append(";");
            }

            sb.append("\n");
            for (Map<String,Object> queryData : list) {
                for(String header: headers){
                    switch (header){
                        case "tables": {
                            ArrayList<String> qTables = (ArrayList<String>) queryData.get("queryTables");
                            if(qTables.size() > 0){
                                for (String element : qTables) {
                                    sb.append(element);
                                    sb.append(",");
                                }
                            }
                            else {
                                sb.append("EMPTY_VALUE");
                            }
                            break;
                        }
                        case "joins": {
                            ArrayList<String> qTables = (ArrayList<String>) queryData.get("queryJoins");
                            if(qTables.size() > 0){
                                for (String element : qTables) {
                                    sb.append(element);
                                    sb.append(",");
                                }
                            }
                            else {
                                sb.append("EMPTY_VALUE");
                            }
                            break;
                        }
                        case "predicates_v2int": {
                            ArrayList<HashMap<String, Object>> qPredicates = (ArrayList<HashMap<String, Object>>) queryData.get("queryPredicates");
                            if(qPredicates.size() > 0) {
                                for (HashMap<String, Object> element : qPredicates) {
                                    sb.append(element.get("col"));
                                    sb.append(",");
                                    sb.append(element.get("operator"));
                                    sb.append(",");
                                    sb.append(element.get("object"));
                                }
                            }
                            else {
                                sb.append("EMPTY_VALUE");
                            }
                            break;
                        }
                        case "predicates_v2uriYo": {
                            ArrayList<HashMap<String, Object>> qPredicates = (ArrayList<HashMap<String, Object>>) queryData.get("queryPredicatesUris");
                            if(qPredicates.size() > 0) {
                                for (HashMap<String, Object> element : qPredicates) {
                                    sb.append(element.get("col"));
                                    sb.append(",");
                                    sb.append(element.get("operator"));
                                    sb.append(",");
                                    sb.append(element.get("object"));
                                }
                            }
                            else {
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
                    sb.append(";");
                }
                sb.append("\n");
            }

            br.write(sb.toString());
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {

    }
}
