package semanticweb.sparql.preprocess;

import com.hp.hpl.jena.rdf.model.Model;
import semanticweb.sparql.SparqlUtils;

import java.util.ArrayList;
import java.util.Map;

public class ReinforcementLearningExtractor extends FeaturesExtractorBase{
    public static Model model;

    /**
     * Retrieve vectors For use the tpf of queries as sequences for recurrent approach.
     *
     * @param urlQueries        Url for queries file.
     * @param output            Url for output file.
     * @param namespaces        Url for Sparql prefixes file.
     * @param queryColumn       query column position.
     * @param idColumn          IdColumn position.
     * @param execTimeColumn  execution time column position.
     * @param length            Length of queries to get from the csv queries file.
     * @param output_delimiter  Delimiter for csv file column.
     * @param input_delimiter   Delimiter for csv file column.
     * @return ArrayList with Map of queries data generated, see @{@link QueryFeatureExtractor}
     */
    public ArrayList<Map<String, Object>> getArrayFeaturesVector(String urlQueries,
                                                                        String output,
                                                                        String namespaces,
                                                                        int queryColumn,
                                                                        int idColumn,
                                                                        int execTimeColumn,
                                                                        int length,
                                                                        String output_delimiter,
                                                                        String input_delimiter) {

        if(!namespaces.equals("false")){
            model = SparqlUtils.getNamespacesDBPed(namespaces);
        }
        ArrayList<Map<String, Object>> vectors = new ArrayList<>();

        ArrayList<ArrayList<String>> featInQueryList = this.getArrayQueriesMetaFromCsv(urlQueries, true, input_delimiter, queryColumn, idColumn, execTimeColumn, length);
        //we use the size of array intead of -1(csv header) because we use extra column called others.
        for (ArrayList<String> queryArr : featInQueryList) {
            try {
                RLQueryFeatureExtractor qfe = new RLQueryFeatureExtractor(queryArr);
                Map<String, Object> queryVecData = qfe.getProcessedData();
                QueryFeatureExtractor qfedeep = new QueryFeatureExtractor(queryArr);
                Map<String, Object> queryVecDatadeep = qfedeep.getProcessedData();
                vectors.add(queryVecDatadeep);
                vectors.add(queryVecData);
            } catch (Exception ex) {
                ex.printStackTrace();
            }

        }
        ArrayList<String> vectorHeader = new ArrayList<>();
        vectorHeader.add("id");
        vectorHeader.add("tpfs");
        vectorHeader.add("execTime");
//        Todo implement de export produceCsvArrayVectors(vectorHeader, vectors, output, output_delimiter);
        return vectors;
    }
}
