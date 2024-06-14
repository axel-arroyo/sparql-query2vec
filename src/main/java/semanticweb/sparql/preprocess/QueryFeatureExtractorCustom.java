package semanticweb.sparql.preprocess;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.*;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.function.library.print;
import org.apache.jena.sparql.path.*;
import org.apache.jena.sparql.syntax.Element;

import java_cup.sym;
import liquibase.util.csv.CSVReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.io.FileReader;
import java.io.IOException;

/**
 * Extracts features from the query string using the LSQ features:
 * 1. "filter_bound",
 * 2. "filter_contains",
 * 3. "filter_eq",
 * 4. "filter_exists",
 * 5. "filter_ge",
 * 6. "filter_gt",
 * 7. "filter_isBlank",
 * 8. "filter_isIRI",
 * 9. "filter_isLiteral",
 * 10. "filter_lang",
 * 11. "filter_langMatches",
 * 12. "filter_le",
 * 13. "filter_lt",
 * 14. "filter_ne",
 * 15. "filter_not",
 * 16. "filter_notexists",
 * 17. "filter_or",
 * 18. "filter_regex",
 * 19. "filter_sameTerm",
 * 20. "filter_str",
 * 21. "filter_strends",
 * 22. "filter_strstarts",
 * 23. "filter_subtract",
 * 24. "has_slice",
 * 25. "max_slice_limit",
 * 26. "max_slice_start",
 * 27. "trees"
 * 
 * The features are stored in a double array.
 */
public class QueryFeatureExtractorCustom {

    private static final String SEPARATOR = "ᶲ";

    public static final String[] QUERY_COLUMNS = {
            "filter_bound", "filter_contains", "filter_eq", "filter_exists",
            "filter_ge", "filter_gt", "filter_isBlank", "filter_isIRI",
            "filter_isLiteral", "filter_lang", "filter_langMatches",
            "filter_le", "filter_lt", "filter_ne", "filter_not",
            "filter_notexists", "filter_or", "filter_regex", "filter_sameTerm",
            "filter_str", "filter_strends", "filter_strstarts", "filter_subtract",
            "has_slice", "max_slice_limit", "max_slice_start"
    };

    public static final List<String> LIST_QUERY_COLUMNS = Arrays.asList(QUERY_COLUMNS);

    private final Map<String, Integer> featureIndex;
    private final Map<String, Integer> predicatesCardinalities;

    public QueryFeatureExtractorCustom(String predicatesCardinalitiesFile) {
        featureIndex = new HashMap<>();
        for (int i = 0; i < LIST_QUERY_COLUMNS.size(); i++) {
            featureIndex.put(LIST_QUERY_COLUMNS.get(i), i);
        }
        predicatesCardinalities = new HashMap<>();
        parsePredicatesCardinalities(predicatesCardinalitiesFile);
    }

    public Integer getCardinality(String predicate) {
        if (!predicatesCardinalities.containsKey(predicate)) {
            return -1;
        }
        return predicatesCardinalities.get(predicate);
    }

    void parsePredicatesCardinalities(String predicatesCardinalitiesFile) {
        try (CSVReader reader = new CSVReader(new FileReader(predicatesCardinalitiesFile))) {
            // Skip header
            reader.readNext();

            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                String predicate = nextLine[0];
                int cardinality = Integer.parseInt(nextLine[1]);

                predicatesCardinalities.put(predicate, cardinality);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void visit(OpFilter opFilter, double[] features) {
        for (Expr expr : opFilter.getExprs().getList()) {
            handleExpr(expr, features);
        }
    }

    private void handleExpr(Expr expr, double[] features) {
        if (expr.isFunction()) {
            String funcName = expr.getFunction().getFunctionSymbol().getSymbol();
            if (funcName != null) {
                if (featureIndex.containsKey("filter_" + funcName)) {
                    features[featureIndex.get("filter_" + funcName)] = 1;
                } else {
                    System.out.println("Function not found in feature index: " + funcName);
                }
            }
            // Recursively handle function arguments
            for (Expr arg : expr.getFunction().getArgs()) {
                handleExpr(arg, features);
            }
        } else if (expr.isVariable()) {
        } else if (expr.isConstant()) {
        } else {
            System.out.println("Other expression type: " + expr);
        }
    }

    private void visit(OpSlice opSlice, double[] features) {
        features[featureIndex.get("has_slice")] = 1;
        features[featureIndex.get("max_slice_limit")] = opSlice.getLength();
        features[featureIndex.get("max_slice_start")] = opSlice.getStart() < 0 ? 0 : opSlice.getStart();
    }

    private List<Object> processBGP(OpBGP bgp, Map<String, Integer> jsonCardinalities) {
        List<Triple> triples = bgp.getPattern().getList();
        return buildJoinTree(triples, 0, jsonCardinalities);
    }

    private List<Object> buildJoinTree(List<Triple> triples, int index, Map<String, Integer> jsonCardinalities) {
        if (index >= triples.size() - 1) {
            return processTriple(triples.get(index), jsonCardinalities);
        } else {
            List<Object> result = new ArrayList<>();
            result.add("\"\"JOIN" + SEPARATOR + getJoinString(triples) + "\"\"");
            result.add(buildJoinTree(triples, index + 1, jsonCardinalities));
            result.add(processTriple(triples.get(index), jsonCardinalities));
            return result;
        }
    }

    private String getJoinString(List<Triple> triples) {
        List<String> predicates = new ArrayList<>();
        for (Triple triple : triples) {
            predicates.add(getPredicateString(triple));
        }
        return String.join(SEPARATOR, predicates);
    }

    private List<Object> processTriple(Triple triple, Map<String, Integer> jsonCardinalities) {
        String subjectType = getNodeType(triple.getSubject());
        String objectType = getNodeType(triple.getObject());
        String predicateType = getNodeType(triple.getPredicate());
        String predicate = getPredicateString(triple);
        jsonCardinalities.put(predicate, predicatesCardinalities.get(predicate));

        String tripleStr = "\"\"" + subjectType + "_" + predicateType + "_" + objectType + SEPARATOR + predicate
                + "\"\"";
        return Collections.singletonList(tripleStr);
    }

    private String getNodeType(Node node) {
        if (node.isURI()) {
            return "URI";
        } else if (node.isLiteral()) {
            return "LITERAL";
        } else if (node.isVariable()) {
            return "VAR";
        } else {
            return "UNKNOWN";
        }
    }

    private String getPredicateString(Triple triple) {
        Node predicate = triple.getPredicate();
        if (predicate.isVariable()) {
            return predicate.getName();
        } else if (predicate.isURI()) {
            return predicate.getURI();
        } else {
            System.out.println("Predicate is not URI or Variable: " + predicate);
            return "NONE";
        }
    }

    /**
     * Process the Op tree and return the tree structure as a list of objects. Also
     * updates the features array by visiting the OpFilter and OpSlice operations.
     * 
     * @param op
     * @return
     */
    private List<Object> processOpTree(Op op, double[] filterFeatures, Map<String, Integer> jsonCardinalities) {
        if (op instanceof Op1) {
            if (op instanceof OpFilter) {
                visit((OpFilter) op, filterFeatures);
            } else if (op instanceof OpSlice) {
                visit((OpSlice) op, filterFeatures);
            }
            return processOpTree(((Op1) op).getSubOp(), filterFeatures, jsonCardinalities);
        } else if (op instanceof OpJoin) {
            List<Object> result = new ArrayList<>();
            result.add("\"\"JOIN\"\"");
            result.add(processOpTree(((OpJoin) op).getLeft(), filterFeatures, jsonCardinalities));
            result.add(processOpTree(((OpJoin) op).getRight(), filterFeatures, jsonCardinalities));
            return result;
        } else if (op instanceof Op2) {
            List<Object> left = processOpTree(((Op2) op).getLeft(), filterFeatures, jsonCardinalities);
            List<Object> right = processOpTree(((Op2) op).getRight(), filterFeatures, jsonCardinalities);
            if (left.isEmpty()) {
                return right;
            }
            List<Object> result = new ArrayList<>();
            result.add("\"\"LEFT_JOIN\"\"");
            result.add(left);
            result.add(right);
            return result;
        } else if (op instanceof OpBGP) {
            OpBGP bgp = (OpBGP) op;
            return processBGP(bgp, jsonCardinalities);
        } else if (op instanceof OpSequence) {
            throw new UnsupportedOperationException("OpSequence not supported");
        } else if (op instanceof OpPath) {
            throw new UnsupportedOperationException("OpPath not supported");
        } else {
            throw new UnsupportedOperationException("Op " + op.getName() + " not supported");
        }
    }

    public Map<String, Object> extractFeatures(String queryStr) {
        Query query = QueryFactory.create(queryStr);
        Map<String, Object> result = new HashMap<>();
        double[] filterFeatures = new double[QUERY_COLUMNS.length];
        Map<String, Integer> jsonCardinalities = new HashMap<>();
        List<Object> featuresTree = new ArrayList<>();

        Element queryPattern = query.getQueryPattern();
        if (queryPattern != null) {
            Op op = Algebra.compile(query);
            featuresTree = processOpTree(op, filterFeatures, jsonCardinalities);
        }
        result.put("features", filterFeatures);
        result.put("trees", "\"" + featuresTree + "\"");
        StringBuilder jsonCardinalitiesStr = new StringBuilder();
        jsonCardinalitiesStr.append("\"");
        jsonCardinalitiesStr.append("{");
        for (String entry : jsonCardinalities.keySet()) {
            jsonCardinalitiesStr.append("\"\"" + entry).append("\"\"").append(":\"\"").append(getCardinality(entry))
                    .append("\"\"")
                    .append(",");
        }
        jsonCardinalitiesStr.deleteCharAt(jsonCardinalitiesStr.length() - 1);
        jsonCardinalitiesStr.append("}");
        jsonCardinalitiesStr.append("\"");
        result.put("json_cardinality", jsonCardinalitiesStr);
        return result;
    }

    public static void printOp(String query) {
        Query q = QueryFactory.create(query);
        Op op = Algebra.compile(q);
        System.out.println(op);
    }

    public static void main(String[] args) {
        QueryFeatureExtractorCustom queryFeatureExtractor = new QueryFeatureExtractorCustom(
                "/home/aarroyo/memoria/my_repos/data/dbpedia_predicate_count.csv");
        String queryStr = "SELECT  ?var1 ?var2 (SAMPLE(?var3) AS ?var4) WHERE   { { SELECT DISTINCT  ?var1 ?var2       WHERE         { ?var2  <http://www.wikidata.org/prop/statement/P360>  <http://www.wikidata.org/entity/Q11774891> .           ?var1  <http://www.wikidata.org/prop/P360>  ?var2         }       LIMIT   101     }     OPTIONAL       { ?var2  ?var3                 ?var5 .         ?var6  <http://wikiba.se/ontology#qualifier>  ?var3       }   } GROUP BY ?var1 ?var2 ";
        Map<String, Object> features = queryFeatureExtractor.extractFeatures(queryStr);
        printOp(queryStr);
        System.out.println("features: " + Arrays.toString((double[]) features.get("features")));
        System.out.println("trees: " + features.get("trees"));
        System.out.println("json_cardinality: " + features.get("json_cardinality"));
        // System.out.println(Arrays.toString(queryFeatureExtractor.getFeatures()));
        // System.out.println("Trees: " + features.get("trees"));
    }
}