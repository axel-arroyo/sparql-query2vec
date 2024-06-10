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
import org.apache.jena.sparql.path.*;
import org.apache.jena.sparql.syntax.Element;

import java_cup.sym;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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
    private final double[] features;

    public QueryFeatureExtractorCustom() {
        featureIndex = new HashMap<>();
        for (int i = 0; i < LIST_QUERY_COLUMNS.size(); i++) {
            featureIndex.put(LIST_QUERY_COLUMNS.get(i), i);
        }
        features = new double[LIST_QUERY_COLUMNS.size()];
    }

    public double[] getFeatures() {
        return features;
    }

    private void visit(OpFilter opFilter) {
        for (Expr expr : opFilter.getExprs().getList()) {
            handleExpr(expr);
        }
    }

    private void handleExpr(Expr expr) {
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
                handleExpr(arg);
            }
        } else if (expr.isVariable()) {
        } else if (expr.isConstant()) {
        } else {
            System.out.println("Other expression type: " + expr);
        }
    }

    private void visit(OpSlice opSlice) {
        features[featureIndex.get("has_slice")] = 1;
        features[featureIndex.get("max_slice_limit")] = opSlice.getLength();
        features[featureIndex.get("max_slice_start")] = opSlice.getStart() < 0 ? 0 : opSlice.getStart();
    }

    private List<Object> processBGP(OpBGP bgp) {
        List<Triple> triples = bgp.getPattern().getList();
        return buildJoinTree(triples, 0);
    }

    private List<Object> buildJoinTree(List<Triple> triples, int index) {
        if (index >= triples.size() - 1) {
            return Collections.singletonList(processTriple(triples.get(index)));
        } else {
            List<Object> result = new ArrayList<>();
            result.add("JOIN" + SEPARATOR + getJoinString(triples));
            result.add(buildJoinTree(triples, index + 1));
            result.add(processTriple(triples.get(index)));
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

    private String processTriple(Triple triple) {
        String subjectType = getNodeType(triple.getSubject());
        String objectType = getNodeType(triple.getObject());
        String predicateType = getNodeType(triple.getPredicate());
        String predicate = getPredicateString(triple);
        return subjectType + "_" + predicateType + "_" + objectType + SEPARATOR + predicate;
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
    private List<Object> processOpTree(Op op) {
        if (op instanceof Op1) {
            if (op instanceof OpFilter) {
                visit((OpFilter) op);
            } else if (op instanceof OpSlice) {
                visit((OpSlice) op);
            }
            return processOpTree(((Op1) op).getSubOp());
        } else if (op instanceof OpConditional) {
            System.out.println("Processing OpConditional: " + op);
            List<Object> result = new ArrayList<>();
            result.add(processOpTree(((OpConditional) op).getLeft()));
            result.add(processOpTree(((OpConditional) op).getRight()));
            return result;
        } else if (op instanceof OpJoin) {
            List<Object> result = new ArrayList<>();
            result.add("JOIN");
            result.add(processOpTree(((OpJoin) op).getLeft()));
            result.add(processOpTree(((OpJoin) op).getRight()));
            return result;
        } else if (op instanceof Op2) {
            List<Object> left = processOpTree(((Op2) op).getLeft());
            List<Object> right = processOpTree(((Op2) op).getRight());
            if (left.isEmpty()) {
                return right;
            }
            List<Object> result = new ArrayList<>();
            result.add("LEFT_JOIN");
            result.add(left);
            result.add(right);
            return result;
        } else if (op instanceof OpBGP) {
            OpBGP bgp = (OpBGP) op;
            return processBGP(bgp);
        } else {
            System.out.println("processOpTree Operation not supported: " + op.getName());
            return Collections.emptyList();
        }
    }

    public Map<String, Object> extractFeatures(String queryStr) {
        Query query = QueryFactory.create(queryStr);
        Map<String, Object> result = new HashMap<>();
        List<Object> featuresTree = new ArrayList<>();

        Element queryPattern = query.getQueryPattern();
        if (queryPattern != null) {
            Op op = Algebra.compile(query);
            featuresTree = processOpTree(op);
        }
        result.put("features", features);
        result.put("trees", featuresTree);
        return result;
    }

    public static void main(String[] args) {
        QueryFeatureExtractorCustom queryFeatureExtractor = new QueryFeatureExtractorCustom();
        String queryStr = "PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#> PREFIX  dbpo: <http://dbpedia.org/ontology/> PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX  foaf: <http://xmlns.com/foaf/0.1/> PREFIX  dbprop: <http://dbpedia.org/property/>  SELECT DISTINCT  * WHERE   { ?person  rdf:type  foaf:Person       { ?person  dbprop:viaf  ?viaf }     UNION       { ?person  dbpo:viafId  ?viaf }     UNION       { ?person  dbpo:viafId  ?viaf }     ?person  foaf:depiction  ?picture     FILTER ( str(?viaf) = \"100235826\" )     OPTIONAL       { ?person  foaf:isPrimaryTopicOf  ?wikiPage }     OPTIONAL       { ?person  dbpo:language  ?language }     OPTIONAL       { ?person  dbpo:birthPlace  ?birthPlace }     OPTIONAL       { ?person  dbpo:deathPlace  ?deathPlace }     OPTIONAL       { ?person  dbpo:nationality  ?nationality }     OPTIONAL       { ?person  dbpo:abstract  ?abstract         FILTER ( lang(?abstract) = \"es\" )       }     OPTIONAL       { ?person  dbpo:notableWork  ?notableWork }     OPTIONAL       { ?person  dbpo:movement  ?movement }   } LIMIT   10";
        Map<String, Object> features = queryFeatureExtractor.extractFeatures(queryStr);
        System.out.println(Arrays.toString(queryFeatureExtractor.getFeatures()));
        System.out.println("Trees: " + features.get("trees"));
    }
}