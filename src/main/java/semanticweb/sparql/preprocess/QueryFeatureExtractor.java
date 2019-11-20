package semanticweb.sparql.preprocess;

import com.hp.hpl.jena.datatypes.xsd.impl.XSDBaseNumericType;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.syntax.*;
import semanticweb.sparql.Operator;

import java.util.*;
/*
              Procedimiento:
              Por cada triple de la forma ?a pred1 ?b:
               registro una tabla(tableId del pred1).
               Check if ?a exist in Object list
             */

/**
 * Process a SPARQL query string and extract sets of tables, joins,predicates, and uriPredicates
 * for future use in DeepSet architecture.
 */
public class QueryFeatureExtractor {

    private ArrayList<String> queryTables;
    private ArrayList<String> queryVariables;
    private ArrayList<String> queryJoins;
    private ArrayList<HashMap<String, Object>> queryPredicates;
    private ArrayList<HashMap<String, Object>> queryPredicatesUris;
    private String query;

    public QueryFeatureExtractor(String query) {
        this.query = query;
        this.queryTables = new ArrayList<>();
        this.queryVariables = new ArrayList<>();
        this.queryJoins = new ArrayList<>();
        this.queryPredicates = new ArrayList<>();
        this.queryPredicatesUris = new ArrayList<>();
    }

    private void processVarPredVar(Node subject, Node predicate, Node object) {
        if (!this.queryTables.contains(predicate.getURI())) {
            this.queryTables.add(predicate.getURI());
        }
        //add variables subject to list
        if (!this.queryVariables.contains(subject.getName())) {
            this.queryVariables.add(subject.getName());
        }
        //add variables object to list
        if (!this.queryVariables.contains(object.getName())) {
            this.queryVariables.add(object.getName());
        }
        //add joins  var1_predicateURI_var2
        this.queryJoins.add(
                ""
                        .concat("v")
                        .concat(
                                String.valueOf(this.queryVariables.indexOf(subject.getName()))
                        )
                        .concat("_")
                        .concat(predicate.getURI())
                        .concat("_")
                        .concat("v")
                        .concat(
                                String.valueOf(this.queryVariables.indexOf(object.getName()))
                        )
        );
    }

    /**
     * Logic for subject var, predicate uri, object int literal like (Var1.foaf:age, 29 )
     *
     * @param subject   Subject of the triple pattern frame
     * @param predicate Predicate  of the triple pattern frame
     * @param object    Object  of the triple pattern frame
     */
    private void processVarPredNumeric(Node subject, Node predicate, Node object) {
        if (!queryTables.contains(predicate.getURI())) {
            queryTables.add(predicate.getURI());
        }
        //add variables subject to list
        if (!queryVariables.contains(subject.getName())) {
            queryVariables.add(subject.getName());
        }
        //add Literal object to list
        HashMap<String, Object> pred = new HashMap<>();
        pred.put("col", predicate.getURI());
        pred.put("operator", Operator.EQUAL);
        pred.put("value", object.getLiteralValue());
        queryPredicates.add(pred);
    }

    /**
     * Logic for subject var, predicate uri, object int literal like (Var1.rdf:type, foaf:Person )
     *
     * @param subject   Subject of the triple pattern frame
     * @param predicate Predicate  of the triple pattern frame
     * @param object    Object  of the triple pattern frame
     */
    private void processVarPredUri(Node subject, Node predicate, Node object) {
        if (!this.queryTables.contains(predicate.getURI())) {
            this.queryTables.add(predicate.getURI());
        }
        //add variables subject to list
        if (!this.queryVariables.contains(subject.getName())) {
            this.queryVariables.add(subject.getName());
        }
        HashMap<String, Object> pred = new HashMap<>();
        pred.put("col", predicate.getURI());
        pred.put("object", object.getURI());
        this.queryPredicatesUris.add(pred);
    }

    public Map<String, ArrayList> getProcessedData() {
        Query query = QueryFactory.create(this.query);
        System.out.println(query);

        // Generate algebra
        Op op = Algebra.compile(query);
        op = Algebra.optimize(op);
        System.out.println("AL: " + op);

        Element e = query.getQueryPattern();
        HashMap<String, TriplePath> tpf = new HashMap<>();
        // AlgebraFeatureExtractor.getFeaturesDeepSet(op);
        // This will walk through all parts of the query
        ElementWalker.walk(e,
            // For each element...
            new ElementVisitorBase() {
                // Delete tpf of Optional of queries from list to tpfs to vectorize.
                public void visit(ElementOptional el) {
                    List<Element> elements = ((ElementGroup) el.getOptionalElement()).getElements();
                    for (Element element : elements) {
                        String key = element.toString();
                        tpf.remove(key);
                    }
                }

                // ...when it's a block of triples...
                public void visit(ElementPathBlock el) {
                    // ...go through all the triples...
                    Iterator<TriplePath> triples = el.patternElts();
                    while (triples.hasNext()) {
                        TriplePath t = triples.next();
                        String tripleHash= t.getPath() != null ? t.getSubject() + " " + t.getPath() + " " + t.getObject() : t.getSubject() + " " + t.getPredicate() + " " + t.getObject();
                        tpf.put(tripleHash, t);
                    }
                }
            }
        );
        for (TriplePath t : tpf.values()) {
            // Loop over elements without optional triples.
            // ...and grab the subject
            Node subject = t.getSubject();
            Node object = t.getObject();
            Node predicate = t.getPredicate();

            if (subject.isVariable() && predicate.isURI() && object.isVariable()) {
                //if not int table list add to.
                this.processVarPredVar(subject, predicate, object);
            }
            else if (subject.isVariable() && predicate.isURI() && object.isURI()) {//if not int table list add to.
                this.processVarPredUri(subject, predicate, object);
            }
            else if (subject.isVariable() && predicate.isURI() && object.isLiteral() && object.getLiteralDatatype().getClass() == XSDBaseNumericType.class) {
                this.processVarPredNumeric(subject, predicate, object);
            }
            // Todo Incorporate other cases...
        }
        Map<String, ArrayList> result = new HashMap<>();
        result.put("queryTables", this.queryTables);
        result.put("queryVariables", this.queryVariables);
        result.put("queryJoins", this.queryJoins);
        result.put("queryPredicates", this.queryPredicates);
        result.put("queryPredicatesUris", this.queryPredicatesUris);
        return result;
    }

    public static void main(String[] args) {
        String s = "PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT ?name ?email WHERE {  ?x foaf:knows ?y . ?y foaf:name ?name .  OPTIONAL { ?y foaf:mbox ?email }  }";
        String s1 = "PREFIX dbo: <http://dbpedia.org/ontology/>\n" +
                "PREFIX res: <http://dbpedia.org/resource/>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "SELECT DISTINCT ?uri ?other \n" +
                "WHERE {\n" +
                "\t?uri rdf:type dbo:Film .\n" +
                "        ?uri dbo:starring res:Julia_Roberts .\n" +
                "        ?uri dbo:starring ?other.\n" +
                "}";
        String s2 = "PREFIX dbo: <http://dbpedia.org/ontology/>\n" +
                "PREFIX res:  <http://dbpedia.org/resource/>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "SELECT DISTINCT ?uri ?string \n" +
                "WHERE {\n" +
                "\t?uri rdf:type dbo:Book .\n" +
                "        ?uri dbo:author res:Danielle_Steel .\n" +
                "\tOPTIONAL { ?uri rdfs:label ?string . FILTER (lang(?string) = 'en') }\n" +
                "}";
        String s3 = "PREFIX dbo: <http://dbpedia.org/ontology/>\n" +
                "PREFIX res:  <http://dbpedia.org/resource/>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "SELECT DISTINCT ?uri ?string \n" +
                "WHERE {\n" +
                "\t?uri rdf:type dbo:Book .\n" +
                "        ?uri dbo:author res:Danielle_Steel .\n" +
                "        ?uri  <http://dbpedia.org/ontology/numberOfPages>  336 . \n" +
                "\tOPTIONAL { ?uri rdfs:label ?string . FILTER (lang(?string) = 'en') }\n" +
                "}";
        String[] queries;
        queries = new String[4];
        queries[0] = s;
        queries[1] = s1;
        queries[2] = s2;
        queries[3] = s3;
        for (String query : queries) {
            QueryFeatureExtractor qfe = new QueryFeatureExtractor(query);
            Map<String, ArrayList> data = qfe.getProcessedData();
            System.out.println(data);
        }
    }
}