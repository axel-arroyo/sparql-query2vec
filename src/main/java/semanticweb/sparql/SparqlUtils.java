package semanticweb.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.syntax.*;
import liquibase.util.csv.opencsv.CSVReader;
import nanoxml.XMLElement;
import semanticweb.EditDistanceAction;
import semanticweb.GraphBuildAction;
import semanticweb.RDF2GXL;
import util.Graph;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

public class SparqlUtils {


	final public static String SPARQL_VAR_NS = "http://wimmics.inria.fr/kolflow/qp#";
    public static Model model;
    public static String prefixes = "";
	public static ArrayList<String[]> queriesError = new ArrayList();
    public static void getPropsAndObjectCount(){
//		Map<String, Integer> map = new HashMap<String, Integer>();
		ParameterizedSparqlString qs = new ParameterizedSparqlString(""+
				"select distinct ?property where {\n"
				+ "  ?subject ?property ?object . \n" +
				"}");

		QueryExecution exec = QueryExecutionFactory.sparqlService("http://dbpedia.org/sparql", qs.asQuery());

		ResultSet results = exec.execSelect();
		try {
			BufferedWriter br = new BufferedWriter(new FileWriter("properties1.txt"));
			BufferedWriter prop_count = new BufferedWriter(new FileWriter("properties_count1.csv"));

			StringBuilder sb = new StringBuilder();
			StringBuilder sb2 = new StringBuilder();
		int count = 0;
		while (results.hasNext()) {
			String prop = results.next().get("property").toString();
			sb.append("<"+prop+">");
			sb.append("\n");
			sb2.append("<"+prop+">");
			sb2.append(",");
			sb2.append(getObjectCount(prop));
			sb2.append("\n");

			count++;
		}
		br.write(sb.toString());
		br.close();
		prop_count.write(sb2.toString());
		prop_count.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
//		System.out.println(map.toString());
    }

	/**
	 * Retrieve array list of csv file, using delimiter  column param
	 * @param url
	 * @param delimiterCol
	 * @param delimiterRow
	 * @return
	 */
    public static ArrayList getArrayFromCsvFile(String url, String delimiterCol, String delimiterRow) {
		BufferedReader csvReader;
		String row;
		ArrayList arrayList = new ArrayList();
		try {
			csvReader = new BufferedReader(new FileReader(url));
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(delimiterCol);
				arrayList.add(data);
			}
			csvReader.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return arrayList;
	}
	public static Integer countQueryInProcess = 0;
    /**
     * Retrieve list of predicates array in file
     * @param url String
     * @return Array of array list of strings
     */
    public static ArrayList<ArrayList<String>> getArrayQueries(String url) {
        String row;
        ArrayList<ArrayList<String>> arrayList = new ArrayList<ArrayList<String>>();
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(url));
            while ((row = csvReader.readLine()) != null) {
				countQueryInProcess++;
                ArrayList<String> predicates = retrievePredicatesInTriples(row);
                arrayList.add(predicates);
            }
            csvReader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return arrayList;
    }
	/**
	 * Retrieve list of predicates array in file
	 * @param url String
	 * @return Array of array list of strings
	 */
	public static ArrayList<ArrayList<String>> getArrayQueriesFromCsv(String url,boolean header, int queryColumn,int idColumn) {
		String row;
		ArrayList<ArrayList<String>> arrayList = new ArrayList<ArrayList<String>>();
		int count = 0;
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader(url));
			if (header){
				//Ignore first read that corresponde with header
				csvReader.readLine();
			}
			while ((row = csvReader.readLine()) != null) {
				countQueryInProcess++;
				String[] rowArray = row.split(",");
				row = rowArray[queryColumn];
				//Remove quotes in init and end of the string...
				row = row.replaceAll("^\"|\"$", "");
				ArrayList<String> predicatesAndId = new ArrayList<>();
				if(idColumn >= 0)
					predicatesAndId.add(rowArray[idColumn]);
				predicatesAndId.addAll(retrievePredicatesInTriples(row));
				arrayList.add(predicatesAndId);
			}
			csvReader.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return arrayList;
	}

	public static ArrayList<String[]> getArrayFromCsvFile(String url) {

        String delimiterCol = ",";
		String delimiterRow = "\n";
		return  getArrayFromCsvFile(url, delimiterCol, delimiterRow);
	}
	/**
	 *
	 * @return
	 */
	public static HashMap<String, String> getNamespacesStr(String url){
		String prefixes = "";
		Model model = ModelFactory.createDefaultModel();
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader(url));
			String row;
			while ((row = csvReader.readLine()) != null) {
				String[] predicates = row.split("\t");
				model.setNsPrefix(predicates[0], predicates[1]);
				prefixes = prefixes.concat("PREFIX ").concat(predicates[0]).concat(": ").concat("<").concat(predicates[1]).concat("> \n");
			}
			csvReader.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return (HashMap<String, String>) model.getNsPrefixMap();
	}
    /**
     *
     * @return
     */
    public static Model getNamespaces(String url){

        Model model = ModelFactory.createDefaultModel();
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(url));
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] predicates = row.split("\t");
                model.setNsPrefix(predicates[0], predicates[1]);
                prefixes = prefixes.concat("PREFIX ").concat(predicates[0]).concat(": ").concat("<").concat(predicates[1]).concat("> \n");
            }
            csvReader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return model;
    }
    /**
     *
	 * Read a file with prefix in format:
	 * 	PREFIX b3s: <http://b3s.openlinksw.com/>
	 * 	PREFIX b3s2: <http://b3s.openlinksw2.com/>
     * @return a {@link Model} object with the prefix loaded
     */
    public static Model getNamespacesDBPed(String url){

        Model model = ModelFactory.createDefaultModel();
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(url));
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] predicates = row.split(" ",2)[1].split(":",2);
                model.setNsPrefix(predicates[0], predicates[1].replaceAll(" ","").replace("<","").replace(">",""));
                prefixes = prefixes.concat(row).concat("\n");
            }
            csvReader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return model;
    }

    public static HashMap<String,Integer> readQueryTPFSampling(String route, Character separator) throws IOException {
		InputStreamReader csv  = new InputStreamReader(new FileInputStream(route));
		CSVReader csvReader = new CSVReader (csv,separator);
		String[] record;
		HashMap<String,Integer> tpfSamplings = new HashMap<>();
		int count = 0;
		while ((record = csvReader.readNext()) != null) {
			if(record[0].equals("")){
				continue;
			}
			count++;
			System.out.println(count);
			tpfSamplings.put(record[0].split("/")[8],Integer.parseInt(record[1].replaceAll(" ","")));
		}
		return tpfSamplings;
	}
    /**
	 *
	 * @return
	 */
	public static Model getNamespaces(){
		String url = "Sparql2vec/prefixes.txt";
		return getNamespaces(url);
	}
    /**
     * Retrieve array of queries in vectors way
     * @param urlQueries
     * @param urlFeatures
     */
	public static ArrayList<String[]> getArrayFeaturesVector(String urlQueries, String urlFeatures, String namespaces, String output) {

        model = getNamespacesDBPed(namespaces);

	    ArrayList<String[]> vectors = new ArrayList<String[]>();

        //Get features list, in [0] uri, in [1] frequency
	    ArrayList<String[]> featuresArray = getArrayFromCsvFile(urlFeatures);
		Map<String,Integer> featuresMap = new HashMap<>();
	    ArrayList<ArrayList<String>> featInQueryList = getArrayQueriesFromCsv(urlQueries,true,1,0);
		//we use the size of array intead of -1(csv header) because we use extra column called others.
	    String[] vectorheader = new String[featuresArray.size()+2];
        vectorheader[0] = "id";
		vectorheader[1] = "OTHER";
        int i = 2;
        while (i < featuresArray.size()) {
            featuresMap.put(featuresArray.get(i)[0],i);
            vectorheader[i] = featuresArray.get(i)[0];
            i++;
        }

//        produceCsvArray2(featInQueryList,output);
		for (ArrayList<String> queryArr : featInQueryList) {
			String[] vector = new String[vectorheader.length];
			boolean idSeted = false;
			for (String s : queryArr) {
				try {
					if(!idSeted)
					{
						idSeted = true;
						vector[0] = s;
						continue;
					}
					int index = featuresMap.get("<" + s + ">");
					if(vector[index] == null)
						vector[index] = String.valueOf('0');
					vector[index] = String.valueOf(Integer.parseInt(vector[index]) + 1);
				} catch (Exception ex) {
					//ex.printStackTrace();
					if(vector[1] == null)
						vector[1] = String.valueOf('0');
					vector[1] = String.valueOf(Integer.parseInt(vector[1]) + 1);
				}

			}
			vectors.add(vector);
		}

		produceCsvArrayVectors(vectorheader,vectors,output);
		return vectors;
    }

	public static String getObjectCount(String property) {
		ParameterizedSparqlString qs = new ParameterizedSparqlString("" +
				"Select (count(?object) as ?count) Where {\n"
				+ "  ?subject <" + property + "> ?object . \n" +
				"}");

		QueryExecution exec = QueryExecutionFactory.sparqlService("http://dbpedia.org/sparql", qs.asQuery());

		ResultSet results = exec.execSelect();

		if (results.hasNext())
			return results.next().getLiteral("count").getString();
		else
			return "0";
	}

	public static String clearQuery(String s) {
		return s.replaceAll("&format=(json|xml|html)","").
				replaceAll("&output=(json|xml|html)","");
	}
	public static String replacePrefixes(String s) {
	    try {
			Query query = QueryFactory.create(prefixes.concat(s));

			query.setPrefixMapping(query.getPrefixMapping());
			return query.serialize();
		}
		catch (QueryParseException ex) {
			return  "";
		}
	}

	public static String fixVariables(String s){
		return s.replaceAll(" [?|$][a-zA-Z0-9_]+"," ?variable ");
	}

	public static String[] getQueryAsTokens(String s){

		String query = clearQuery(s);
		query = replacePrefixes(query);
		query = fixVariables(query);
		query = query.
				replaceAll("[\\{\\}\\(\\)( )]+"," ").
				replaceAll("[\n]*","").
				replaceAll(" \\. "," ").
				replaceAll(" \\."," ").
				replaceAll("\\. "," ").
				toLowerCase();
		return  query.split(" ");
	}

    /**
     * Retrive query ready for excecution, is cleaned some dirty elements.
     * @param queryString String
     * @return String
     */
	public static String getQueryReadyForExecution(String queryString){
        try {
            queryString = java.net.URLDecoder.decode(queryString, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        queryString = queryString.substring(queryString.toLowerCase().indexOf("query=")+6);
        queryString = clearQuery(queryString);
        queryString = replacePrefixes(queryString);
        queryString = queryString.replaceAll("[\n]*","");
		return  queryString;
	}
	/**
	 * Retrive query ready for excecution, is cleaned some dirty elements.
	 * @param queryString String
	 * @return String
	 */
	public static String getQueryReadyForExecution(String queryString, boolean isCleaned){

		//If not cleaned the query process based on logs format....
		if (!isCleaned) {
			try {
				queryString = java.net.URLDecoder.decode(queryString, StandardCharsets.UTF_8.name());
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			queryString = queryString.substring(queryString.toLowerCase().indexOf("query=") + 6);
			queryString = clearQuery(queryString);
		}
		queryString = replacePrefixes(queryString);
		queryString = queryString.replaceAll("[\n]*","");
		return  queryString;
	}

	/**
     * Create a csv with array data passed as parameters.
     * @param list
     * @param filepath
     */
    public static void produceCsvArray(ArrayList<String[]> list, String filepath) {
        BufferedWriter br = null;
        try {
            br = new BufferedWriter(new FileWriter(filepath));

            StringBuilder sb = new StringBuilder();
            // Append strings from array
            for (String[] aList : list) {
                for (String element : aList) {
                    sb.append(element);
                    sb.append(";,;");
                }
                sb.append("\n");
            }

            br.write(sb.toString());
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	/**
	 * Create a csv with array data passed as parameters.
	 * @param list
	 * @param filepath
	 */
	public static void produceCsvArray2(ArrayList<ArrayList<String>> list, String filepath) {
		BufferedWriter br = null;
		try {
			br = new BufferedWriter(new FileWriter(filepath));

			StringBuilder sb = new StringBuilder();
			// Append strings from array
			for (ArrayList<String> aList : list) {
				for (String element : aList) {
					sb.append(element);
					sb.append(";,;");
				}
				sb.append("\n");
			}

			br.write(sb.toString());
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Create a csv with array data passed as parameters.
	 * @param list
	 * @param filepath
	 */
	public static void produceCsvArrayVectors(String[] headers, ArrayList<String[]> list, String filepath) {
		BufferedWriter br = null;
		try {
			br = new BufferedWriter(new FileWriter(filepath));

			StringBuilder sb = new StringBuilder();
			// Append strings from array
			for (String element : headers) {
				sb.append(element);
				sb.append(",");
			}

			sb.append("\n");
			for (String[] aList : list) {
				for (String element : aList) {
					if(element == null)
						element = String.valueOf(0);
					sb.append(element);
					sb.append(",");
				}
				sb.append("\n");
			}

			br.write(sb.toString());
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Retrieve the set of triples in a sparql query pattern
	 * @param s a sparql query
	 * @return Set<Triple>
	 */
	public static Set<Triple> retrieveTriples(String s) {
		
		final Set<Triple> allTriples = new HashSet<Triple>();
	
        // Parse
        final Query query = QueryFactory.create(s) ;
        
        Element e = query.getQueryPattern();
        
        // This will walk through all parts of the query
        ElementWalker.walk(e,
            // For each element...
            new ElementVisitorBase() {
                // ...when it's a block of triples...
                public void visit(ElementPathBlock el) {
                    // ...go through all the triples...
                    Iterator<TriplePath> triples = el.patternElts();
                    while (triples.hasNext()) {
                    	TriplePath t = triples.next();
                    	allTriples.add(t.asTriple());
                    }
                }
                public void visit(ElementTriplesBlock el) {
                    Iterator<Triple> triples = el.patternElts();
                    while (triples.hasNext()) {
                    	Triple t = triples.next();
                    	allTriples.add(t);
                    }
                }
                
                public void visit(ElementSubQuery el) {
                	
                	
                	Query sQuery = el.getQuery();
                	sQuery.setPrefixMapping(query.getPrefixMapping());
                	String sQueryStr = el.getQuery().serialize();

                	Set<Triple> triples = retrieveTriples(sQueryStr);
                	allTriples.addAll(triples);
                	
                }
            }
        );
        return allTriples;
		
	}
    /**
     * Retrieve predicates list from query string.
     * @param s a sparql query
     * @return Set<Triple>
     */
    public static ArrayList<String> retrievePredicatesInTriples(final String s) {

        final ArrayList<String> predicates = new ArrayList<String>();
		try {
			final Query query = QueryFactory.create(getQueryReadyForExecution(s,true));
			Element e = query.getQueryPattern();

			// This will walk through all parts of the query
			ElementWalker.walk(e,
					// For each element...
					new ElementVisitorBase() {
						// ...when it's a block of triples...
						public void visit(ElementPathBlock el) {
							// ...go through all the triples...
							Iterator<TriplePath> triples = el.patternElts();
							while (triples.hasNext()) {
								// ...and grab the subject
								//subjects.add(triples.next().getSubject());
								TriplePath t = triples.next();
								//System.out.println(t.toString());
								try {
									if(!t.getPredicate().isURI())
										continue;
									predicates.add(t.getPredicate().getURI());
								}
								catch (Exception ex){
									String[] a= new String[2];
									a[0]=s;
									a[1]= String.valueOf(countQueryInProcess);
									queriesError.add(a);
									ex.printStackTrace();
								}
							}
						}

						public void visit(ElementTriplesBlock el) {
							// ...go through all the triples...
							Iterator<Triple> triples = el.patternElts();
							while (triples.hasNext()) {
								// ...and grab the subject
								//subjects.add(triples.next().getSubject());
								Triple t = triples.next();
								//System.out.println(t.toString());
								predicates.add(t.getPredicate().getURI());
							}


						}

						public void visit(ElementSubQuery el) {

							Query sQuery = el.getQuery();
							sQuery.setPrefixMapping(query.getPrefixMapping());
							String sQueryStr = el.getQuery().serialize();

							ArrayList<String> predicats = retrievePredicatesInTriples(sQueryStr);
							predicates.addAll(predicats);
						}
					}
			);
			return predicates;
		}
		catch (Exception ex){
			return predicates;
		}


    }


	/**
	 * Replaces the ? with a URI to hel create an RDF graph with the sparql variables 
	 * @param symbol name of the variable
	 * @return refined String URI for the sparql variable 
	 */
	private static String refineSymbol(String symbol) {
		if(symbol.contains("?")) {
			symbol = symbol.replaceAll("\\?", SPARQL_VAR_NS);
		}
		return symbol;
	}
	/**
	 * Replaces the ? with a URI to hel create an RDF graph with the sparql variables 
	 * @param node node for the sparql variable
	 * @return refined String URI for the sparql variable
	 */
	private static String refineSymbol(Node node) {
		return refineSymbol(node.toString());
		
	}
	
	/**
	 * Builds an RDF graph from the sparql query pattern
	 * @param s sparql query string
	 * @return an RDF graph from the sparql query pattern
	 */
	
	public static Model buildQueryRDFGraph(String s) {
		 // create an empty model
		 Model model = ModelFactory.createDefaultModel();
		 
		 Set<Triple> triples = retrieveTriples(s);
		 
		 for(Triple t:triples) {
			 Node sub = t.getSubject();

			 
			 Resource rSub = null;
			 
			 if(sub.isVariable()) {
				
				String refineSubURI = refineSymbol(sub);
				
				rSub = model.createResource(refineSubURI);
			
			 } else {
				 rSub = model.asRDFNode(sub).asResource(); 
			 }
			 
			 Node pred = t.getPredicate();
			 
			 Property rPred = null;
			 
			 if(pred.isVariable()) {
				 
				 
				 String refinePredUri = refineSymbol(pred);
				 
				 rPred = model.createProperty(refinePredUri);
			 } else {
				 rPred = model.createProperty(pred.toString());
				 
			 }
			 
			 
			 Node obj = t.getObject();			 
			 RDFNode rObj = null;
			 
			 if(obj.isVariable()) {
				 
				 
				 String refineObjUri = refineSymbol(obj);
				 
				 rObj = model.createResource(refineObjUri);
			 } else {
				 rObj = model.asRDFNode(obj);
			 }
			 
			 //System.out.println(rSub.getClass());
			 //System.out.println(rPred.getClass());
			 //System.out.println(rObj.getClass());
			 
			 Statement st = model.createStatement(rSub, rPred, rObj);
			 //System.out.println(st);
			 model.add(st);
			 
		 }
		 return model;
		
	}
	/**
	 * Builds an RDF graph from the sparql query pattern
	 * @param s sparql query string
	 * @return an RDF graph from the sparql query pattern
	 */

	public static Model buildQueryRDFGraph(String s, Model model) {
		// create an empty model
		Set<Triple> triples = retrieveTriples(s);

		for(Triple t:triples) {
			Node sub = t.getSubject();


			Resource rSub = null;

			if(sub.isVariable()) {

				String refineSubURI = refineSymbol(sub);

				rSub = model.createResource(refineSubURI);

			} else {
				rSub = model.asRDFNode(sub).asResource();
			}

			Node pred = t.getPredicate();

			Property rPred = null;

			if(pred.isVariable()) {


				String refinePredUri = refineSymbol(pred);

				rPred = model.createProperty(refinePredUri);
			} else {
				rPred = model.createProperty(pred.toString());

			}


			Node obj = t.getObject();
			RDFNode rObj = null;

			if(obj.isVariable()) {


				String refineObjUri = refineSymbol(obj);

				rObj = model.createResource(refineObjUri);
			} else {
				rObj = model.asRDFNode(obj);
			}

			//System.out.println(rSub.getClass());
			//System.out.println(rPred.getClass());
			//System.out.println(rObj.getClass());

			Statement st = model.createStatement(rSub, rPred, rObj);
			//System.out.println(st);
			model.add(st);

		}
		return model;

	}
	
	/**
	 * Returns true if the Resource represented by the URI was a variable in the original sparql query 
	 * @param uri a RDF resource URI
	 * @return true or false
	 */
	private static boolean wasVariable(String uri) {
		if(uri.contains(SPARQL_VAR_NS)) return true;
		return false;
	}
	
	/**
	 * Builds a GXL graph suitable for the GMT library from a sparql query
	 * @param qr a sparql query
	 * @param graphId an id for the query, sometimes useful for indexing
	 * @return a representation of the GXL graph
	 * @throws Exception
	 */
	
	public static Graph buildSPARQL2GXLGraph(String qr, String graphId) throws Exception{
		
		Model model = buildQueryRDFGraph(qr);
		
		XMLElement gxl = RDF2GXL.getGXLRootElement(); 
		
		XMLElement graph = RDF2GXL.getGXLGraphElement(graphId);

		gxl.addChild(graph);
		


		// write it to standard out
		//model.write(System.out);
		
		
		ResIterator subIterator = model.listSubjects();
		while(subIterator.hasNext()) {
			Resource sub = subIterator.nextResource();
			XMLElement gxlSub = null;
			if(wasVariable(sub.toString())) {
				
				gxlSub = RDF2GXL.transformResourceURI2GXL(sub.toString(),"?");
				
			} else {
				gxlSub = RDF2GXL.transformResourceURI2GXL(sub.toString());
			}
			
			graph.addChild(gxlSub);
		}
		
		NodeIterator objIterator = model.listObjects();
		while(objIterator.hasNext()){
			RDFNode obj = objIterator.nextNode();
			XMLElement gxlObj = null;
			if(wasVariable(obj.toString())) {
				gxlObj = RDF2GXL.transformResourceURI2GXL(obj.toString(),"?");
			} else {
				//check in RDF spec whether literals with same values are considered as same RDF graph nodes.
				gxlObj = RDF2GXL.transformResourceURI2GXL(obj.toString());
			}
			graph.addChild(gxlObj);
		}
		
		
		StmtIterator stmtIterator = model.listStatements();
		
		while(stmtIterator.hasNext()){
			Statement s = stmtIterator.nextStatement();
			//System.out.println(s);
			String fromURI = s.getSubject().toString();
			String predicateURI = wasVariable(s.getPredicate().toString())?"?":s.getPredicate().toString();
			String toResource = s.getObject().toString();
			
			XMLElement edge = RDF2GXL.transformTriple2GXL(fromURI, predicateURI, toResource);
			graph.addChild(edge);
			
			
		}
		
		return RDF2GXL.parseGXL(gxl);
		      		
		
	}
	/**
	 * Builds a GXL graph suitable for the GMT library from a sparql query
	 * @param qr a sparql query
	 * @param graphId an id for the query, sometimes useful for indexing
	 * @return a representation of the GXL graph
	 * @throws Exception
	 */

	public static Graph buildSPARQL2GXLGraph(String qr, String graphId, Model model) throws Exception{

		model = buildQueryRDFGraph(qr,model);

		XMLElement gxl = RDF2GXL.getGXLRootElement();

		XMLElement graph = RDF2GXL.getGXLGraphElement(graphId);

		gxl.addChild(graph);



		// write it to standard out
		//model.write(System.out);


		ResIterator subIterator = model.listSubjects();
		while(subIterator.hasNext()) {
			Resource sub = subIterator.nextResource();
			XMLElement gxlSub = null;
			if(wasVariable(sub.toString())) {

				gxlSub = RDF2GXL.transformResourceURI2GXL(sub.toString(),"?");

			} else {
				gxlSub = RDF2GXL.transformResourceURI2GXL(sub.toString());
			}

			graph.addChild(gxlSub);
		}

		NodeIterator objIterator = model.listObjects();
		while(objIterator.hasNext()){
			RDFNode obj = objIterator.nextNode();
			XMLElement gxlObj = null;
			if(wasVariable(obj.toString())) {
				gxlObj = RDF2GXL.transformResourceURI2GXL(obj.toString(),"?");
			} else {
				//check in RDF spec whether literals with same values are considered as same RDF graph nodes.
				gxlObj = RDF2GXL.transformResourceURI2GXL(obj.toString());
			}
			graph.addChild(gxlObj);
		}


		StmtIterator stmtIterator = model.listStatements();

		while(stmtIterator.hasNext()){
			Statement s = stmtIterator.nextStatement();
			//System.out.println(s);
			String fromURI = s.getSubject().toString();
			String predicateURI = wasVariable(s.getPredicate().toString())?"?":s.getPredicate().toString();
			String toResource = s.getObject().toString();

			XMLElement edge = RDF2GXL.transformTriple2GXL(fromURI, predicateURI, toResource);
			graph.addChild(edge);


		}

		return RDF2GXL.parseGXL(gxl);


	}
    public static ArrayList<String> getQueries(String trainingQueryFile,ArrayList<Integer> not_include){
        Model model = getNamespaces();
        Map<String, String> pref = model.getNsPrefixMap();
        Object[] keys = pref.keySet().toArray();
        boolean header = true;
        ArrayList<String> queries = new ArrayList<String>();
        int index = 0;
        try {
            InputStreamReader csv = new InputStreamReader(new FileInputStream(trainingQueryFile));
            CSVReader csvReader = new CSVReader (csv);
            String[] record;
            while ((record = csvReader.readNext()) != null) {
                if (header){
                    header = false;
                    continue;
                }
                if(not_include.contains(index)){
                    index++;
                    continue;
                }
                else
                {
                    index++;
                }
                String query = record[1].replaceAll("^\"|\"$", "");
                String prefixesStr = "";
                for (int i = 0; i < model.getNsPrefixMap().size(); i++) {

                    int a = query.indexOf(String.valueOf(keys[i]+":"));
                    if (a != -1 ) {
                        prefixesStr = prefixesStr.concat("PREFIX ").concat(String.valueOf(keys[i])).concat(": ").concat("<").concat(pref.get(String.valueOf(keys[i]))).concat("> \n");
                    }
                }
                query  = prefixesStr.concat(" " +query);
                queries.add(query);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return  queries;
    }

public static void main(String[] args) throws Exception {
	String input = "";
	String output = "";
	String prefixes = "";
	int cores = 1;
		try {
			input = args[0];
			output = args[1];
			prefixes = args[2];
			cores = Integer.parseInt(args[3]);
		}
		catch (Exception ex) {
			System.out.println("args[0] : Input csv \n args[1] : Output path \n args[2] : prefixes path\n args[3] : Cores to use\n");
			return;
		}
	System.out.println(args[0]);
	Model model = getNamespaces(prefixes);
	Map<String, String> pref = model.getNsPrefixMap();
	Object[] keys = pref.keySet().toArray();
	boolean header = true;
	ArrayList<String[]> queries = new ArrayList<String[]>();

	try {
		InputStreamReader csv = new InputStreamReader(new FileInputStream(input));
		CSVReader csvReader = new CSVReader (csv);
		String[] record;
		while ((record = csvReader.readNext()) != null) {
			if (header){
				header = false;
				continue;
			}

			String query = record[1].replaceAll("^\"|\"$", "");
			String prefixesStr = "";
			for (int i = 0; i < model.getNsPrefixMap().size(); i++) {

				int a = query.indexOf(String.valueOf(keys[i]+":"));
				if (a != -1 ){
					prefixesStr = prefixesStr.concat("PREFIX ").concat(String.valueOf(keys[i])).concat(": ").concat("<").concat(pref.get(String.valueOf(keys[i]))).concat("> \n");
				}
			}
			query  = prefixesStr.concat(" " +query);
			String[] q = new String[3];
			q[0] = record[0];
			q[1] = query;
			q[2] = record[7];
			queries.add(q);
		}
	}
	catch (IOException e) {
		e.printStackTrace();
	}

	ForkJoinPool pool = new ForkJoinPool();

	GraphBuildAction task = new GraphBuildAction(queries,0,queries.size());
	ArrayList<Map> grafos = pool.invoke(task);

	EditDistanceAction task2 = new EditDistanceAction(grafos,output,cores,0,grafos.size(),false);
	pool.invoke(task2);

	String a = "";
}
}