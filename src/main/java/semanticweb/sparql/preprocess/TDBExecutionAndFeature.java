package semanticweb.sparql.preprocess;

import com.google.common.base.Stopwatch;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.tdb.TDBFactory;
import semanticweb.sparql.SparqlUtils;
import semanticweb.sparql.config.ProjectConfiguration;
import semanticweb.sparql.utils.DBPediaUtils;
import semanticweb.sparql.utils.GeneralUtils;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TDBExecutionAndFeature {
	
	private List<String[]> trainingQueries;
	private List<String> validationQueries;
	private List<String> testQueries;
	private Properties prop;
	private Model model;
	private boolean directTDB = false;
	
	public TDBExecutionAndFeature() throws IOException {
		prop = new Properties();
		prop.load(new FileInputStream(ProjectConfiguration.CONFIG_FILE));
		
		//loadAllQueries();
		
	}
	public TDBExecutionAndFeature(String config_file) throws IOException {
		prop = new Properties();
		prop.load(new FileInputStream(config_file));
	}

	public void loadTrainingQueries() throws IOException {
		String trainingQueryFile = prop.getProperty("TrainingQuery");
		trainingQueries = AlgebraFeatureExtractor.getQueries(trainingQueryFile,prop.getProperty("Namespaces"), new ArrayList<>());
	}
	
	public ResultSet queryTDB(String qStr) {
		String q = DBPediaUtils.refineForDBPedia(qStr);
		Query query = QueryFactory.create(q);
		QueryExecution qexec = directTDB ? QueryExecutionFactory.create(query, model): QueryExecutionFactory.sparqlService(prop.getProperty("Endpoint"), query);
		ResultSet results = qexec.execSelect();
		return results;

	}
	
	public void initTDB() {
		String assemblerFile = prop.getProperty("TDBAssembly");
		System.out.println(assemblerFile);
		Dataset dataset = TDBFactory.assembleDataset(assemblerFile) ;
		//model = TDBFactory.assembleModel(assemblerFile) ;
		model = dataset.getDefaultModel();
		
		
	}
	
	public void closeModel() {
		model.close();
	}
	
	private void executeQueries(List<String[]> queries, String timeOutFile, String recCountOutFile) throws IOException {
		PrintStream psTime = new PrintStream(timeOutFile);
		PrintStream psRec = new PrintStream(recCountOutFile);
		
		Stopwatch watch = new Stopwatch();
		watch.start();
		
		
		int count = 0;
		for(String[] q:queries) {
			String qStr = DBPediaUtils.getQueryForDBpedia(q[1]);
			if(count%1000==0) {
				System.out.println(count+" queries processed");
			}
			
			watch.reset();
			watch.start();
			ResultSet results = queryTDB(qStr);
			psTime.println(watch.elapsed(TimeUnit.MILLISECONDS));
			count++;
		}
		psTime.close();
		psRec.close();
	}
	
	public void executeTrainingQueries() throws IOException {
		System.out.println("Processing training queries");
		
		executeQueries(trainingQueries,prop.getProperty("TDBTrainingExecutionTime"),prop.getProperty("TDBTrainingRecordCount"));
		
	}
	
	public void executeDirectTDB() {
		
		directTDB = true;
		initTDB();
		queryTDB("select * where {<http://dbpedia.org/resource/Berlin> ?p ?o}");
		
		try {
			executeTrainingQueries();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		closeModel();
		
	}
	
	public void fusekiTDB() {
		directTDB = false;
		queryTDB("select * where {<http://dbpedia.org/resource/Berlin> ?p ?o}");
		try {
			executeTrainingQueries();
		}
		catch(Exception e) {
			e.printStackTrace();
		}		
	}

	public void executeRandomlySelectedQueries() throws IOException {
		PrintStream psTime = new PrintStream(prop.getProperty("TDBTrainingExecutionTime"));
		PrintStream psRec = new PrintStream(prop.getProperty("TDBTrainingRecordCount"));
		PrintStream psQuery = new PrintStream(prop.getProperty("TrainingQuery"));
		
		
		
		
		Stopwatch watch = new Stopwatch();
		watch.start();
		
		
		int count = 0;
		FileInputStream fis = new FileInputStream(prop.getProperty("QueryFile"));

		Scanner in = new Scanner(fis);
		
		int totalQuery = Integer.parseInt(prop.getProperty("TotalQuery"));
		
		int dataSplit = (int) (totalQuery * 0.6);
		int validationSplit = (int) (totalQuery * 0.2);

		while(in.hasNext()) {
			if(count>=totalQuery) break;
			
			if(count == dataSplit) {
				System.out.println("initilizing validation files");
				psTime.close();
				psRec.close();
				psQuery.close();
				
				psQuery = new PrintStream(prop.getProperty("ValidationQuery"));
				
				psTime = new PrintStream(prop.getProperty("TDBValidationExecutionTime"));
				psRec = new PrintStream(prop.getProperty("TDBValidationRecordCount"));				
				
			} else if(count== (dataSplit+validationSplit)) {
				System.out.println("initilizing test files");
				psTime.close();
				psRec.close();
				psQuery.close();
				
				psQuery = new PrintStream(prop.getProperty("TestQuery"));
				
				psTime = new PrintStream(prop.getProperty("TDBTestExecutionTime"));
				psRec = new PrintStream(prop.getProperty("TDBTestRecordCount"));					
			}

			String line = in.nextLine();
			String[] ss = line.split(" ");
			String q = ss[6].substring(1, ss[6].length()-1);
			String qStr = DBPediaUtils.getQueryForDBpedia(q);
			watch.reset();
			watch.start();
			try {
				
				ResultSet results = queryTDB(qStr);
				long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);

				ResultSetRewindable rsrw = ResultSetFactory.copyResults(results);
			    int numberOfResults = rsrw.size();
			    if(numberOfResults>0) {
					psTime.println(elapsed);
					psQuery.println(q);
				    psRec.println(numberOfResults);
					count++;
			    }
				
				if(count%1000==0) {
					System.out.println(count+" queries processed");
				}			    
			} catch(Exception e) {
				//do nothing
			}

		}
		
		
		psTime.close();
		psRec.close();
		psQuery.close();
		
		
		fis.close();

	}
	
	public void fusekiTDBRandomlySelectedQueries() throws IOException {
		directTDB = false;
		executeRandomlySelectedQueries();
	}
	
	private void generateAlgebraFeatureDataset() throws IOException {
		loadTrainingQueries();
		String[] header = ProjectConfiguration.getAlgebraFeatureHeader(prop.getProperty("FeaturesList"));
		generateAlgebraFeatures(prop.getProperty("TrainingAlgebraFeatures"), header, trainingQueries);
		System.out.println("Precess finished.");
	}
	
	private void generateAlgebraFeatures(String output, String[] header, List<String[]> queries) throws IOException {
		AlgebraFeatureExtractor fe = new AlgebraFeatureExtractor(header);
		BufferedWriter br = null;
		StringBuilder sb = new StringBuilder();
		try {
			br = new BufferedWriter(new FileWriter(output));
			{
				sb.append("query_id");
				sb.append(",");
				int i = 0;
				while (i < header.length) {
					sb.append(header[i]);
					sb.append(",");
					i++;
				}
				sb.append("\n");
			}
			for(String[] q:queries) {
				try {
					double[] features = fe.extractFeatures(q[1]);
					//Print Id of query.
					sb.append(q[0]);
					sb.append(",");
					for (double feature : features) {
						sb.append(feature);
						sb.append(",");
					}
				}
				catch (Exception ex){
					ex.printStackTrace();
				}
				sb.append("\n");
			}
			br.write(sb.toString());
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("Inside algebra features generation");
		String config_file = "";
		try{
			config_file = args[0];
		}
		catch (Exception ex) {
			System.out.println("You need to specify a config file url as first parameter");
			return;
		}
		Stopwatch watch = new Stopwatch();
		watch.start();		
		TDBExecutionAndFeature wrapper = new TDBExecutionAndFeature(config_file);

		wrapper.generateAlgebraFeatureDataset();
		watch.stop();
		System.out.println("Total time for algebra query extraction: "+watch.elapsed(TimeUnit.MILLISECONDS)+" ms");
		
	}
	
}
