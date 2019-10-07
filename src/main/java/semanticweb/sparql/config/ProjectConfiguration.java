package semanticweb.sparql.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ProjectConfiguration {
	public static String CONFIG_FILE = "/home/daniel/Documentos/ML/rhassan/graph-edit-distance/data/config/config-100.prop";
	
	private String configFile;
	private String distanceMatrixFile = null;
	private String trainingQueryExecutionTimesFile = null;
	private String trainingQueryFile = null;

	private String trainingARFFFile;

	private int numberOfClusters = 2;
	private int xMeansMaxK = 2;
	private Properties prop;

	private String trainingClassFeatureKmeansFile;

	private String trainingTimeClassKmeansFile;

	private String trainingClassFeatureXmeansFile;

	private String trainingTimeClassXmeansFile;

	private String trainingAlgebraFeaturesFile;

	private String trainingSimilarityVectorfeatureFile;
	
	private String trainingQueryExecutionTimesPredictedFile;
	
	private String  trainingNumberOfRecordsFile;

	public ProjectConfiguration() throws IOException{
		this(ProjectConfiguration.CONFIG_FILE);

	}
	
	public ProjectConfiguration(String cFile) throws IOException {
		this.configFile = cFile;
		prop = new Properties();
		prop.load(new FileInputStream(cFile));
		loadConfig();
	}
	
	public void loadConfig() {
		
		distanceMatrixFile = prop.getProperty("TrainingDistanceHungarianMatrix");
	
		numberOfClusters = Integer.parseInt(prop.getProperty("K"));

		trainingQueryFile = prop.getProperty("TrainingQuery");

		trainingSimilarityVectorfeatureFile = prop.getProperty("TrainingSimilarityVectorfeature");

		trainingClassFeatureKmeansFile = prop.getProperty("TrainingClassVectorfeatureKmeans");

		trainingQueryExecutionTimesFile = prop.getProperty("TrainingQueryExecutionTimes");

		trainingTimeClassKmeansFile = prop.getProperty("TrainingTimeClassKmeans");

		trainingClassFeatureXmeansFile = prop.getProperty("TrainingClassVectorfeatureXmeans");

		trainingTimeClassXmeansFile = prop.getProperty("TrainingTimeClassXmeans");

		trainingAlgebraFeaturesFile = prop.getProperty("TrainingAlgebraFeatures");

		trainingQueryExecutionTimesPredictedFile = prop.getProperty("TrainingQueryExecutionTimesPredicted");

		trainingARFFFile = prop.getProperty("TrainingARFFFile");
		
		trainingNumberOfRecordsFile = prop.getProperty("TrainingNumberOfRecords");
	}
	
	public String getTrainingNumberOfRecordsFile() {
		return trainingNumberOfRecordsFile;
	}

	public String getTrainingSimilarityVectorfeatureFile() {
		return trainingSimilarityVectorfeatureFile;
	}

	public String getTrainingAlgebraFeaturesFile() {
		return trainingAlgebraFeaturesFile;
	}

	public String getTrainingQueryExecutionTimesFile() {
		return trainingQueryExecutionTimesFile;
	}

	public String getDistanceMatrixFile() {
		return distanceMatrixFile;
	}
	
	public int getNumberOfClusters() {
		return numberOfClusters;
	}
	
	public String getTrainingQueryFile() {
		return trainingQueryFile;
	}

	public String getTrainingClassFeatureKmeansFile() {
		return trainingClassFeatureKmeansFile;
	}

	public String getTrainingTimeClassKmeansFile() {
		return trainingTimeClassKmeansFile;
	}

	public String getTrainingQueryExecutionTimesPredictedFile() {
		return trainingQueryExecutionTimesPredictedFile;
	}

	public String getTrainingARFFFile() {
		return trainingARFFFile;
	}

	public int getxMeansMaxK() {
		return xMeansMaxK;
	}
	
	public String getTrainingTimeClassXmeansFile() {
		return trainingTimeClassXmeansFile;
	}

	public String getTrainingClassFeatureXmeansFile() {
		return trainingClassFeatureXmeansFile;
	}

	public static String getAlgebraFeatureHeader() {
		return "triple,bgp,join,leftjoin,union,filter,graph,extend,minus,path*,pathN*,path+,pathN+,notoneof,tolist,order,project,distinct,reduced,multi,top,group,assign,sequence,slice,treesize";
	}
	
	public static String getPatternClusterSimVecFeatureHeader(int dim) {
		String out = "";
		for(int i=0;i<dim;i++) {
			if(i!=0) {
				out += ",";
			}
			out += ("pcs"+i);
		}
		return out;
	}

	public static String getTimeClusterBinaryVecFeatureHeader(int dim) {
		String out = "";
		for(int i=0;i<dim;i++) {
			if(i!=0) {
				out += ",";
			}
			out += ("tcb"+i);
		}
		return out;
	}

	public static String getTimClusterLabelHeader() {
		return "time_class";
	}
	
	public static String getExecutionTimeHeader() {
		return "ex_time";
	}
}
