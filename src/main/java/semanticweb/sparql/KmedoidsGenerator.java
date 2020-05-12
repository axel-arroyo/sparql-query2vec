package semanticweb.sparql;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class KmedoidsGenerator {

    public static HashMap<String, Object> getArrayFromCsvFile(String url, String delimiterCol, int idColumn, int execTimeColumn) {
        BufferedReader csvReader;
        String row;
        double[][] arrayList = new double[0][];
        ArrayList<HashMap<String,String>> idTimeVals = new ArrayList<>();
        boolean first = true;
        try {
            csvReader = new BufferedReader(new FileReader(url));
            int i = 0;
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(delimiterCol);
                int len = data.length - 2;
                if(first){
                    arrayList = new double[len][len];
                    first = false;
                }
                HashMap<String,String> idTime = new HashMap<>();
                int indexReal = 0;
                for (int j = 0; j < len; j++) {
                    if(j == idColumn){
                        idTime.put("id", data[j]);
                    }
                    else if(j==execTimeColumn){
                        idTime.put("time", data[j]);
                    }
                    else{
                        arrayList[i][indexReal] = Double.parseDouble(data[j]);
                        indexReal++;
                    }
                }
                idTimeVals.add(idTime);
                i++;
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        HashMap<String,Object> result = new HashMap();
        result.put("idTime", idTimeVals);
        result.put("arrayData", arrayList);
        return result;
    }

    private RealMatrix getDataAsIndex(RealMatrix distance_matrix) {
        double[][] doubles = new double[distance_matrix.getRowDimension()][distance_matrix.getRowDimension()];
        for (int i = 0; i < distance_matrix.getRowDimension(); i++) {
            for (int j = 0; j < distance_matrix.getRowDimension(); j++) {
                doubles[i][j] = i;
            }
        }
        return  new Array2DRowRealMatrix(doubles);
    }

    public void proccessQueries(String[] args,int k, String input_delimiter, char output_delimiter, int idColumn, int execTimeColumn){
        System.out.println("Inside");
        String input = "";
        String output = "";
        String indexesFile = "";
        try {
            input = args[0];
        }
        catch (Exception ex) {
            System.out.println("You need to specify the input URL as the first parameter");
            return;
        }
        try {
            output = args[1];
        }
        catch (Exception ex) {
            System.out.println("You need to specify the output URL as the second parameter");
            return;
        }
        HashMap<String,Object> data = getArrayFromCsvFile(input, input_delimiter,idColumn,execTimeColumn);
        ArrayList<HashMap<String,String>> idTime = (ArrayList<HashMap<String, String>>) data.get("idTime");
        double[][] distances = (double[][]) data.get("arrayData");
//        ArrayList<String[]> ids_time = SparqlUtils.getArrayFromCsvFile(indexesFile);

        double[][] doubles = new double[distances.length][distances.length];
        for (int i = 0; i < distances.length; i++) {
            for (int j = 0; j < distances.length; j++) {
                doubles[i][j] = i;
            }
        }
//        ArrayList<String> indexes = new ArrayList<>();
        RealMatrix rm = MatrixUtils.createRealMatrix(distances);

        KmedoidsED km = new KmedoidsED(MatrixUtils.createRealMatrix(doubles),rm,k);
        km.fit();
        final int[] results = km.getLabels();

        System.out.println(results.toString());
        List<Integer> centroidList = new ArrayList<>();

        ArrayList<double[]> centroids = km.getCentroids();
        for (int i = 0; i < km.getCentroids().size(); i++) {
            centroidList.add((int) centroids.get(i)[0]);
        }
        StringBuilder sb = new StringBuilder();

        BufferedWriter br;
        try {
            br = new BufferedWriter(new FileWriter(output));
            //Write header
            sb.append("id");
            sb.append(output_delimiter);
            for (int i = 0; i < k; i++) {
                sb.append("pcs").append(k);
                sb.append(output_delimiter);
            }
            sb.append("time");
            sb.append("\n");
            for (int i = 0; i < distances.length; i++) {
                sb.append(idTime.get(i).get("id"));
                sb.append(output_delimiter);
                for (int j = 0; j < centroidList.size(); j++) {

                    int currentCentroid = centroidList.get(j);
                    double distance = distances[i][currentCentroid];
                    double similarity = 1 / (1+ distance);
                    sb.append(similarity);
                    sb.append(output_delimiter);
                }
                sb.append(idTime.get(i).get("time"));
                sb.append("\n");
            }
            br.write(sb.toString());
            br.close();
            System.out.println("Medoids vectors computed, output writed in :" + output);
        }
        catch (Exception ex){
            System.out.println("Something was wrong in the writing process of the output");

        }
    }

    public static int[] makeSequence(int end) {
        int[] ret = new int[end+1];
        for (int i=0; i<=end; i++) {
            ret[i] = i;
        }
        return ret;
    }
}
