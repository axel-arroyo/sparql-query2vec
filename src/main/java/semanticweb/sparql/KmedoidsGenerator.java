package semanticweb.sparql;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class KmedoidsGenerator {

    public static double[][] getArrayFromCsvFile(String url, String delimiterCol, String delimiterRow) {
        BufferedReader csvReader;
        String row;
        double[][] arrayList = new double[0][];
        try {
            csvReader = new BufferedReader(new FileReader(url));
            row = csvReader.readLine();
            String[] first_split = row.split(delimiterCol);
            int len = first_split.length;
            arrayList = new double[len][len];
            for (int i = 0; i < len; i++) {
                arrayList[0][i] = Double.parseDouble(first_split[i]);
            }
            //Siguientes líneas.
            for (int i = 1; i < len; i++) {
                String rowcurrent = csvReader.readLine();
                String[] split = rowcurrent.split(delimiterCol);
                for (int j = 0; j < len; j++) {
                    arrayList[i][j] = Double.parseDouble(split[j]);
                }
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return arrayList;
    }

    public static double[][] getArrayFromCsvFile(String url) {

        String delimiterCol = ",";
        String delimiterRow = "\n";
        return  getArrayFromCsvFile(url, delimiterCol, delimiterRow);
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
    public static void main(String[] args) {
        System.out.println("Inside");
        String input = "";
        String output = "";
        String indexesFile = "";
        int k = 2;
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
        try {
            indexesFile = args[2];
        }
        catch (Exception ex) {
            System.out.println("You need to specify the output URL as the third parameter");
            return;
        }
        try {
            if(args.length ==4)
                k = Integer.parseInt(args[3]);
        }
        catch (Exception ex) {
            System.out.println("You need to specify the k value as the fourth parameter");
            return;
        }
//        int columnIndex = Integer.parseInt(args[2]);
        Random random = new Random();


        double[][] distances = getArrayFromCsvFile(input);
        ArrayList<String[]> ids_time = SparqlUtils.getArrayFromCsvFile(indexesFile);

        double[][] doubles = new double[distances.length][distances.length];
        for (int i = 0; i < distances.length; i++) {
            for (int j = 0; j < distances.length; j++) {
                doubles[i][j] = i;
            }
        }
        ArrayList<String> indexes = new ArrayList<>();
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
            sb.append(",");
            sb.append("time");
            sb.append(",");
            for (int i = 0; i < k; i++) {
                sb.append("pcs".concat(String.valueOf(k)));
                sb.append(",");
            }
            sb.append("\n");
            for (int i = 0; i < distances.length; i++) {
                if(ids_time.size() > 0){
                    for (int j = 0; j < ids_time.get(i).length; j++) {
                        sb.append(ids_time.get(i)[j]);
                        sb.append(",");
                    }
                }
                for (int j = 0; j < centroidList.size(); j++) {

                    int currentCentroid = centroidList.get(j);
                    double distance = distances[i][currentCentroid];
                    double similarity = 1 / (1+ distance);
                    sb.append(similarity);
                    sb.append(",");
                }
                sb.append("\n");
            }
            br.write(sb.toString());
            br.close();
            System.out.println("Medoids vectors computed, output writed in :" +output);
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
