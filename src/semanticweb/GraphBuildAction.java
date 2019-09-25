package semanticweb;

import semanticweb.sparql.SparqlUtils;
import util.Graph;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class GraphBuildAction extends RecursiveTask<ArrayList<Map>> {

    final int THRESHOLD = 2;
    ArrayList<String[]> queries;
    int indexStart, indexLast;
    StringBuilder procesado;
//    boolean recursive;
    int cores;
    ArrayList<Map> graphs;


    public GraphBuildAction(ArrayList<String[]> queries, int indexStart, int indexLast) {
        this.queries = queries;
        this.indexStart = indexStart;
        this.indexLast = indexLast;
        this.procesado = new StringBuilder();
//        this.recursive = recursive;
//        this.cores = cores;
//        this.graphs = graphs; //ArrayList<Map> graphs,
    }

    private ArrayList<Map> computeGraphCreation(int indexStart,int indexLast) {
        StringBuilder sb = new StringBuilder();
        ArrayList<String> fail_rows = new ArrayList<>();
        ArrayList<Map> graphs = new ArrayList<Map>();
        for (int i = indexStart; i < indexLast; i++) {
            try {
                Graph Gi = SparqlUtils.buildSPARQL2GXLGraph(queries.get(i)[1],  "row_"+String.valueOf(i));
                Map mGi = new HashMap();
                mGi.put("id",queries.get(i)[0]);
                mGi.put("time",queries.get(i)[2]);
                mGi.put("graph", Gi);
                graphs.add(mGi);
                }
            catch (Exception ex){
                fail_rows.add(queries.get(i)[0]);
            }
            System.out.println(fail_rows);
        }
        return graphs;
    }

    @Override
    protected ArrayList<Map> compute() {
        if (indexLast - indexStart < 50) {
            System.out.println("Inicial core "+" preocesando : "+indexStart+ " - "+indexLast);
            return computeGraphCreation(indexStart, indexLast);
        }
        else {
            int cores = Runtime.getRuntime().availableProcessors();
            System.out.println("Cantidad de cores"+ cores);
//            int size = indexLast - indexStart;
//            int cantidadByMicro = size/cores;
//            int index1Start = indexStart;
//            int index1Last = index1Start + cantidadByMicro;
            GraphBuildAction left = new GraphBuildAction(queries, indexStart, (indexLast+indexStart)/2);
            GraphBuildAction right = new GraphBuildAction(queries, (indexLast+indexStart)/2, indexLast);
            left.fork();
            ArrayList<Map> rightAns = right.compute();
            ArrayList<Map> leftAns = left.join();
            leftAns.addAll(rightAns);
            return leftAns;
        }
    }
}