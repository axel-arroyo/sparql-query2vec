package semanticweb;

import semanticweb.sparql.SparqlUtils;
import util.Graph;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RecursiveTask;

public class GraphBuildAction extends RecursiveTask<ArrayList<Map>> {

    private ArrayList<String[]> queries;
    private int indexStart, indexLast;

    public GraphBuildAction(ArrayList<String[]> queries, int indexStart, int indexLast) {
        this.queries = queries;
        this.indexStart = indexStart;
        this.indexLast = indexLast;
    }

    private ArrayList<Map> computeGraphCreation(int indexStart,int indexLast) {
        ArrayList<String> fail_rows = new ArrayList<>();
        ArrayList<Map> graphs = new ArrayList<>();
        for (int i = indexStart; i < indexLast; i++) {
            try {
                Graph Gi = SparqlUtils.buildSPARQL2GXLGraph(queries.get(i)[1],  "row_"+ i);
                HashMap mGi = new HashMap();
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
            return computeGraphCreation(indexStart, indexLast);
        }
        else {
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