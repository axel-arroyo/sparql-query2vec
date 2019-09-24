package semanticweb;

import semanticweb.sparql.SparqlUtils;
import util.Graph;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class EditDistanceAction extends RecursiveTask {

    final int THRESHOLD = 2;
    ArrayList<String[]> queries;
    int indexStart, indexLast;
    StringBuilder procesado;
    boolean recursive;
    BufferedWriter br;
    BufferedWriter br2;

    public EditDistanceAction(ArrayList<String[]> queries,int indexStart,int indexLast, boolean recursive) {
        this.queries = queries;
        this.indexStart = indexStart;
        this.indexLast = indexLast;
        this.procesado = new StringBuilder();
        this.recursive = recursive;
    }

    private void computeSubMatrix(int indexStart,int indexLast) {
        ArrayList<double[]> distances = new ArrayList<>();
//        BufferedWriter br = new BufferedWriter(new FileWriter("/home/daniel/Documentos/ML/rhassan/graph-edit-distance/data/config/100/hungarian_distance"));
        StringBuilder sb = new StringBuilder();
        RDFGraphMatching matcher = new RDFGraphMatching();
        String fail_rows = "";
        StringBuilder failed_row_column = new StringBuilder();
        for (int i = indexStart; i < indexLast; i++) {

            try {
                Graph Gi = SparqlUtils.buildSPARQL2GXLGraph(queries.get(i)[1],  "row_"+String.valueOf(i));
                for (int j = 0; j < queries.size(); j++) {
                    double dist = -1;
                    if (i == j){
                        sb.append(0.0);
                        sb.append(",");
                        continue;
                    }
                    try{
                        Graph Gj = SparqlUtils.buildSPARQL2GXLGraph(queries.get(j)[1],  "col_"+String.valueOf(j));
                        dist =  matcher.distanceBipartiteHungarian(Gi, Gj);
                    }
                    catch (Exception ex){
                       failed_row_column.append(queries.get(j)[0]);
                       failed_row_column.append("\n");
//                       continue;
                    }
                    sb.append(dist);
                    sb.append(",");
                }
                sb.deleteCharAt(sb.length()-1);
                sb.append("\n");
                System.out.println(i);
            }
            catch (Exception ex){
                failed_row_column.append(queries.get(i)[0]);
                failed_row_column.append("\n");
                sb.append("failrow");
                sb.append("\n");
                continue;
            }
            System.out.println(fail_rows);
        }
        try {
            br = new BufferedWriter(new FileWriter("/home/daniel/Documentos/ML/rhassan/graph-edit-distance/data/config/100/hungarian_distance"+String.format("%06d", indexStart)+"_"+String.format("%06d", indexLast)+".csv"));
            br.write(sb.toString());
            br2 = new BufferedWriter(new FileWriter("/home/daniel/Documentos/ML/rhassan/graph-edit-distance/data/config/100/errors"+String.format("%06d", indexStart)+"_"+String.format("%06d", indexLast)+".txt"));
            br2.write(failed_row_column.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Object compute() {

        if (indexLast - indexStart < 5) {
            computeSubMatrix(indexStart, indexLast);
        }
        else {
            int size = indexLast - indexStart;
            int cantidadByMicro = size/4;
            int index1Start = indexStart;
            int index1Last = index1Start + cantidadByMicro;

            int index2Start = index1Last;
            int index2Last = index2Start+cantidadByMicro;

            int index3Start = index2Last;
            int index3Last = index3Start+cantidadByMicro;

            int index4Start = index3Last;
            int index4Last = indexLast;

            EditDistanceAction t1 = new EditDistanceAction(queries,index1Start,cantidadByMicro,true);
            EditDistanceAction t2 = new EditDistanceAction(queries,index2Start,index2Last,true);
            EditDistanceAction t3 = new EditDistanceAction(queries,index3Start,index3Last,true);
            EditDistanceAction t4 = new EditDistanceAction(queries,index4Start,index4Last,true);
            ForkJoinTask.invokeAll(t1,t2,t3,t4);

//            t1.fork();
//            t2.fork();
//            t3.fork();
//            t4.fork();
//            t1.join();
//            t2.join();
//            t3.join();
//            t4.join();
        }


        return null;
    }
}
//    ForkJoinPool pool = new ForkJoinPool();
//    EditDistanceAction task = new EditDistanceAction(queries,index1Start,cantidadByMicro);
//        pool.invoke(task);
