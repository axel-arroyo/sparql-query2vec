package semanticweb;
import util.Graph;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class EditDistanceAction extends RecursiveTask {

    private ArrayList<Map> queries;
    private int indexStart, indexLast;
    private boolean recursive;
    private int cores;
    private String output;

    public EditDistanceAction(ArrayList<Map> queries, String output, int cores, int indexStart, int indexLast, boolean recursive) {
        this.queries = queries;
        this.indexStart = indexStart;
        this.indexLast = indexLast;
        this.recursive = recursive;
        this.output = output;
        this.cores = cores;
    }

    private void computeSubMatrix(int indexStart,int indexLast) {
        StringBuilder sb = new StringBuilder();
        RDFGraphMatching matcher = new RDFGraphMatching();
        StringBuilder failed_row_column = new StringBuilder();
        for (int i = indexStart; i < indexLast; i++) {

            try {
                Graph Gi = (Graph) queries.get(i).get("graph");
                for (int j = 0; j < queries.size(); j++) {
                    if(j==0){
                        sb.append(queries.get(i).get("id"));
                        sb.append(",");
                        sb.append(queries.get(i).get("time"));
                        sb.append(",");
                    }
                    double dist = -1;
                    if (i == j){
                        sb.append(0.0);
                        sb.append(",");
                        continue;
                    }
                    try{
                        Graph Gj = (Graph) queries.get(j).get("graph");
                        dist =  matcher.distanceBipartiteHungarian(Gi, Gj);
                    }
                    catch (Exception ex){
                       failed_row_column.append(queries.get(j).get("id"));
                       failed_row_column.append("\n");
                    }
                    sb.append(dist);
                    sb.append(",");
                }
                sb.deleteCharAt(sb.length()-1);
                sb.append("\n");
            }
            catch (Exception ex){
                failed_row_column.append(queries.get(i).get("id"));
                failed_row_column.append("\n");
                sb.append("failrow");
                sb.append("\n");
            }
        }
        try {
            BufferedWriter br = new BufferedWriter(new FileWriter(output + "hungarian_distance" + String.format("%06d", indexStart) + "_" + String.format("%06d", indexLast) + ".csv"));
            br.write(sb.toString());
            BufferedWriter br2 = new BufferedWriter(new FileWriter(output + "errors" + String.format("%06d", indexStart) + "_" + String.format("%06d", indexLast) + ".txt"));
            br2.write(failed_row_column.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Object compute() {
        if (recursive) {
            computeSubMatrix(indexStart,indexLast);
            System.out.println("Processed range: "+indexStart+ " - "+indexLast);
        }
        else {
            int size = indexLast - indexStart;
            int cantidadByMicro = size/cores;
            int index1Start = indexStart;
            int index1Last = index1Start + cantidadByMicro;
            List<EditDistanceAction> dividedTasks = new ArrayList<>();
            for (int i = 0; i < cores; i++) {
                dividedTasks.add(new EditDistanceAction(queries, output, cores, index1Start, index1Last, true));
                index1Start = index1Last;
                index1Last = index1Start + cantidadByMicro;

            }
            ForkJoinTask.invokeAll(dividedTasks);
        }
        return null;
    }
}