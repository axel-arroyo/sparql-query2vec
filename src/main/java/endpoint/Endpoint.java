package endpoint;

import semanticweb.sparql.KmedoidsGenerator;

public class Endpoint {

    public static void main(String[] args) throws Exception {
        //Getting parameters
        if(args.length == 0){
            System.out.println("Try with some of this parameters:");
            System.out.println("java -jar file.jar kmedoids /path/to/input.csv /path/to/output.csv /path/to/ids_time.csv #-of-centroids");
            System.out.println("java -jar file.jar edit-distance /path/to/input.csv /path/to/output.csv /path/to/prefixes #-of-cores");
            return;
        }
        String[] params = new String[args.length-1];
        System.arraycopy(args, 1, params, 0, args.length - 1);
        try {
            String task = args[0];
            if(task.equals("kmedoids")){
                System.out.println("Entering to kmedoids class");
                KmedoidsGenerator.main(params);
            }
            else if (task.equals("edit-distance")){

            }
        }catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("You need to specify a task as first parameter");
        }

    }
}
