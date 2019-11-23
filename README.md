Approaches to vectorize Sparql queries and their metadata for Machine Learning tasks.
### Some baselines
- Rhassan code for algebra queries representation.
- Rhassan code for graph pattern queries representation based in kmedoids.
- **Queries representations using sets of features for DeepSet architecture**(working on..)

### Run
For run use that

System.out.println("Try with some of this parameters:");
```$bash
 java -jar file.jar kmedoids /path/to/input.csv /path/to/output.csv /path/to/ids_time.csv #-of-centroids
```
#### For generate edit distances vectors
```$bash
java -jar file.jar edit-distance /path/to/input.csv /path/to/output.csv /path/to/prefixes #-of-cores
```
Last number(4) is the number of cores or proccess to run in paralell.

#### For generate vectors for deepset architecture:
```$bash
java -jar file.jar deepset-features /path/to/input.csv /path/to/output.csv tables,joins,predicates /path/to/prefixes [--cores=numOfCores] [--length=numOfTuples] [--output-delimiter=symbolToDelimitColumns]
```

            
### Compilation
Execute to compile:
```$bash
mvn clean package
```
In the generated target you will find a graph-edit-distance-1.0-SNAPSHOT.jar
