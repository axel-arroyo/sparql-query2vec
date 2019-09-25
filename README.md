Wrapper for using graph algorithms in Graph Matching Toolkit http://www.fhnw.ch/wirtschaft/iwi/gmt for RDF graphs

###Run
For run use that
```$bash
java -jar graph-edit-distance.jar /path/to/data_queries.csv /path_to_output/ /path/to/prefixes.txt 4
```
Last number(4) is the number of cores or proccess to run in paralell.

###Compilation
Execute to compile:
```$bash
mvn clean package
```
In the generated target you will find a graph-edit-distance-1.0-SNAPSHOT.jar