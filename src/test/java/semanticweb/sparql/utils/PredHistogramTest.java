package semanticweb.sparql.utils;

import com.hp.hpl.jena.datatypes.xsd.impl.XSDBaseNumericType;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.syntax.*;
import com.hp.hpl.jena.tdb.store.Hash;
import junit.framework.Assert;
import org.junit.jupiter.api.Test;
import semanticweb.sparql.Operator;
import semanticweb.sparql.QDistanceHungarian;
import semanticweb.sparql.SparqlUtils;
import static org.junit.Assert.assertEquals;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PredHistogramTest {
    @Test
    public void testCreatePredHistogram() {
        PredHistogram predHistogram = new PredHistogram("uri","sub"," Select a....","rdf:type");
        assertEquals("", predHistogram.getOnSubQuery()," Select a....");
        assertEquals("",predHistogram.getOnSubType(),"uri");
    }
    @Test
    public void testToSamplingQueryFileString() {
        PredHistogram predHistogram = new PredHistogram("numeric","sub"," Sub a....","rdf:type");
        String resp = predHistogram.toSamplingQueryFileString(",");
        assertEquals("", 1,resp.split("\n").length);

        predHistogram.setQuery("obj","uri","Obj a....");
        resp = predHistogram.toSamplingQueryFileString(",");
        assertEquals("", 2,resp.split("\n").length);
        assertEquals("",predHistogram.getOnObjType(),"numeric");

        PredHistogram predHistogram1 = new PredHistogram("uri","obj"," Obj a....","rdf:type");
        resp = predHistogram1.toSamplingQueryFileString(",");
        assertEquals("", 1,resp.split("\n").length);
    }
}
