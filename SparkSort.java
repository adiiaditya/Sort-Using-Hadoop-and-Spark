import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.io.Serializable;

public class SparkSort {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 2)
        {
            System.err.println("Usage: SparkSort <in> <out>");
            System.exit(2);
        }
        
        String inputPath = args[0];
        
        SparkConf config = new SparkConf.setAppName("SparkSort");
        JavaSparkContext sparkContext = new JavaSparkContext(config);
        
        JavaRDD<String> lines = sparkContext.textFile(inputPath, 1);
 
     //extracting data and creating pairs
    JavaPairRDD<String, Tuple2<String>> pairs = lines.mapToPair(new PairFunction<String, String, Tuple2<String>>()
	{ 
      public Tuple2<String, Tuple2<String>> call(String s)
	  { 
		String sortByte = s.substring(0,10);
		String restByte = s.substring(10, s.length()-1);
        Tuple2<String> rest = new Tuple2<String>(restByte); 
        return new Tuple2<String, Tuple2<String>>(sortByte, restByte); 
      } 
    }); 
 
    List<Tuple2<String, Tuple2<String>>> output = pairs.collect(); 
    for (Tuple2 part1 : output)
	{ 
       Tuple2<String> rest = (Tuple2<String>) part1._2; 
    } 
 
    JavaPairRDD<String, Iterable<Tuple2<String>>> groups = pairs.groupByKey();     
    List<Tuple2<String, Iterable<Tuple2<String>>>> outpupart2 = groups.collect(); 
     
    //inplementing sorting
    JavaPairRDD<String, Iterable<Tuple2<String>>> sort = groups.mapValues(new Function<Iterable<Tuple2<String>>, Iterable<Tuple2<String>> >()
	{   
      public Iterable<Tuple2<String>> call(Iterable<Tuple2<String>> s) { 
        List<Tuple2<String>> newList = new ArrayList<Tuple2<String>>(iterableToList(s));       
        Collections.sort(newList, SparkTupleComparator.INSTANCE); 
        return newList; 
      } 
    }); 
	
    sort.saveAsTextFile(args[1]);
    System.exit(0); 
  } 
   
  static List<Tuple2<String>> iterableToList(Iterable<Tuple2<String>> iterable)
  { 
    List<Tuple2<tring>> list = new ArrayList<Tuple2<String>>(); 
    for (Tuple2<String> item : iterable) { 
       lispart1.add(item); 
    } 
    return list; 
  } 

// comparator
    public class SparkTupleComparator  implements Comparator<Tuple2<String>>, Serializable 
	{ 
 
    static final SparkTupleComparator INSTANCE = new SparkTupleComparator(); 
        public int compare(Tuple2<String> t1, Tuple2<String> part2)
        { 
           return t1._1.compareTo(part2._1); 
        } 
    }
}