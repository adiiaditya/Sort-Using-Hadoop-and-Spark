package org.myorg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


public class HadoopSort
{
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        if (args.length != 2)
        {
            System.err.println("Usage: HadoopSort <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Hadoop Sort");
        job.setJarByClass(HadoopSort.class);
        job.setJobName("HadoopSort");
        
        job.setNumReduceTasks(4);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        //Setting the output key value type of the mapper
        job.setMapOutputKeyClass(KeyPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(SortBytesMapper.class);
        job.setReducerClass(SortBytesReducer.class);
        job.setPartitionerClass(SortBytesPartitioner.class);

        job.setSortComparatorClass(SortBytesComparator.class);
        job.setGroupingComparatorClass(SortBytesGroupingComparator.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    //Mapper
    public static class SortBytesMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
        private Text one = new Text();
        private Text word = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            String s = line.substring(0,10);
            String r = line.substring(10,line.length()-1);
            // send it to reducer
            context.write(new KeyPair(s), new Text(r));
        }
    }
    
    //Reducer
    public static class SortBytesReducer extends Reducer<KeyPair, Text, Text, Text> {
        public void reduce(KeyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            for (Text val : values)
            {
                //print in sorted order
                context.write(key.getsortBytes() , val);
            }
        }
    }
    
    //Comparator
    public static class SortBytesComparator extends WritableComparator
    {
        public SortBytesComparator() {
            super(KeyPair.class, true);
        }
        
        public int compare(WritableComparable wc1, WritableComparable wc2)
        {
            KeyPair key1 = (KeyPair) wc1;
            KeyPair key2 = (KeyPair) wc2;
            return key1.getsortBytes().compareTo(key2.getsortBytes());
        }
    }
    
    //Grouping Comparator
    public static class SortBytesGroupingComparator extends WritableComparator
    {
        public SortBytesGroupingComparator() {
            super(KeyPair.class, true);
        }
        
        public int compare(WritableComparable wc1, WritableComparable wc2)
        {
            KeyPair key1 = (KeyPair) wc1;
            KeyPair key2 = (KeyPair) wc2;
            return key1.getsortBytes().compareTo(key2.getsortBytes());
        }
    }

    
    //Partitioner
    public static class SortBytesPartitioner extends Partitioner<KeyPair, Text>
    {
        
        @Override
        public int getPartition(KeyPair key, Text value, int numReducers)
        {
          return Math.abs((key.sortBytes.hashCode() & Integer.MAX_VALUE) % numReducers);
        }
    }
    
    public static class KeyPair implements WritableComparable<KeyPair> {
        
        private Text sortBytes;
        
        //The default constructor
        public KeyPair()
        {
            sortBytes = new Text();
        }
        
        //constructor, initializing the sortBytes
        public KeyPair(String sort)
        {
            sortBytes = new Text(sort);
        }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            // TODO Auto-generated method stub
            sortBytes.readFields(in);
        }
        
        @Override
        public void write(DataOutput out) throws IOException {
            // TODO Auto-generated method stub
            sortBytes.write(out);
        }
        
        
        @Override
        /**
         * This comparator controls the sort order of the keys.
         */
        public int compareTo(KeyPair pair)
        {
            int compareValue = this.sortBytes.compareTo(pair.sortBytes);
            return compareValue;    // sort ascending
        }
        
        //the Getter and setter methods
        public Text getsortBytes()
        {
            return sortBytes;
        }
        
        public void setsortBytes(Text sortBytes)
        {
            this.sortBytes = sortBytes;
        }
        
    }
}