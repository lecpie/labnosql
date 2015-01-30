// cc JoinMapper1 Mapper for a reduce-side join

package org.myorg;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//vv JoinMapper1
public class JoinMapper1
    extends Mapper<LongWritable, Text, TextPair, Text> {

/* here define the variables */
        
	  @Override
	  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
  
/* here the code for retrieving the triples from file01 and send the prefix of the dewey_pid as key */
      
	  String line = value.toString();
     String[] tokens = line.split(" ");
     String id = tokens[0];
     String attr = tokens[1];
     String val = tokens[2];     
     
     if ("species".equals(attr) && val.contains("P_KK")) {
    	 int end = id.lastIndexOf('.');
    	 end = id.lastIndexOf('.', end - 1);
    	 
    	 String prefix = id.substring(0, end);
    	 
    	 Text word = new Text(prefix);
    	 TextPair pair = new TextPair(word, new Text("1"));
    	 
    	 context.write(pair, word);
     }
	  	  
  }
}
//^^ JoinMapper1

