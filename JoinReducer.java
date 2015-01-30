// cc JoinReducer Reducer for join

package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// vv JoinReducer
public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

  @Override
  protected void reduce(TextPair key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

      /* here comes the reducer code */

	  System.out.println("This is a group with key : {" + key.getFirst().toString() + "," + key.getSecond().toString() + "}");
	  
	  boolean first  = false;
	  boolean second = false;
	  ArrayList<Text> goodValues = new ArrayList<Text>();
	  
	  for (Text val : values) {
		 /* 
		  if (key.getFirst().equals(val)) continue;
		  
		  context.write(key.getFirst(), val);
		  */
		  
		  if (key.getFirst().equals(val)) {
			  first = true;
		  }
		  else {
			  second = true;
			  goodValues.add(val);
		  }
		  
		  System.out.println("value : " + val.toString());
	  }
	  
	  if (first && second) {
		  System.out.println("Match !");
		  
		  for (Text val : goodValues) {
			  
			  System.out.println("Writing : " + "{" + key.getFirst().toString() + "," + key.getSecond().toString() + "} : "
			  + val.toString());
			  context.write(key.getFirst(), val);
			  
		  }
	  }
  }
}
// ^^ JoinReducer

