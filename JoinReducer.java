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
	  
	  boolean first  = false;
	  boolean second = false;
	  ArrayList<String> goodValues = new ArrayList<String>();
	  
	  for (Text val : values) {
		  
		  if (key.getFirst().equals(val)) {
			  first = true;
		  }
		  else {
			  second = true;
			  goodValues.add(val.toString());
		  }		  
	  }
	  
	  if (first && second) {		  
		  for (String val : goodValues) {			  
			  context.write(key.getFirst(), new Text(val));
			  
		  }
	  }
  }
}
// ^^ JoinReducer

