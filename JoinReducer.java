// cc JoinReducer Reducer for join

package org.myorg;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// vv JoinReducer
public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

  @Override
  protected void reduce(TextPair key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

      /* here comes the reducer code */

	  for (Text val : values) {
		  if (key.getFirst().equals(val)) continue;
		  
		  context.write(key.getFirst(), val);	  
	  }
	  
  }
}
// ^^ JoinReducer

