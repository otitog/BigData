//IntWritable: writable class extends comparable class.
//writable class: serialization and deserialization(into byte).
//10 mappers-driver, 10 mappers; mapper-upper limits? slave-node-core-mapper.
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
        private String regex;

        @Override
		public void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            noGram = configuration.getInt(name:"noGram",defaultValue:5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			   //read sentence by sentence
               //split into ngram(2gram - ngram)
               //write to disk
               String sentence = value.toString();
               sentence = sentence.trim().toLowerCase().replace("[^a-z]"," ");

               // i love big data n=3
               //i love,love big, big data
               //i love big, love big data
               String[] words = regex.split("\\s+");
               if(words.length <2 ) {
                   logger.info("it only has one word: xxx");
                   return;

               }
               StringBuilder phrase;
               for(int i = 0;i < words.length; i++)
               {
                   phrase = new StringBuilder();
                   phrase.append(words[i]);
                   for(int j = 1;i+j<words.length&&j<noGram; j++)
                   {
                       phrase.append(words[i+j]);
                       //i love
                       //i love big
                       //love big
                       //love big data
                       context.write(new Text(phrase,toString()), new IntWritable(value:1));


			}
			

				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
		    //key = big, big data, love big data...
            //value = <1,1,1,1....>
            int sum = 0;
            for(IntWritable value: values) {
                sum + = value.get();
            }
            IntWritable i = new IntWritable(value:1);
            context.write(key,new IntWritable(sum));

		}
	}

}