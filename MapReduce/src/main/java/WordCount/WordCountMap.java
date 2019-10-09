package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMap  extends Mapper<LongWritable, Text,Text, IntWritable> {
    Text k = new Text();
    IntWritable i = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        for (String s: split) {
            k.set(s);
            i.set(1);
            context.write(k,i);
        }
    }
}
