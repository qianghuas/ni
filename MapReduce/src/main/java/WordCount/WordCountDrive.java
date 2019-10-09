package WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDrive  extends Configured implements Tool {


    private  Configuration configuration = null;

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
    }



    @Override
    public Configuration getConf() {
        return configuration;
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration);
        StringBuilder stringBuilder = new StringBuilder();
        String s = stringBuilder.toString();

        job.setJarByClass(WordCountDrive.class);

        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCounteduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new WordCountDrive(), args);



    }
}
