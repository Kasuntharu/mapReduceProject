package question2_BDA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PopAgg2 {

    public static class PopAgg2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text popular = new Text("Number of movies: ");  // Key for movies with popularity over 500.0

        @Override


        protected void map(LongWritable key, Text value, Context context)

                throws IOException, InterruptedException {
            // Skip the header line (first line)
            if (key.get() > 0) {
                String[] fields = value.toString().split(",");  // Split by commas
                double popularity = Double.parseDouble(fields[1]);
                double vote_average = Double.parseDouble(fields[2]);
                double vote_count = Double.parseDouble(fields[3]);// Extract popularity (2nd field)
                if (vote_count > 10000.0) {
                    context.write(popular, one);  // Emit <"popular", 1> for each qualifying movie
                }
            }
        }
    }

    public static class PopAgg2Reducer extends

            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)

                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            result.set(count);
            context.write(key, result);  // Emit <"popular", total_count>
        }
    }

    public static void main(String[] args)
            throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Popular Movie Count");
        job.setJarByClass(PopAgg2.class);
        job.setMapperClass(PopAgg2Mapper.class);
        job.setReducerClass(PopAgg2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}