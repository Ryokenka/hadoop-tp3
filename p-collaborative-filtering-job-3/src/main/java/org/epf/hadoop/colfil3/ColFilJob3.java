package org.epf.hadoop.colfil3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob3 {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RecommendationDriver <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job3-Recommendations");
        job.setJarByClass(ColFilJob3.class);

        // Mapper
        job.setMapperClass(RecommendationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RecommendationWritable.class);

        // Reducer
        job.setReducerClass(RecommendationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // By requirement, only 1 reducer
        job.setNumReduceTasks(1);

        // Inputs/Outputs
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}