package org.epf.hadoop.colfil2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob2 {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: CommonFriendsDriver <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CommonFriendsJob2");

        job.setJarByClass(ColFilJob2.class);

        // Mapper
        job.setMapperClass(CommonFriendsMapper.class);
        // Sortie mapper
        job.setMapOutputKeyClass(UserPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reducer
        job.setReducerClass(CommonFriendsReducer.class);
        // Nombre de reducers (comme demandé)
        job.setNumReduceTasks(2);

        // Sortie job
        job.setOutputKeyClass(UserPair.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Lancement du job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}