package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RecommendationMapper
        extends Mapper<LongWritable, Text, Text, RecommendationWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // "value" is something like: "alice,david    1"
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        // Split into "alice,david" and "1"
        String[] parts = line.split("\\s+");
        if (parts.length != 2) {
            return; // or handle error
        }
        String pairStr = parts[0];  // e.g. "alice,david"
        int mutualCount = Integer.parseInt(parts[1]);

        // Now parse "alice,david" -> (user1, user2)
        String[] users = pairStr.split(",");
        if (users.length != 2) {
            return; // or handle error
        }
        String user1 = users[0];
        String user2 = users[1];

        // Then emit recommendations in both directions
        context.write(
                new Text(user1),
                new RecommendationWritable(user2, mutualCount)
        );
        context.write(
                new Text(user2),
                new RecommendationWritable(user1, mutualCount)
        );
    }
}
