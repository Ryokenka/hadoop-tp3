package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RecommendationReducer
        extends Reducer<Text, RecommendationWritable, Text, Text> {

    private static final int K = 5;  // top 5

    @Override
    protected void reduce(Text key, Iterable<RecommendationWritable> values, Context context)
            throws IOException, InterruptedException {

        // 1. Collect all
        List<RecommendationWritable> list = new ArrayList<>();
        for (RecommendationWritable val : values) {
            // We *must* create a copy if we are storing them, because
            // the val object can be reused by Hadoop
            list.add(new RecommendationWritable(
                    val.getRecommendedUser(),
                    val.getCount()
            ));
        }

        // 2. Sort by count DESC; if tie, by recommended user ASC
        Collections.sort(list, new Comparator<RecommendationWritable>() {
            @Override
            public int compare(RecommendationWritable o1, RecommendationWritable o2) {
                // Compare count DESC
                int cmp = Integer.compare(o2.getCount(), o1.getCount());
                if (cmp != 0) {
                    return cmp;
                }
                // Tiebreak: recommended user alphabetical
                return o1.getRecommendedUser().compareTo(o2.getRecommendedUser());
            }
        });

        // 3. Keep top 5
        if (list.size() > K) {
            list = list.subList(0, K);
        }

        // 4. Build a single output string
        // e.g. "alice:3,bob:2,xxx:1..."
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<list.size(); i++) {
            if (i>0) sb.append(",");
            sb.append(list.get(i).toString());
            // or manually:  sb.append(list.get(i).getRecommendedUser())
            //               .append(":")
            //               .append(list.get(i).getCount());
        }

        // 5. Write output
        context.write(key, new Text(sb.toString()));
    }
}
