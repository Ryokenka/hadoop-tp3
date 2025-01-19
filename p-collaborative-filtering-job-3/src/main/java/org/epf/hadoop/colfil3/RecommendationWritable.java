package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecommendationWritable implements Writable {
    private Text recommendedUser;
    private IntWritable count;

    // No-arg constructor
    public RecommendationWritable() {
        this.recommendedUser = new Text();
        this.count = new IntWritable(0);
    }

    public RecommendationWritable(String user, int count) {
        this.recommendedUser = new Text(user);
        this.count = new IntWritable(count);
    }

    public String getRecommendedUser() {
        return recommendedUser.toString();
    }

    public int getCount() {
        return count.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        recommendedUser.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        recommendedUser.readFields(in);
        count.readFields(in);
    }

    @Override
    public String toString() {
        return recommendedUser + ":" + count.get();
    }
}
