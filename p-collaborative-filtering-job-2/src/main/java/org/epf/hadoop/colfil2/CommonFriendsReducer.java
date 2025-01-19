package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonFriendsReducer
        extends Reducer<UserPair, IntWritable, UserPair, IntWritable> {

    private final IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(UserPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        boolean alreadyDirectFriends = false;
        int commonFriendsCount = 0;

        for (IntWritable val : values) {
            if (val.get() == 0) {
                // Ils sont déjà connectés directement
                alreadyDirectFriends = true;
                // On pourrait faire un "break" puisqu'on sait déjà
                // qu'on n'émettra rien pour cette paire.
                break;
            } else {
                // val.get() == 1
                commonFriendsCount += 1;
            }
        }

        // Si pas de relation directe ET le nb d'amis en commun est > 0
        if (!alreadyDirectFriends && commonFriendsCount > 0) {
            outValue.set(commonFriendsCount);
            context.write(key, outValue);
        }
    }
}
