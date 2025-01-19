package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class CommonFriendsMapper
        extends Mapper<LongWritable, Text, UserPair, IntWritable> {

    private final static IntWritable ZERO = new IntWritable(0);
    private final static IntWritable ONE  = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Ex: value = "Alice    Bob,Charlie"
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;  // skip empty lines
        }

        // On sépare selon une tabulation, par ex
        // On peut avoir à ajuster selon le format exact de votre job1
        String[] parts = line.split("\\s+", 2);
        if (parts.length < 2) {
            return; // malformed line
        }

        String user = parts[0];
        String friendListStr = parts[1];

        // Split la liste d'amis
        String[] friends = friendListStr.split(",");
        // Nettoyage éventuel, si besoin
        // Ex: remove empty strings, etc.

        // 1) Emettre (user, friend) => 0 pour signaler une liaison directe
        for (String friend : friends) {
            if (!friend.isEmpty()) {
                context.write(
                        new UserPair(user, friend),
                        ZERO
                );
            }
        }

        // 2) Emettre (f1, f2) => 1 pour toutes les paires f1 != f2
        //    qui apparaissent dans la liste d'amis du user
        //    On peut faire un double "for" sur l'array friends
        for (int i = 0; i < friends.length; i++) {
            for (int j = i + 1; j < friends.length; j++) {
                String f1 = friends[i];
                String f2 = friends[j];

                if (!f1.isEmpty() && !f2.isEmpty()) {
                    context.write(new UserPair(f1, f2), ONE);
                }
            }
        }
    }
}
