package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Créer une liste pour les utilisateurs liés
        ArrayList<String> relatedUsers = new ArrayList<>();
        
        // Ajouter les utilisateurs liés à la liste
        for (Text value : values) {
            relatedUsers.add(value.toString());
        }
        
        // Trier la liste pour s'assurer qu'elle soit dans l'ordre
        Collections.sort(relatedUsers);

        // Joindre les utilisateurs liés dans une seule chaîne séparée par des virgules
        String result = String.join(",", relatedUsers);
        
        // Écrire la sortie (clé = utilisateur, valeur = liste d'utilisateurs liés)
        context.write(key, new Text(result));
    }
}
