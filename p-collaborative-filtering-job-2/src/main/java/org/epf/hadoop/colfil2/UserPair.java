package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserPair implements WritableComparable<UserPair> {
    private Text user1;
    private Text user2;

    // --- Constructeurs ---
    public UserPair() {
        this.user1 = new Text("");
        this.user2 = new Text("");
    }

    /**
     * On force l'ordre lexicographique user1 <= user2
     */
    public UserPair(String user1, String user2) {
        if (user1.compareTo(user2) <= 0) {
            this.user1 = new Text(user1);
            this.user2 = new Text(user2);
        } else {
            this.user1 = new Text(user2);
            this.user2 = new Text(user1);
        }
    }

    // --- Getters utiles ---
    public String getFirstUser() {
        return user1.toString();
    }

    public String getSecondUser() {
        return user2.toString();
    }

    // --- Méthodes de l'interface Writable ---
    @Override
    public void write(DataOutput out) throws IOException {
        user1.write(out);
        user2.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        user1.readFields(in);
        user2.readFields(in);
    }

    // --- Méthode compareTo (pour le tri / regroupement) ---
    @Override
    public int compareTo(UserPair o) {
        // Compare d'abord user1
        int cmp = this.user1.compareTo(o.user1);
        if (cmp != 0) {
            return cmp;
        }
        // Si user1 est le même, compare user2
        return this.user2.compareTo(o.user2);
    }

    // --- Égalité / Hashcode si besoin ---
    // (Facultatif mais souvent utile, le partitionneur par défaut
    //  utilise le hashcode. Également la logique d'égalité.)
    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof UserPair)) {
            return false;
        }
        UserPair other = (UserPair) obj;
        return this.user1.equals(other.user1) && this.user2.equals(other.user2);
    }

    @Override
    public int hashCode() {
        return user1.hashCode() * 163 + user2.hashCode();
    }

    // --- toString (pour l'affichage final) ---
    @Override
    public String toString() {
        return user1.toString() + "," + user2.toString();
    }
}
