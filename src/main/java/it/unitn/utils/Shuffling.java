package it.unitn.utils;

import org.eclipse.collections.api.list.ImmutableList;

import java.util.Collections;
import java.util.Random;

public interface Shuffling {

    static <V> ImmutableList<V> shuffle(ImmutableList<V> vs, long seed) {
        final var tmp = vs.toList();
        Collections.shuffle(tmp, new Random(seed));
        return tmp.toImmutable();
    }

}
