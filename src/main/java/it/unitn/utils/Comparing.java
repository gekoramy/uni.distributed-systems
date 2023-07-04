package it.unitn.utils;

public interface Comparing {

    static <T extends Comparable<T>> Ordering cmp(T a, T b) {
        final var cmp = a.compareTo(b);

        if (cmp < 0)
            return Ordering.LT;

        if (cmp > 0)
            return Ordering.GT;

        return Ordering.EQ;
    }

}
