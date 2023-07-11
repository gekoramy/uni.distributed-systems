package it.unitn.utils;

import static it.unitn.utils.Comparing.cmp;
import static it.unitn.utils.Ordering.*;

public interface Ranging {

    static boolean disjoint(Range a, Range b) {
        final var A = cmp(a.gt(), a.lte());
        final var B = cmp(b.gt(), b.lte());

        if (A == EQ || B == EQ)
            return false;

        if (A == GT && B == GT)
            return false;

        return A == LT && B == LT
            ? a.lte() <= b.gt() || b.lte() <= a.gt()   // (....](....]
            : a.lte() <= b.gt() && b.lte() <= a.gt();  // ..](....](..
    }

}
