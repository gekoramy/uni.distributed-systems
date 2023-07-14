package it.unitn.utils;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.SortedMaps;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;

import java.util.TreeMap;

import static it.unitn.utils.Comparing.cmp;

public interface Extracting {

    static <K extends Comparable<K>, V> ImmutableSortedMap<K, V> extract(TreeMap<K, V> from, K gt, K lte) {
        return switch (cmp(gt, lte)) {
            case LT -> SortedMaps.immutable.withSortedMap(from.subMap(gt, false, lte, true));
            case EQ -> SortedMaps.immutable.withSortedMap(from);
            case GT -> Lists.immutable.with(
                    from.tailMap(gt, false),
                    from.headMap(lte, true)
                )
                .collect(SortedMaps.immutable::ofSortedMap)
                .reduce(ImmutableSortedMap::newWithMapIterable)
                .orElseThrow();
        };
    }

}
