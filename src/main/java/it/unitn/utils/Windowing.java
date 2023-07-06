package it.unitn.utils;

import org.eclipse.collections.api.list.ImmutableList;

import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface Windowing {

    static <T> Stream<ImmutableList<T>> windowed(int n, ImmutableList<T> xs) {
        return IntStream.range(0, xs.size() - n + 1).mapToObj(i -> xs.subList(i, i + n));
    }

}
