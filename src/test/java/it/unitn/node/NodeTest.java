package it.unitn.node;

import it.unitn.Config;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.SortedSets;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.list.ListIterable;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.time.Duration;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.eclipse.collections.impl.collector.Collectors2.toImmutableList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

class NodeTest {

    @TestFactory
    Stream<DynamicTest> clockwise() {

        final var key2value =
            Maps.immutable.with(
                1, "1",
                2, "2",
                3, "3",
                4, "4"
            );

        return Stream.of(
            dynamicTest(
                "w/ key = 1",
                () -> assertEquals(
                    Lists.immutable.with("1", "2", "3", "4"),
                    Node.clockwise(key2value, 1)
                )
            ),
            dynamicTest(
                "w/ key = 3",
                () -> assertEquals(
                    Lists.immutable.with("3", "4", "1", "2"),
                    Node.clockwise(key2value, 3)
                )
            ),
            dynamicTest(
                "w/ key > maximum",
                () -> assertEquals(
                    Lists.immutable.with("1", "2", "3", "4"),
                    Node.clockwise(key2value, 5)
                )
            ),
            dynamicTest(
                "w/ key < minimum",
                () -> assertEquals(
                    Lists.immutable.with("1", "2", "3", "4"),
                    Node.clockwise(key2value, 0)
                )
            ),
            dynamicTest(
                "w/ empty key2node, is always empty",
                () -> IntStream.range(0, 1_0000).forEach(key -> assertEquals(
                    Lists.immutable.empty(),
                    Node.clockwise(Maps.immutable.empty(), key)
                ))
            ),
            dynamicTest(
                "w/ singleton key2node, always the same",
                () -> IntStream.range(0, 1_0000).forEach(key -> assertEquals(
                    Lists.immutable.with("0"),
                    Node.clockwise(Maps.immutable.with(0, "0"), key)
                ))
            )
        );
    }

    @TestFactory
    Stream<DynamicTest> clusters() {
        return Stream.of(
            dynamicTest(
                "it does not handle less than N elements",
                () -> IntStream.range(5, 10).mapToObj(N -> new Config(N, 0, 0, Duration.ZERO)).forEach(c -> assertThrows(
                    IllegalArgumentException.class,
                    () -> Node.clusters(c, SortedSets.immutable.with(1, 2, 3, 4))
                ))
            ),
            dynamicTest(
                "w/ M elements, it always returns M sub-lists",
                () -> IntStream.range(5, 10).mapToObj(N -> new Config(N, 0, 0, Duration.ZERO)).forEach(c -> assertEquals(
                    10,
                    Node.clusters(c, SortedSets.immutable.with(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).count()
                ))
            ),
            dynamicTest(
                "w/ N, it always returns sub-lists of N + 1 elements",
                () -> IntStream.range(5, 10).mapToObj(N -> new Config(N, 0, 0, Duration.ZERO)).forEach(c -> assertEquals(
                    IntSets.immutable.with(c.N() + 1),
                    IntSets.immutable.withAll(Node.clusters(c, SortedSets.immutable.with(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).mapToInt(RichIterable::size))
                ))
            ),
            dynamicTest(
                "the sub-list's last elements composes the original sortedset",
                () -> IntStream.range(5, 10).mapToObj(N -> new Config(N, 0, 0, Duration.ZERO)).forEach(c -> assertEquals(
                    SortedSets.immutable.with(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toImmutableList(),
                    Node.clusters(c, SortedSets.immutable.with(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map(ListIterable::getLast).collect(toImmutableList())
                ))
            )
        );
    }

}