package it.unitn.node;

import it.unitn.Config;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.SortedMaps;
import org.eclipse.collections.api.factory.SortedSets;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.tuple.primitive.IntObjectPair;
import org.eclipse.collections.impl.collector.Collectors2;
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.time.Duration;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.eclipse.collections.impl.collector.Collectors2.toImmutableList;
import static org.eclipse.collections.impl.collector.Collectors2.toImmutableSortedMap;
import static org.eclipse.collections.impl.tuple.Tuples.pair;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

class NodeTest {

    @TestFactory
    Stream<DynamicTest> grant() {
        return Stream.of(
            dynamicTest(
                "[] → ([], [])",
                () -> assertEquals(
                    Tuples.pair(SortedMaps.immutable.empty(), Lists.immutable.empty()),
                    Node.grant(Lists.immutable.<IntObjectPair<Object>>empty(), IntObjectPair::getOne, IntObjectPair::getTwo)
                )
            ),
            dynamicTest(
                "[1, 2, 3, 4, 5] → ([1, 2, 3, 4, 5], [])",
                () -> {
                    final var xs = Lists.immutable.with(
                        PrimitiveTuples.pair(1, new Object()),
                        PrimitiveTuples.pair(2, new Object()),
                        PrimitiveTuples.pair(3, new Object()),
                        PrimitiveTuples.pair(4, new Object()),
                        PrimitiveTuples.pair(5, new Object())
                    );

                    assertEquals(
                        Tuples.pair(
                            xs.stream().collect(Collectors2.toImmutableSortedMap(IntObjectPair::getOne, IntObjectPair::getTwo)),
                            Lists.immutable.empty()
                        ),
                        Node.grant(xs, IntObjectPair::getOne, IntObjectPair::getTwo)
                    );
                }
            ),
            dynamicTest(
                "[1, 1, 2, 2] → ([1, 2], [1, 2])",
                () -> {
                    final var xs = Lists.immutable.with(
                        PrimitiveTuples.pair(1, new Object()),
                        PrimitiveTuples.pair(1, new Object()),
                        PrimitiveTuples.pair(2, new Object()),
                        PrimitiveTuples.pair(2, new Object())
                    );

                    assertEquals(
                        Tuples.pair(
                            Stream.of(xs.get(0), xs.get(2)).collect(Collectors2.toImmutableSortedMap(IntObjectPair::getOne, IntObjectPair::getTwo)),
                            Lists.immutable.with(xs.get(1), xs.get(3))
                        ),
                        Node.grant(xs, IntObjectPair::getOne, IntObjectPair::getTwo)
                    );
                }
            ),
            dynamicTest(
                "[5, 4, 3, 2, 1] → ([5], [4, 3, 2, 1])",
                () -> {
                    final var xs = Lists.immutable.with(
                        PrimitiveTuples.pair(5, new Object()),
                        PrimitiveTuples.pair(4, new Object()),
                        PrimitiveTuples.pair(3, new Object()),
                        PrimitiveTuples.pair(2, new Object()),
                        PrimitiveTuples.pair(1, new Object())
                    );

                    assertEquals(
                        Tuples.pair(
                            xs.take(1).toSortedMap(IntObjectPair::getOne, IntObjectPair::getTwo).toImmutable(),
                            xs.drop(1)
                        ),
                        Node.grant(xs, IntObjectPair::getOne, IntObjectPair::getTwo)
                    );
                }
            ),
            dynamicTest(
                "[1, 3, 2, 5, 4] → ([1, 3, 5], [2, 4])",
                () -> {
                    final var xs = Lists.immutable.with(
                        PrimitiveTuples.pair(1, new Object()),
                        PrimitiveTuples.pair(3, new Object()),
                        PrimitiveTuples.pair(2, new Object()),
                        PrimitiveTuples.pair(5, new Object()),
                        PrimitiveTuples.pair(4, new Object())
                    );

                    assertEquals(
                        Tuples.pair(
                            Stream.of(xs.get(0), xs.get(1), xs.get(3)).collect(Collectors2.toImmutableSortedMap(IntObjectPair::getOne, IntObjectPair::getTwo)),
                            Lists.immutable.with(xs.get(2), xs.get(4))
                        ),
                        Node.grant(xs, IntObjectPair::getOne, IntObjectPair::getTwo)
                    );
                }
            )
        );
    }

    @TestFactory
    Stream<DynamicTest> stakeholdersByPriority() {

        final var key2nodes =
            IntStream.range(1, 10)
                .boxed()
                .collect(toImmutableSortedMap(k -> k, String::valueOf));

        return Stream.of(
            dynamicTest(
                "it contains N distinct elements",
                () -> IntStream.range(1, 10)
                    .mapToObj(N -> new Config(N, 0, 0, Duration.ZERO))
                    .forEach(c -> {
                        assertEquals(
                            c.N(),
                            Node.stakeholdersByPriority(c, 4, key2nodes, 5).size()
                        );
                        assertEquals(
                            c.N(),
                            Node.stakeholdersByPriority(c, 4, key2nodes, 5).toSet().size()
                        );
                    })
            ),
            dynamicTest(
                "if it lists 'priority', it lists it in 1st position",
                () -> {
                    final var actor = key2nodes.get(4);

                    IntStream.range(1, 10)
                        .mapToObj(N -> new Config(N, 0, 0, Duration.ZERO))
                        .forEach(c -> {
                            final var premise =
                                key2nodes.keysView()
                                    .collect(key -> Node.stakeholdersByPriority(c, 4, key2nodes, key))
                                    .select(xs -> xs.contains(actor));

                            assertFalse(premise.isEmpty());
                            premise.forEach(xs -> assertEquals(actor, xs.getFirst()));
                        });
                }
            ),
            dynamicTest(
                "priority parameter changes only the order",
                () -> IntStream.range(1, 10)
                    .mapToObj(N -> new Config(N, 0, 0, Duration.ZERO))
                    .forEach(c -> key2nodes.keysView().forEach(key -> {

                        final var xss = key2nodes.keysView()
                            .collect(node -> Node.stakeholdersByPriority(c, node, key2nodes, key))
                            .collect(RichIterable::toImmutableSortedList)
                            .toImmutableList();

                        xss.forEach(xs -> assertEquals(xss.getFirst(), xs));

                    }))
            )
        );
    }

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
                    Lists.immutable.with(pair(1, "1"), pair(2, "2"), pair(3, "3"), pair(4, "4")),
                    Node.clockwise(key2value, 1)
                )
            ),
            dynamicTest(
                "w/ key = 3",
                () -> assertEquals(
                    Lists.immutable.with(pair(3, "3"), pair(4, "4"), pair(1, "1"), pair(2, "2")),
                    Node.clockwise(key2value, 3)
                )
            ),
            dynamicTest(
                "w/ key > maximum",
                () -> assertEquals(
                    Lists.immutable.with(pair(1, "1"), pair(2, "2"), pair(3, "3"), pair(4, "4")),
                    Node.clockwise(key2value, 5)
                )
            ),
            dynamicTest(
                "w/ key < minimum",
                () -> assertEquals(
                    Lists.immutable.with(pair(1, "1"), pair(2, "2"), pair(3, "3"), pair(4, "4")),
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
                    Lists.immutable.with(pair(0, "0")),
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
