package it.unitn.utils;

import org.eclipse.collections.api.factory.Lists;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.stream.Stream;

import static org.eclipse.collections.impl.collector.Collectors2.toImmutableList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

class WindowingTest {

    @TestFactory
    Stream<DynamicTest> windowed() {

        final var xs = Lists.immutable.with(1, 2, 3, 4, 5, 6, 7, 8, 9);

        return Stream.of(
            dynamicTest(
                "w/ n = 3",
                () -> assertEquals(
                    Lists.immutable.with(
                        Lists.immutable.with(1, 2, 3),
                        Lists.immutable.with(2, 3, 4),
                        Lists.immutable.with(3, 4, 5),
                        Lists.immutable.with(4, 5, 6),
                        Lists.immutable.with(5, 6, 7),
                        Lists.immutable.with(6, 7, 8),
                        Lists.immutable.with(7, 8, 9)
                    ),
                    Windowing.windowed(3, xs).collect(toImmutableList())
                )
            ),
            dynamicTest(
                "w/ n = size",
                () -> assertEquals(
                    Lists.immutable.with(
                        Lists.immutable.with(1, 2, 3, 4, 5, 6, 7, 8, 9)
                    ),
                    Windowing.windowed(9, xs).collect(toImmutableList())
                )
            ),
            dynamicTest(
                "w/ n > size",
                () -> assertEquals(
                    Lists.immutable.empty(),
                    Windowing.windowed(10, xs).collect(toImmutableList())
                )
            ),
            dynamicTest(
                "w/ n < 0",
                () -> assertEquals(
                    Lists.immutable.empty(),
                    Windowing.windowed(10, xs).collect(toImmutableList())
                )
            )
        );
    }

}
