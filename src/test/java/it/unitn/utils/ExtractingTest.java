package it.unitn.utils;

import org.eclipse.collections.api.factory.SortedMaps;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

class ExtractingTest {

    @TestFactory
    Stream<DynamicTest> extract() {

        final var tree = new TreeMap<>(Map.of(
            1, "1",
            2, "2",
            3, "3",
            4, "4",
            5, "5",
            6, "6",
            7, "7",
            8, "8",
            9, "9"
        ));

        return Stream.of(
            dynamicTest(
                "w/ LT, it should discard above",
                () -> assertEquals(
                    SortedMaps.immutable.of(
                        1, "1",
                        2, "2",
                        3, "3"
                    ),
                    Extracting.extract(tree, -1, 3)
                )
            ),
            dynamicTest(
                "w/ LT, it should discard below",
                () -> assertEquals(
                    SortedMaps.immutable.of(
                        7, "7",
                        8, "8",
                        9, "9"
                    ),
                    Extracting.extract(tree, 6, 9)
                )
            ),
            dynamicTest(
                "w/ LT, it should discard above and below",
                () -> assertEquals(
                    SortedMaps.immutable.of(
                        2, "2",
                        3, "3"
                    ),
                    Extracting.extract(tree, 1, 3)
                )
            ),
            dynamicTest(
                "w/ EQ, it should not discard anything",
                () -> assertEquals(
                    SortedMaps.immutable.ofSortedMap(tree),
                    Extracting.extract(tree, 1, 1)
                )
            ),
            dynamicTest(
                "w/ GT, it should discard the middle",
                () -> assertEquals(
                    SortedMaps.immutable.of(
                        1, "1",
                        2, "2",
                        8, "8",
                        9, "9"
                    ),
                    Extracting.extract(tree, 7, 2)
                )
            )
        );
    }


}
