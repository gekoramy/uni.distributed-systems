package it.unitn.utils;

import org.eclipse.collections.api.factory.Lists;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

class ShufflingTest {

    @TestFactory
    Stream<DynamicTest> shuffle() {

        final var xs = Lists.immutable.with(1, 2, 3, 4, 5, 6, 7, 8, 9);

        return Stream.of(
            dynamicTest(
                "reproducible",
                () -> assertEquals(
                    Lists.immutable.with(8, 9, 1, 3, 4, 2, 7, 5, 6),
                    Shuffling.shuffle(xs, 3)
                )
            )
        );
    }

}
