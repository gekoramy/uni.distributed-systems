package it.unitn.utils;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

class ComparingTest {

    @TestFactory
    Stream<DynamicTest> cmp() {
        return Stream.of(
            dynamicTest("a < b : LT", () -> assertEquals(Ordering.LT, Comparing.cmp(1, 2))),
            dynamicTest("a > b : GT", () -> assertEquals(Ordering.GT, Comparing.cmp(2, 1))),
            dynamicTest("a = b : EQ", () -> assertEquals(Ordering.EQ, Comparing.cmp(1, 1)))
        );
    }

}
