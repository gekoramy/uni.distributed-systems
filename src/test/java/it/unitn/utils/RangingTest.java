package it.unitn.utils;

import org.eclipse.collections.api.factory.primitive.IntLists;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.junit.jupiter.api.DynamicTest.stream;
import static org.junit.jupiter.api.Named.named;

class RangingTest {

    @TestFactory
    Stream<DynamicTest> disjoint() {

        record Test(Range a, Range b, boolean expected) {}

        final var examples =
            Stream.of(
                    new Test(new Range(1, 5), new Range(2, 5), false),
                    new Test(new Range(1, 5), new Range(5, 6), true),
                    new Test(new Range(1, 5), new Range(1, 5), false),
                    new Test(new Range(7, 3), new Range(3, 7), true),
                    new Test(new Range(7, 3), new Range(3, 4), true),
                    new Test(new Range(7, 3), new Range(3, 9), false)
                )
                .map(x -> named("%s %s → %s".formatted(x.a(), x.b(), x.expected()), x));

        return Stream.concat(
            stream(
                examples,
                t -> assertEquals(t.expected(), Ranging.disjoint(t.a(), t.b()))
            ),
            Stream.of(
                dynamicTest(
                    "a and b, where a is EQ, are never disjoint",
                    () -> {

                        final var rng = new Random(3L);

                        final var as_gt = IntLists.immutable.withAll(rng.ints(1_000L, 0, 1_000));
                        final var as_lte = IntLists.immutable.withAll(rng.ints(1_000L, 0, 1_000));

                        final var as = as_gt.zipInt(as_lte).collect(p -> new Range(p.getOne(), p.getTwo()));
                        final var bs = IntLists.immutable.withAll(rng.ints(1000L, 0, 1_000)).collect(b -> new Range(b, b));

                        as.zip(bs).forEach(p -> assertFalse(Ranging.disjoint(p.getOne(), p.getTwo())));
                    }
                ),
                dynamicTest(
                    "a and a are never disjoint",
                    () -> {
                        final var rng = new Random(3L);

                        final var as_gt = IntLists.immutable.withAll(rng.ints(1_000L, 0, 1_000));
                        final var as_lte = IntLists.immutable.withAll(rng.ints(1_000L, 0, 1_000));

                        as_gt.zipInt(as_lte)
                            .collect(p -> new Range(p.getOne(), p.getTwo()))
                            .forEach(r -> assertFalse(Ranging.disjoint(r, r)));
                    }
                ),
                dynamicTest(
                    "a and b are disjoint IFF b and a are disjoint",
                    () -> {
                        final var rng = new Random(3L);

                        final var as_gt = IntLists.immutable.withAll(rng.ints(1_000L, 0, 1_000));
                        final var as_lte = IntLists.immutable.withAll(rng.ints(1_000L, 0, 1_000));
                        final var bs_gt = IntLists.immutable.withAll(rng.ints(1_000L, 0, 1_000));
                        final var bs_lte = IntLists.immutable.withAll(rng.ints(1_000L, 0, 1_000));

                        final var as = as_gt.zipInt(as_lte).collect(p -> new Range(p.getOne(), p.getTwo()));
                        final var bs = bs_gt.zipInt(bs_lte).collect(p -> new Range(p.getOne(), p.getTwo()));

                        as.zip(bs).forEach(p -> assertEquals(
                            Ranging.disjoint(p.getOne(), p.getTwo()),
                            Ranging.disjoint(p.getTwo(), p.getOne())
                        ));
                    }
                )
            )
        );
    }

}
