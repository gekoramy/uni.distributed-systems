package it.unitn.root;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.Config;
import it.unitn.client.Client;
import it.unitn.extensions.ActorTestKitExt;
import it.unitn.node.Node;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.stream.Stream;

import static it.unitn.root.GetOrPut.Get;
import static it.unitn.root.GetOrPut.Put;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

class RootTest {

    @TestFactory
    Stream<DynamicTest> onDDJoin() {
        return Stream.of(
            dynamicTest(
                "cannot join w/ already taken key",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {

                        final var probe = test.kit().<DidOrDidnt.Join>createTestProbe();

                        final var root = test.kit().spawn(
                            RootImpl.init(
                                new Config(4, 3, 2, Duration.ofSeconds(1L)),
                                IntSets.immutable.with(10, 20, 30, 40),
                                (r, k2n, n) -> Behaviors.monitor(DidOrDidnt.Join.class, probe.ref(), RootImpl.onDDJoin(r, k2n, n)),
                                RootImpl::onDDLeave,
                                RootImpl::onDDRecover,
                                RootImpl::tillTerminated
                            ),
                            "root"
                        );

                        root.tell(new Root.Join(40, 10));

                        probe.expectNoMessage();
                    }
                }
            ),
            dynamicTest(
                "should join w/ >= R non-crashed nodes",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var probe = test.kit().<DidOrDidnt.Join>createTestProbe();

                        final var root = test.kit().spawn(
                            RootImpl.init(
                                new Config(4, 3, 2, Duration.ofSeconds(1L)),
                                IntSets.immutable.with(10, 20, 30, 40),
                                (r, k2n, n) -> Behaviors.monitor(DidOrDidnt.Join.class, probe.ref(), RootImpl.onDDJoin(r, k2n, n)),
                                RootImpl::onDDLeave,
                                RootImpl::onDDRecover,
                                RootImpl::tillTerminated
                            ),
                            "root"
                        );

                        root.tell(new Root.Crash(30, 40));
                        root.tell(new Root.Join(50, 10));

                        probe.expectMessageClass(DidOrDidnt.Join.Did.class);
                    }
                }
            ),
            dynamicTest(
                "cannot join w/ < R non-crashed",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var probe = test.kit().<DidOrDidnt.Join>createTestProbe();

                        final var root = test.kit().spawn(
                            RootImpl.init(
                                new Config(4, 3, 2, Duration.ofSeconds(1L)),
                                IntSets.immutable.with(10, 20, 30, 40),
                                (r, k2n, n) -> Behaviors.monitor(DidOrDidnt.Join.class, probe.ref(), RootImpl.onDDJoin(r, k2n, n)),
                                RootImpl::onDDLeave,
                                RootImpl::onDDRecover,
                                RootImpl::tillTerminated
                            ),
                            "root"
                        );

                        root.tell(new Root.Crash(20, 30, 40));
                        root.tell(new Root.Join(50, 10));

                        probe.expectMessageClass(DidOrDidnt.Join.Didnt.class);
                    }
                }
            ),
            dynamicTest(
                "cannot join w/ crashed 'with' node",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var probe = test.kit().<DidOrDidnt.Join>createTestProbe();

                        final var root = test.kit().spawn(
                            RootImpl.init(
                                new Config(4, 3, 2, Duration.ofSeconds(1L)), IntSets.immutable.with(10, 20, 30, 40),
                                (r, k2n, n) -> Behaviors.monitor(DidOrDidnt.Join.class, probe.ref(), RootImpl.onDDJoin(r, k2n, n)),
                                RootImpl::onDDLeave,
                                RootImpl::onDDRecover,
                                RootImpl::tillTerminated
                            ),
                            "root"
                        );

                        root.tell(new Root.Crash(10));
                        root.tell(new Root.Join(50, 10));

                        probe.expectMessageClass(DidOrDidnt.Join.Didnt.class);
                    }
                }
            )
        );
    }

    @TestFactory
    Stream<DynamicTest> onDDLeave() {
        return Stream.of(
            dynamicTest(
                "cannot leave w/ N nodes",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var probe = test.kit().<DidOrDidnt.Leave>createTestProbe();

                        final var root = test.kit().spawn(
                            RootImpl.init(
                                new Config(4, 3, 2, Duration.ofSeconds(1L)),
                                IntSets.immutable.with(10, 20, 30, 40),
                                RootImpl::onDDJoin,
                                (r, k2n, n, w) -> Behaviors.monitor(DidOrDidnt.Leave.class, probe.ref(), RootImpl.onDDLeave(r, k2n, n, w)),
                                RootImpl::onDDRecover,
                                RootImpl::tillTerminated
                            ),
                            "root"
                        );

                        root.tell(new Root.Leave(10));

                        probe.expectMessageClass(DidOrDidnt.Leave.Didnt.class);
                    }
                }
            ),
            dynamicTest(
                "cannot leave w/ only crashed nodes",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var probe = test.kit().<DidOrDidnt.Leave>createTestProbe();

                        final var root = test.kit().spawn(
                            RootImpl.init(
                                new Config(4, 3, 2, Duration.ofSeconds(1L)),
                                IntSets.immutable.with(10, 20, 30, 40, 50),
                                RootImpl::onDDJoin,
                                (r, k2n, n, w) -> Behaviors.monitor(DidOrDidnt.Leave.class, probe.ref(), RootImpl.onDDLeave(r, k2n, n, w)),
                                RootImpl::onDDRecover,
                                RootImpl::tillTerminated
                            ),
                            "root"
                        );

                        root.tell(new Root.Crash(20, 30, 40, 50));
                        root.tell(new Root.Leave(10));

                        probe.expectMessageClass(DidOrDidnt.Leave.Didnt.class);
                    }
                }
            ),
            dynamicTest(
                "should leave w/ (at least) 1 non-crashed node, w/ empty key2word",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var probe = test.kit().<DidOrDidnt.Leave>createTestProbe();

                        final var root = test.kit().spawn(
                            RootImpl.init(
                                new Config(4, 3, 2, Duration.ofSeconds(1L)),
                                IntSets.immutable.with(10, 20, 30, 40, 50),
                                RootImpl::onDDJoin,
                                (r, k2n, n, w) -> Behaviors.monitor(DidOrDidnt.Leave.class, probe.ref(), RootImpl.onDDLeave(r, k2n, n, w)),
                                RootImpl::onDDRecover,
                                RootImpl::tillTerminated
                            ),
                            "root"
                        );

                        root.tell(new Root.Crash(30, 40));
                        root.tell(new Root.Leave(10));

                        probe.expectMessageClass(DidOrDidnt.Leave.Did.class);
                    }
                }
            ),
            dynamicTest(
                "cannot leave w/ < W non-crashed nodes, w/ non-empty key2word",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var probe = test.kit().<DidOrDidnt.Leave>createTestProbe();

                        final var root = test.kit().spawn(
                            RootImpl.init(
                                new Config(4, 3, 2, Duration.ofSeconds(1L)),
                                IntSets.immutable.with(10, 20, 30, 40),
                                RootImpl::onDDJoin,
                                (r, k2n, n, w) -> Behaviors.monitor(DidOrDidnt.Leave.class, probe.ref(), RootImpl.onDDLeave(r, k2n, n, w)),
                                RootImpl::onDDRecover,
                                RootImpl::tillTerminated
                            ),
                            "root"
                        );

                        root.tell(new Root.Crash(30));
                        root.tell(new Root.Clients(Lists.immutable.with(Lists.immutable.with(new Put(10, 10, "test")))));
                        root.tell(new Root.Join(50, 10));
                        root.tell(new Root.Crash(40));
                        root.tell(new Root.Leave(10));

                        probe.expectMessageClass(DidOrDidnt.Leave.Didnt.class);
                    }
                }
            ),
            dynamicTest(
                "cannot leave w/ >= W non-crashed nodes, but < W for (at least) 1 word, w/ non-empty key2word",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var probe = test.kit().<DidOrDidnt.Leave>createTestProbe();

                        final var root = test.kit().spawn(
                            RootImpl.init(
                                new Config(4, 3, 2, Duration.ofSeconds(1L)),
                                IntSets.immutable.with(10, 20, 30, 40),
                                RootImpl::onDDJoin,
                                (r, k2n, n, w) -> Behaviors.monitor(DidOrDidnt.Leave.class, probe.ref(), RootImpl.onDDLeave(r, k2n, n, w)),
                                RootImpl::onDDRecover,
                                RootImpl::tillTerminated
                            ),
                            "root"
                        );

                        root.tell(new Root.Crash(20));
                        root.tell(new Root.Clients(Lists.immutable.with(Lists.immutable.with(new Put(10, 10, "test")))));
                        root.tell(new Root.Join(50, 10));
                        root.tell(new Root.Join(60, 10));
                        root.tell(new Root.Join(70, 10));
                        root.tell(new Root.Join(80, 10));
                        root.tell(new Root.Crash(30, 40, 50));
                        root.tell(new Root.Leave(10));

                        probe.expectMessageClass(DidOrDidnt.Leave.Didnt.class);
                    }
                }
            ),
            dynamicTest(
                "should leave w/ >= W non-crashed nodes, w/ >= W replacers for each word, w/ non-empty key2word",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var probe = test.kit().<DidOrDidnt.Leave>createTestProbe();

                        final var root = test.kit().spawn(
                            RootImpl.init(
                                new Config(4, 3, 2, Duration.ofSeconds(1L)),
                                IntSets.immutable.with(10, 20, 30, 40),
                                RootImpl::onDDJoin,
                                (r, k2n, n, w) -> Behaviors.monitor(DidOrDidnt.Leave.class, probe.ref(), RootImpl.onDDLeave(r, k2n, n, w)),
                                RootImpl::onDDRecover,
                                RootImpl::tillTerminated
                            ),
                            "root"
                        );

                        root.tell(new Root.Crash(40));
                        root.tell(new Root.Clients(Lists.immutable.with(Lists.immutable.with(new Put(10, 10, "test")))));
                        root.tell(new Root.Join(50, 10));
                        root.tell(new Root.Leave(10));

                        probe.expectMessageClass(DidOrDidnt.Leave.Did.class);
                    }
                }
            )
        );
    }

    @TestFactory
    Stream<DynamicTest> onDDRecover() {
        return Stream.of(
            dynamicTest(
                "cannot recover non-crashed node",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var probe = test.kit().<DidOrDidnt.Recover>createTestProbe();

                        final var root = test.kit().spawn(
                            RootImpl.init(
                                new Config(4, 3, 2, Duration.ofSeconds(1L)),
                                IntSets.immutable.with(10, 20, 30, 40),
                                RootImpl::onDDJoin,
                                RootImpl::onDDLeave,
                                (r, k2n) -> Behaviors.monitor(DidOrDidnt.Recover.class, probe.ref(), RootImpl.onDDRecover(r, k2n)),
                                RootImpl::tillTerminated
                            ),
                            "root"
                        );

                        root.tell(new Root.Recover(10, 20));

                        probe.expectMessageClass(DidOrDidnt.Recover.Didnt.class);
                    }
                }
            ),
            dynamicTest(
                "cannot recover w/ crashed 'with' node",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var probe = test.kit().<DidOrDidnt.Recover>createTestProbe();

                        final var root = test.kit().spawn(
                            RootImpl.init(
                                new Config(4, 3, 2, Duration.ofSeconds(1L)),
                                IntSets.immutable.with(10, 20, 30, 40),
                                RootImpl::onDDJoin,
                                RootImpl::onDDLeave,
                                (r, k2n) -> Behaviors.monitor(DidOrDidnt.Recover.class, probe.ref(), RootImpl.onDDRecover(r, k2n)),
                                RootImpl::tillTerminated
                            ),
                            "root"
                        );

                        root.tell(new Root.Crash(10, 20));
                        root.tell(new Root.Recover(10, 20));

                        probe.expectMessageClass(DidOrDidnt.Recover.Didnt.class);
                    }
                }
            ),
            dynamicTest(
                "should recover w/ (at least) non-crashed 'with' node",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var probe = test.kit().<DidOrDidnt.Recover>createTestProbe();

                        final var root = test.kit().spawn(
                            RootImpl.init(
                                new Config(4, 3, 2, Duration.ofSeconds(1L)),
                                IntSets.immutable.with(10, 20, 30, 40),
                                RootImpl::onDDJoin,
                                RootImpl::onDDLeave,
                                (r, k2n) -> Behaviors.monitor(DidOrDidnt.Recover.class, probe.ref(), RootImpl.onDDRecover(r, k2n)),
                                RootImpl::tillTerminated
                            ),
                            "root"
                        );

                        root.tell(new Root.Crash(10, 30, 40));
                        root.tell(new Root.Recover(10, 20));

                        probe.expectMessageClass(DidOrDidnt.Recover.Did.class);
                    }
                }
            )
        );
    }

    @TestFactory
    Stream<DynamicTest> convert() {
        return Stream.of(
            dynamicTest(
                "get to get",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var node = test.kit().spawn(Node.newbie(5)).<Node.Cmd>narrow();

                        assertEquals(
                            new Client.Get(node.narrow(), 10),
                            RootImpl.convert(IntObjectMaps.immutable.of(5, node), new Get(5, 10))
                        );
                    }
                }
            ),
            dynamicTest(
                "put to put",
                () -> {
                    try (final var test = new ActorTestKitExt(ActorTestKit.create())) {
                        final var node = test.kit().spawn(Node.newbie(5)).<Node.Cmd>narrow();

                        assertEquals(
                            new Client.Put(node.narrow(), 10, Optional.of("")),
                            RootImpl.convert(IntObjectMaps.immutable.of(5, node), new Put(5, 10, Optional.of("")))
                        );
                    }
                }
            ),
            dynamicTest(
                "it doesn't handle missing actors",
                () -> assertThrows(
                    NullPointerException.class,
                    () -> RootImpl.convert(IntObjectMaps.immutable.empty(), new Get(5, 10))
                )
            )
        );
    }

}
