package it.unitn.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.Config;
import it.unitn.root.DidOrDidnt;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.SortedMaps;
import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.map.ImmutableMapIterable;
import org.eclipse.collections.api.map.primitive.ImmutableIntObjectMap;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.api.tuple.primitive.IntObjectPair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;

import static it.unitn.utils.Comparing.cmp;
import static org.eclipse.collections.impl.collector.Collectors2.toImmutableSortedMap;

public interface Node {

    Word DEFAULT = new Word(Optional.empty(), BigInteger.ZERO);

    record Word(Optional<String> word, BigInteger version) {}

    record State(
        int node,
        Config config,
        ImmutableSortedMap<Integer, ActorRef<Cmd>> key2node,
        ImmutableSortedMap<Integer, Word> key2word,
        ImmutableIntObjectMap<Pair<ImmutableSortedMap<Integer, ActorRef<Writing.Cmd>>, ImmutableList<ActorRef<Reading.DidRead>>>> key2locks,
        ImmutableIntObjectMap<ImmutableList<ActorRef<Void>>> key2writing
    ) {}

    sealed interface Msg {}

    sealed interface Cmd extends Msg {}

    sealed interface Event extends Msg {}

    record Setup(Config config, ImmutableIntObjectMap<ActorRef<Cmd>> key2node) implements Cmd {}

    record Ask4key2node(ActorRef<Joining.Res4key2node> replyTo) implements Cmd {}

    record Crash() implements Cmd {}

    record Recover(ActorRef<DidOrDidnt.Recover> replyTo, ActorRef<Node.Cmd> ref) implements Cmd {}

    record Leave(ActorRef<DidOrDidnt.Leave.Didnt> replyTo) implements Cmd {}

    record DidJoin(Config config, ImmutableSortedMap<Integer, ActorRef<Cmd>> key2node) implements Event {}

    record DidntJoin(Throwable cause) implements Event {}

    record Announce(int node, ActorRef<Cmd> ref) implements Cmd {}

    record AnnounceLeaving(ActorRef<Leaving.Ack> replyTo, int node) implements Cmd {}

    record DidLeave() implements Event {}

    record DidntLeave(Throwable cause) implements Event {}

    record Get(ActorRef<DidOrDidnt.Get> replyTo, int k) implements Cmd {}

    record Put(ActorRef<DidOrDidnt.Put> replyTo, int k, Optional<String> value) implements Cmd {}

    record Lock(ActorRef<Writing.Cmd> replyTo, int k, int node) implements Cmd {}

    record Unlock(int k, int node) implements Cmd {}

    record Write(int node, int k, Word word) implements Cmd {}

    record DidWrite(int k, ActorRef<Void> who) implements Event {}

    record Read(ActorRef<Reading.DidRead> replyTo, int k) implements Cmd {}

    static Behavior<Msg> newbie(int node) {
        return Behaviors.receive((ctx, msg) -> {

            ctx.getLog().info("newbie\n\t%d\n\t%s".formatted(node, msg));

            return switch (msg) {

                case Setup x -> {

                    final var s = new State(
                        node,
                        x.config(),
                        x.key2node().keyValuesView().reduceInPlace(toImmutableSortedMap(IntObjectPair::getOne, IntObjectPair::getTwo)),
                        SortedMaps.immutable.empty(),
                        IntObjectMaps.immutable.empty(),
                        IntObjectMaps.immutable.empty()
                    );

                    yield switch (cmp(x.key2node().size(), x.config().N())) {
                        case LT -> throw new AssertionError("|key2node| >= N");
                        case EQ -> minimal(s);
                        case GT -> redundant(s);
                    };
                }

                default -> Behaviors.stopped(() -> {
                    throw new AssertionError("only %s".formatted(Setup.class.getName()));
                });

            };
        });
    }

    static Behavior<Msg> newbie(ActorRef<DidOrDidnt.Join> replyTo, int node, ActorRef<Cmd> with) {
        return Behaviors.withStash(
            1_000_000,
            stash -> Behaviors.setup(ctx -> {

                if (Objects.equals(with, ctx.getSelf()))
                    throw new AssertionError("cannot join myself...");

                ctx.spawn(Joining.joining(ctx.getSelf().narrow(), with.narrow()), "joining");

                return Behaviors.<Msg>receiveMessage(msg -> {

                    ctx.getLog().info("newbie\n\t%d\n\t%s".formatted(node, msg));

                    return switch (msg) {

                        case DidJoin x -> {
                            x.key2node().forEachValue(ref -> ref.tell(new Announce(node, ctx.getSelf().narrow())));
                            replyTo.tell(new DidOrDidnt.Join.Did(ctx.getSelf().narrow()));

                            final var key2node = x.key2node().newWithKeyValue(node, ctx.getSelf().narrow());
                            yield stash.unstashAll(redundant(new State(node, x.config(), key2node, SortedMaps.immutable.empty(), IntObjectMaps.immutable.empty(), IntObjectMaps.immutable.empty()))); // TODO
                        }

                        case DidntJoin x -> {
                            replyTo.tell(new DidOrDidnt.Join.Didnt(x.cause()));
                            yield Behaviors.stopped();
                        }

                        default -> {
                            stash.stash(msg);
                            yield Behaviors.same();
                        }

                    };
                });
            })
        );
    }

    private static Behavior<Msg> minimal(State s) {
        return Behaviors.receive((ctx, msg) -> {

            ctx.getLog().info("minimal\n\t%s\n\t%s".formatted(s, msg));

            return switch (msg) {

                case Ask4key2node x -> {
                    x.replyTo().tell(new Joining.Res4key2node(s.config(), s.key2node()));
                    yield Behaviors.same();
                }

                case Announce x -> {
                    final var key2node = s.key2node().newWithKeyValue(x.node(), x.ref());
                    yield redundant(new State(s.node(), s.config(), key2node, SortedMaps.immutable.empty(), IntObjectMaps.immutable.empty(), IntObjectMaps.immutable.empty())); // TODO
                }

                case Crash ignored -> crashed(s.node());

                case Recover x -> {
                    x.replyTo().tell(new DidOrDidnt.Recover.Didnt(new AssertionError("not crashed")));
                    yield Behaviors.same();
                }

                case Leave x -> {
                    x.replyTo().tell(new DidOrDidnt.Leave.Didnt(new AssertionError("cannot leave")));
                    yield Behaviors.same();
                }

                case Lock x -> {

                    final var lock = s.key2locks().getIfAbsent(
                        x.k(),
                        () -> Tuples.pair(SortedMaps.immutable.empty(), Lists.immutable.empty())
                    );

                    final var node2ref = lock.getOne();
                    final var queue = lock.getTwo();

                    final int maximum = node2ref.keysView().maxOptional().orElse(0);

                    switch (cmp(x.node(), maximum)) {
                        case EQ -> throw new AssertionError("impossible");
                        case LT -> {
                            // do nothing
                            // ie, put it on hold
                        }
                        case GT -> {
                            // grant lock
                            x.replyTo().tell(new Writing.Ack(
                                ctx.getSelf().narrow(),
                                s.key2word().getOrDefault(x.k(), DEFAULT).version()
                            ));
                        }
                    }

                    yield minimal(
                        new State(
                            s.node(),
                            s.config(),
                            s.key2node(),
                            s.key2word(),
                            s.key2locks()
                                .newWithKeyValue(
                                    x.k(),
                                    Tuples.pair(
                                        node2ref.newWithKeyValue(x.node(), x.replyTo()),
                                        queue
                                    )
                                ),
                            s.key2writing()
                        )
                    );
                }

                case Unlock x -> {

                    // unlock never-locked resource ?
                    // it cannot happen

                    final var lock = s.key2locks().get(x.k());
                    final var node2ref = lock.getOne().newWithoutKey(x.node());
                    final var queue = lock.getTwo();

                    if (node2ref.isEmpty()) {
                        final var word = s.key2word().getOrDefault(x.k(), DEFAULT);
                        queue.forEach(ref -> ref.tell(new Reading.DidRead(word)));
                    }

                    final var key2locks = node2ref.isEmpty()
                        ? s.key2locks().newWithoutKey(x.k())
                        : s.key2locks().newWithKeyValue(x.k(), Tuples.pair(node2ref, queue));

                    yield minimal(
                        new State(
                            s.node(),
                            s.config(),
                            s.key2node(),
                            s.key2word(),
                            key2locks,
                            s.key2writing()
                        )
                    );
                }

                case Write x -> switch (cmp(x.word().version(), s.key2word().getOrDefault(x.k(), DEFAULT).version())) {

                    case EQ -> throw new AssertionError("impossible");

                    case LT -> minimal(s);

                    case GT -> {

                        final var lock = s.key2locks().get(x.k());
                        final var node2ref = lock.getOne();
                        final var queue = lock.getTwo();

                        final var original = x.word().version().subtract(BigInteger.valueOf(x.node()));

                        // treat smaller ones as if they happened before
                        node2ref.keyValuesView()
                            .select(p -> p.getOne() < x.node())
                            .forEach(p -> p.getTwo().tell(new Writing.Skip(original.add(BigInteger.valueOf(p.getOne())))));

                        // it is safe to reply to RD
                        queue.forEach(ref -> ref.tell(new Reading.DidRead(x.word())));

                        yield minimal(
                            new State(
                                s.node(),
                                s.config(),
                                s.key2node(),
                                s.key2word().newWithKeyValue(x.k(), x.word()),
                                s.key2locks(),
                                s.key2writing()
                            )
                        );
                    }

                };

                case Put x -> {

                    final var toWait = s.key2writing().getIfAbsent(x.k(), Lists.immutable::empty);

                    final ActorRef<Void> task = ctx.spawnAnonymous(
                        Writing.writing(x.replyTo(), s.config(), s.node(), x.k(), x.value(), s.key2node(), toWait)
                    ).unsafeUpcast();

                    ctx.watchWith(task, new DidWrite(x.k(), task));

                    yield minimal(
                        new State(
                            s.node(),
                            s.config(),
                            s.key2node(),
                            s.key2word(),
                            s.key2locks(),
                            s.key2writing().newWithKeyValue(x.k(), toWait.newWith(task))
                        )
                    );
                }

                case DidWrite x -> {

                    final var toWait = s.key2writing().get(x.k()).newWithout(x.who());

                    yield minimal(
                        new State(
                            s.node(),
                            s.config(),
                            s.key2node(),
                            s.key2word(),
                            s.key2locks(),
                            toWait.isEmpty()
                                ? s.key2writing().newWithoutKey(x.k())
                                : s.key2writing().newWithKeyValue(x.k(), toWait)
                        )
                    );
                }

                case Get x -> {
                    ctx.spawnAnonymous(Reading.reading(x.replyTo(), s.config(), x.k(), s.key2node()));
                    yield minimal(s);
                }

                case Read x -> {

                    final var lock = s.key2locks().get(x.k());

                    if (lock == null) {
                        final var word = s.key2word().getIfAbsentValue(x.k(), DEFAULT);
                        x.replyTo().tell(new Reading.DidRead(word));
                        yield Behaviors.same();
                    }

                    final var node2ref = lock.getOne();
                    final var queue = lock.getTwo().newWith(x.replyTo());

                    yield minimal(
                        new State(
                            s.node(),
                            s.config(),
                            s.key2node(),
                            s.key2word(),
                            s.key2locks().newWithKeyValue(x.k(), Tuples.pair(node2ref, queue)),
                            s.key2writing()
                        )
                    );
                }

                default -> Behaviors.same();

            };

        });
    }

    private static Behavior<Msg> redundant(State s) {
        return Behaviors.receive((ctx, msg) -> {

            ctx.getLog().info("redundant\n\t%s\n\t%s".formatted(s, msg));

            return switch (msg) {

                case Ask4key2node x -> {
                    x.replyTo().tell(new Joining.Res4key2node(s.config(), s.key2node()));
                    yield Behaviors.same();
                }

                case Announce x -> {
                    final var key2node = s.key2node().newWithKeyValue(x.node(), x.ref());
                    yield redundant(new State(s.node(), s.config(), key2node, SortedMaps.immutable.empty(), IntObjectMaps.immutable.empty(), IntObjectMaps.immutable.empty())); // TODO
                }

                case Crash ignored -> crashed(s.node());

                case Recover x -> {
                    x.replyTo().tell(new DidOrDidnt.Recover.Didnt(new AssertionError("not crashed")));
                    yield Behaviors.same();
                }

                case Leave x -> leaving(x.replyTo(), s);

                case AnnounceLeaving x -> {
                    x.replyTo().tell(new Leaving.Ack());

                    final var key2node = s.key2node().newWithoutKey(x.node());
                    final var newState = new State(s.node(), s.config(), key2node, SortedMaps.immutable.empty(), IntObjectMaps.immutable.empty(), IntObjectMaps.immutable.empty()); // TODO

                    yield key2node.size() == s.config().N()
                        ? minimal(newState)
                        : redundant(newState);
                }

                default -> Behaviors.same();

            };

        });
    }

    private static Behavior<Msg> crashed(int k) {
        return Behaviors.receive((ctx, msg) -> {

            ctx.getLog().info("crashed\n\t%d\n\t%s".formatted(k, msg));

            return switch (msg) {

                case Recover x -> recovering(x.replyTo(), k, x.ref());

                default -> Behaviors.same();

            };
        });
    }

    private static Behavior<Msg> recovering(ActorRef<DidOrDidnt.Recover> replyTo, int node, ActorRef<Cmd> ref) {
        return Behaviors.withStash(
            1_000_000,
            stash -> Behaviors.setup(ctx -> {

                if (Objects.equals(ref, ctx.getSelf()))
                    throw new AssertionError("cannot recover w/ myself...");

                ctx.spawn(Joining.joining(ctx.getSelf().narrow(), ref.narrow()), "recovering");

                return Behaviors.<Msg>receiveMessage(msg -> {

                    ctx.getLog().info("recovering\n\t%d\n\t%s".formatted(node, msg));

                    return switch (msg) {

                        case DidJoin x -> {
                            replyTo.tell(new DidOrDidnt.Recover.Did());

                            final var key2node = x.key2node().newWithKeyValue(node, ctx.getSelf().narrow());
                            final var newState = new State(node, x.config(), key2node, SortedMaps.immutable.empty(), IntObjectMaps.immutable.empty(), IntObjectMaps.immutable.empty()); // TODO

                            yield stash.unstashAll(
                                key2node.size() == x.config().N()
                                    ? minimal(newState)
                                    : redundant(newState)
                            );
                        }

                        case DidntJoin x -> {
                            replyTo.tell(new DidOrDidnt.Recover.Didnt(x.cause()));
                            yield Behaviors.stopped();
                        }

                        default -> {
                            stash.stash(msg);
                            yield Behaviors.same();
                        }

                    };
                });
            })
        );
    }

    private static Behavior<Msg> leaving(ActorRef<DidOrDidnt.Leave.Didnt> replyTo, State s) {
        return Behaviors.withStash(
            1_000_000,
            stash -> Behaviors.setup(ctx -> {

                final ImmutableList<ActorRef<AnnounceLeaving>> nodes =
                    s.key2node()
                        .newWithoutKey(s.node())
                        .collect(ActorRef::<AnnounceLeaving>narrow, Lists.mutable.empty())
                        .toImmutable();

                ctx.spawn(Leaving.leaving(ctx.getSelf().narrow(), s.node(), nodes), "leaving");

                return Behaviors.<Msg>receiveMessage(msg -> {

                    ctx.getLog().info("leaving\n\t%s\n\t%s".formatted(s, msg));

                    return switch (msg) {

                        case DidLeave ignored -> Behaviors.stopped();

                        case DidntLeave x -> {
                            replyTo.tell(new DidOrDidnt.Leave.Didnt(x.cause()));
                            yield s.key2node().size() == s.config().N()
                                ? minimal(s)
                                : redundant(s);
                        }

                        default -> {
                            stash.stash(msg);
                            yield Behaviors.same();
                        }

                    };
                });
            })
        );
    }

    static <K extends Comparable<K>, V> ImmutableList<V> clockwise(ImmutableMapIterable<K, V> key2node, K key) {

        final var partition = key2node
            .keyValuesView()
            .partition(x -> x.getOne().compareTo(key) >= 0);

        return Lists.immutable.with(partition.getSelected(), partition.getRejected())
            .flatCollect(x -> x)
            .collect(Pair::getTwo);
    }

}
