package it.unitn.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.Config;
import it.unitn.root.DidOrDidnt;
import it.unitn.utils.Logging;
import it.unitn.utils.MBehavior;
import it.unitn.utils.MBehaviors;
import it.unitn.utils.Range;
import org.eclipse.collections.api.collection.ImmutableCollection;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.SortedMaps;
import org.eclipse.collections.api.factory.SortedSets;
import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.map.ImmutableMapIterable;
import org.eclipse.collections.api.map.primitive.ImmutableIntObjectMap;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;
import org.eclipse.collections.api.set.sorted.ImmutableSortedSet;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.api.tuple.Triple;
import org.eclipse.collections.api.tuple.primitive.IntObjectPair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static it.unitn.utils.Comparing.cmp;
import static it.unitn.utils.Extracting.extract;
import static it.unitn.utils.Shuffling.shuffle;
import static it.unitn.utils.Windowing.windowed;
import static java.util.stream.Collectors.collectingAndThen;
import static org.eclipse.collections.impl.collector.Collectors2.toImmutableList;
import static org.eclipse.collections.impl.collector.Collectors2.toImmutableSortedMap;

/**
 * {@link Node} actor is responsible for all the protocol operations.
 * <br>
 * Whenever it can, it delegates the operations to on-demand children:
 * <ul>
 *     <li>{@link Joining}</li>
 *     <li>{@link Leaving}</li>
 *     <li>{@link Reading}</li>
 *     <li>{@link Writing}</li>
 * </ul>
 */
public interface Node {

    Word DEFAULT = new Word(Optional.empty(), BigInteger.ZERO);

    record Word(Optional<String> word, BigInteger version) {}

    record State(
        int node,
        Config config,
        ImmutableSortedMap<Integer, ActorRef<Cmd>> key2node,
        ImmutableSortedMap<Integer, Word> key2word,
        ImmutableIntObjectMap<Triple<ActorRef<Writing.Cmd>, ImmutableSortedSet<ActorRef<Writing.Cmd>>, ImmutableList<ActorRef<Reading.DidRead>>>> key2locks
    ) {}

    sealed interface Msg {}

    sealed interface Cmd extends Msg {}

    sealed interface Event extends Msg {}

    sealed interface Common {}

    record Setup(ActorRef<DidOrDidnt.Setup.Did> replyTo, Config config, ImmutableIntObjectMap<ActorRef<Cmd>> key2node) implements Cmd {}

    record Ask4key2node(ActorRef<Joining.Res4key2node> replyTo) implements Cmd, Common {}

    record Ask4key2word(ActorRef<Joining.Res4key2word> replyTo, int gt, int lte) implements Cmd, Common {}

    record Crash() implements Cmd, Common {}

    record Recover(ActorRef<DidOrDidnt.Recover> replyTo, ActorRef<Node.Cmd> ref) implements Cmd, Common {}

    record Leave(ActorRef<DidOrDidnt.Leave.Didnt> replyTo) implements Cmd {}

    record DidJoin(Config config, ImmutableSortedMap<Integer, ActorRef<Cmd>> key2node, ImmutableSortedMap<Integer, Word> key2word) implements Event {}

    record DidntJoin(Throwable cause) implements Event {}

    record DidntJoinSafely(Throwable cause, Config config, ImmutableSortedMap<Integer, ActorRef<Cmd>> key2node, ImmutableSortedMap<Integer, Word> key2word) implements Event {}

    record Announce(int node, ActorRef<Cmd> ref) implements Cmd, Common {}

    record Ping(ActorRef<Leaving.Ack> replyTo) implements Cmd {}

    record AnnounceLeaving(int node, ImmutableSortedMap<Integer, Word> key2word) implements Cmd {}

    record DidLeave() implements Event {}

    record DidntLeave(Throwable cause) implements Event {}

    record Get(ActorRef<DidOrDidnt.Get> replyTo, int k) implements Cmd, Common {}

    record Put(ActorRef<DidOrDidnt.Put> replyTo, int k, Optional<String> value) implements Cmd, Common {}

    record Lock(ActorRef<Writing.Cmd> replyTo, int k) implements Cmd, Common {}

    record Unlock(int k, ActorRef<Writing.Cmd> who) implements Event, Common {}

    record Write(int k, Word word) implements Cmd, Common {}

    record Read(ActorRef<Reading.DidRead> replyTo, int k) implements Cmd, Common {}

    static Behavior<Msg> newbie(int node) {

        record Newbie(int node) {}

        return Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), new Newbie(node), msg, switch (msg) {

            case Setup x -> {

                final var s = new State(
                    node,
                    x.config(),
                    x.key2node().keyValuesView().reduceInPlace(toImmutableSortedMap(IntObjectPair::getOne, IntObjectPair::getTwo)),
                    SortedMaps.immutable.empty(),
                    IntObjectMaps.immutable.empty()
                );

                yield switch (cmp(x.key2node().size(), x.config().N())) {
                    case LT -> throw new AssertionError("|key2node| >= N");
                    case EQ -> {
                        x.replyTo().tell(new DidOrDidnt.Setup.Did(node));
                        yield minimal(s);
                    }
                    case GT -> {
                        x.replyTo().tell(new DidOrDidnt.Setup.Did(node));
                        yield redundant(s);
                    }
                };
            }

            default -> throw new AssertionError("%s only".formatted(Setup.class.getName()));

        }));
    }

    static Behavior<Msg> newbie(ActorRef<DidOrDidnt.Join> replyTo, int node, ActorRef<Cmd> with) {

        record Newbie(ActorRef<DidOrDidnt.Join> replyTo, int node, ActorRef<Cmd> with) {}

        return Behaviors.setup(ctx -> {

            if (Objects.equals(with, ctx.getSelf()))
                throw new AssertionError("cannot join myself...");

            ctx.spawn(Joining.init(ctx.getSelf().narrow(), node, with.narrow()), "joining");

            return Behaviors.receiveMessage(msg -> Logging.logging(ctx.getLog(), new Newbie(replyTo, node, with), msg, switch (msg) {

                case DidJoin x -> {
                    x.key2node().forEachValue(ref -> ref.tell(new Announce(node, ctx.getSelf().narrow())));
                    replyTo.tell(new DidOrDidnt.Join.Did(ctx.getSelf().narrow()));

                    yield redundant(new State(
                        node,
                        x.config(),
                        x.key2node().newWithKeyValue(node, ctx.getSelf().narrow()),
                        x.key2word(),
                        IntObjectMaps.immutable.empty()
                    ));
                }

                case DidntJoin x -> MBehaviors.<Msg>stopped(() -> replyTo.tell(new DidOrDidnt.Join.Didnt(x.cause())));

                case DidntJoinSafely x -> MBehaviors.<Msg>stopped(() -> replyTo.tell(new DidOrDidnt.Join.Didnt(x.cause())));

                default -> throw new AssertionError("%s only".formatted(Lists.immutable.of(DidJoin.class, DidntJoin.class, DidntJoinSafely.class).collect(Class::getName)));

            }));

        });
    }

    private static MBehavior<Msg> minimal(State s) {

        record Minimal(State s) {}

        final var state = new Minimal(s);

        return new MBehavior<>(
            state,
            Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), state, msg, switch (msg) {

                case Leave x -> {
                    x.replyTo().tell(new DidOrDidnt.Leave.Didnt(new AssertionError("cannot leave")));
                    yield minimal(s);
                }

                case Common x -> common(s, ctx, x, Node::minimal);

                default -> minimal(s);

            }))
        );
    }

    private static MBehavior<Msg> redundant(State s) {

        record Redundant(State s) {}

        final var state = new Redundant(s);

        return new MBehavior<>(
            state,
            Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), state, msg, switch (msg) {

                case Leave x -> leaving(x.replyTo(), s);

                case Ping x -> {
                    x.replyTo().tell(new Leaving.Ack(ctx.getSelf().narrow()));
                    yield redundant(s);
                }

                case AnnounceLeaving x -> {
                    final var key2node = s.key2node().newWithoutKey(x.node());
                    final var key2word = x.key2word().keyValuesView()
                        .reduceInPlace(
                            s.key2word()::toSortedMap,
                            (acc, p) -> acc.merge(p.getOne(), p.getTwo(), (a, b) -> Lists.immutable.of(a, b).maxBy(Word::version))
                        )
                        .toImmutable();

                    final var newState = new State(
                        s.node(),
                        s.config(),
                        key2node,
                        key2word,
                        IntObjectMaps.immutable.empty()
                    );

                    yield key2node.size() == s.config().N()
                        ? minimal(newState)
                        : redundant(newState);
                }

                case Common x -> common(s, ctx, x, Node::redundant);

                default -> redundant(s);

            }))
        );
    }

    private static MBehavior<Msg> common(
        State s,
        ActorContext<Msg> ctx,
        Common msg,
        Function<State, MBehavior<Msg>> same
    ) {
        return switch (msg) {

            case Ask4key2node x -> {
                x.replyTo().tell(new Joining.Res4key2node(s.config(), s.key2node()));
                yield same.apply(s);
            }

            case Ask4key2word x -> {
                x.replyTo().tell(new Joining.Res4key2word(s.node(), extract(new TreeMap<>(s.key2word().castToSortedMap()), x.gt(), x.lte())));
                yield same.apply(s);
            }

            case Announce x -> {
                final var key2node = s.key2node().newWithKeyValue(x.node(), x.ref());
                final var range =
                    ranges(s.config(), SortedSets.immutable.ofSortedSet(new TreeMap<>(key2node.castToSortedMap()).navigableKeySet()))
                        .filter(p -> s.node() == p.lte())
                        .findAny()
                        .orElseThrow();

                yield redundant(new State(
                    s.node(),
                    s.config(),
                    key2node,
                    extract(new TreeMap<>(s.key2word().castToSortedMap()), range.gt(), range.lte()),
                    IntObjectMaps.immutable.empty()
                ));
            }

            case Crash ignored -> crashed(s.node(), s.key2word());

            case Recover x -> {
                x.replyTo().tell(new DidOrDidnt.Recover.Didnt(new AssertionError("not crashed")));
                yield same.apply(s);
            }

            case Lock x -> {

                // if there are no pending wrs
                // then, grant the lock
                // otherwise, put it on hold, ie, do nothing

                ctx.watchWith(x.replyTo(), new Unlock(x.k(), x.replyTo()));

                final Triple<ActorRef<Writing.Cmd>, ImmutableSortedSet<ActorRef<Writing.Cmd>>, ImmutableList<ActorRef<Reading.DidRead>>> newLock =
                    Optional.ofNullable(s.key2locks().get(x.k()))
                        .map((lock) -> {
                            final var cur = lock.getOne();
                            final var wrs = lock.getTwo();
                            final var rds = lock.getThree();

                            return Tuples.triple(cur, wrs.newWith(x.replyTo()), rds);
                        })
                        .orElseGet(() -> {
                            x.replyTo().tell(new Writing.Ack(
                                ctx.getSelf().narrow(),
                                s.key2word().getOrDefault(x.k(), DEFAULT).version()
                            ));

                            return Tuples.triple(x.replyTo(), SortedSets.immutable.empty(), Lists.immutable.empty());
                        });

                yield same.apply(
                    new State(
                        s.node(),
                        s.config(),
                        s.key2node(),
                        s.key2word(),
                        s.key2locks().newWithKeyValue(x.k(), newLock)
                    )
                );
            }

            case Unlock x -> {

                final var lock = s.key2locks().get(x.k());
                final var cur = lock.getOne();
                final var wrs = lock.getTwo();
                final var rds = lock.getThree();

                if (!Objects.equals(cur, x.who())) {

                    yield same.apply(
                        new State(
                            s.node(),
                            s.config(),
                            s.key2node(),
                            s.key2word(),
                            s.key2locks().newWithKeyValue(x.k(), Tuples.triple(cur, wrs.newWithout(x.who()), rds))
                        )
                    );
                }

                if (wrs.isEmpty()) {

                    final var word = s.key2word().getOrDefault(x.k(), DEFAULT);

                    rds.forEach(ref -> ref.tell(new Reading.DidRead(word)));

                    yield same.apply(
                        new State(
                            s.node(),
                            s.config(),
                            s.key2node(),
                            s.key2word(),
                            s.key2locks().newWithoutKey(x.k())
                        )
                    );
                }

                final var min = wrs.getFirst();

                min.tell(new Writing.Ack(
                    ctx.getSelf().narrow(),
                    s.key2word().getOrDefault(x.k(), DEFAULT).version()
                ));

                yield same.apply(
                    new State(
                        s.node(),
                        s.config(),
                        s.key2node(),
                        s.key2word(),
                        s.key2locks().newWithKeyValue(
                            x.k(),
                            Tuples.triple(min, wrs.newWithout(min), rds)
                        )
                    )
                );
            }

            case Write x -> switch (cmp(x.word().version(), s.key2word().getOrDefault(x.k(), DEFAULT).version())) {

                case LT, EQ -> throw new AssertionError("impossible: [%s] , [%s]".formatted(s, x));

                case GT -> {

                    final var lock = s.key2locks().get(x.k());
                    final var cur = lock.getOne();
                    final var wrs = lock.getTwo();
                    final var rds = lock.getThree();

                    // it is safe to reply to RD
                    rds.forEach(rd -> rd.tell(new Reading.DidRead(x.word())));

                    yield same.apply(
                        new State(
                            s.node(),
                            s.config(),
                            s.key2node(),
                            s.key2word().newWithKeyValue(x.k(), x.word()),
                            s.key2locks().newWithKeyValue(x.k(), Tuples.triple(cur, wrs, Lists.immutable.empty()))
                        )
                    );
                }

            };

            case Put x -> {
                ctx.spawnAnonymous(Writing.init(x.replyTo(), s.config(), s.node(), x.k(), x.value(), s.key2node()));
                yield same.apply(s);
            }

            case Get x -> {
                ctx.spawnAnonymous(Reading.init(x.replyTo(), s.config(), s.node(), x.k(), s.key2node()));
                yield same.apply(s);
            }

            case Read x -> {

                final var lock = s.key2locks().get(x.k());

                if (lock == null) {
                    final var word = s.key2word().getOrDefault(x.k(), DEFAULT);
                    x.replyTo().tell(new Reading.DidRead(word));
                    yield same.apply(s);
                }

                final var cur = lock.getOne();
                final var wrs = lock.getTwo();
                final var rds = lock.getThree().newWith(x.replyTo());

                yield same.apply(
                    new State(
                        s.node(),
                        s.config(),
                        s.key2node(),
                        s.key2word(),
                        s.key2locks().newWithKeyValue(x.k(), Tuples.triple(cur, wrs, rds))
                    )
                );
            }

        };
    }

    private static MBehavior<Msg> crashed(int node, ImmutableSortedMap<Integer, Word> key2word) {

        record Crashed(int node, ImmutableSortedMap<Integer, Word> key2word) {}

        final var state = new Crashed(node, key2word);

        return new MBehavior<>(
            state,
            Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), state, msg, switch (msg) {

                case Recover x -> prerecovering(x.replyTo(), node, x.ref(), key2word);

                default -> crashed(node, key2word);

            }))
        );
    }

    private static MBehavior<Msg> prerecovering(ActorRef<DidOrDidnt.Recover> replyTo, int node, ActorRef<Cmd> ref, ImmutableSortedMap<Integer, Word> key2word) {

        record PreRecovering(ActorRef<DidOrDidnt.Recover> replyTo, int node, ActorRef<Cmd> ref, ImmutableSortedMap<Integer, Word> key2word) {}

        final var state = new PreRecovering(replyTo, node, ref, key2word);

        return new MBehavior<>(
            state,
            Behaviors.setup(ctx -> {

                if (Objects.equals(ref, ctx.getSelf()))
                    throw new AssertionError("cannot recover w/ myself...");

                ctx.spawn(Joining.init(ctx.getSelf().narrow(), node, ref.narrow()), "recovering");

                return Logging.logging(ctx.getLog(), state, recovering(replyTo, node, key2word));

            })
        );
    }

    private static MBehavior<Msg> recovering(ActorRef<DidOrDidnt.Recover> replyTo, int node, ImmutableSortedMap<Integer, Word> key2word) {

        record Recovering(ActorRef<DidOrDidnt.Recover> replyTo, int node, ImmutableSortedMap<Integer, Word> key2word) {}

        final var state = new Recovering(replyTo, node, key2word);

        return new MBehavior<>(
            state,
            Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), state, msg, switch (msg) {

                case DidJoin x -> recover(replyTo, node, x.config(), x.key2node(), x.key2word());

                case DidntJoinSafely x -> recover(replyTo, node, x.config(), x.key2node(), x.key2word());

                case DidntJoin x -> {
                    replyTo.tell(new DidOrDidnt.Recover.Didnt(x.cause()));
                    yield crashed(node, key2word);
                }

                case Ask4key2word x -> {
                    x.replyTo().tell(new Joining.Res4key2word(node, extract(new TreeMap<>(key2word.castToSortedMap()), x.gt(), x.lte())));
                    yield recovering(replyTo, node, key2word);
                }

                default -> throw new AssertionError("%s only".formatted(Lists.immutable.of(DidJoin.class, DidntJoin.class, DidntJoinSafely.class, Ask4key2node.class).collect(Class::getName)));

            }))
        );
    }

    private static MBehavior<Msg> leaving(ActorRef<DidOrDidnt.Leave.Didnt> replyTo, State s) {

        record Leaving(ActorRef<DidOrDidnt.Leave.Didnt> replyTo, State s) {}

        final var state = new Leaving(replyTo, s);

        return new MBehavior<>(
            state,
            Behaviors.setup(ctx -> {

                ctx.spawn(it.unitn.node.Leaving.init(ctx.getSelf().narrow(), s.config(), s.node(), s.key2node(), s.key2word()), "leaving");

                return Behaviors.receiveMessage(msg -> Logging.logging(ctx.getLog(), state, msg, switch (msg) {

                    case DidLeave ignored -> MBehaviors.<Msg>stopped();

                    case DidntLeave x -> {
                        replyTo.tell(new DidOrDidnt.Leave.Didnt(x.cause()));
                        yield s.key2node().size() == s.config().N()
                            ? minimal(s)
                            : redundant(s);
                    }

                    default -> throw new AssertionError("%s only".formatted(Lists.immutable.of(DidLeave.class, DidntLeave.class).collect(Class::getName)));

                }));
            })
        );
    }

    private static MBehavior<Msg> recover(
        ActorRef<DidOrDidnt.Recover> replyTo,
        int node,
        Config config,
        ImmutableSortedMap<Integer, ActorRef<Cmd>> key2node,
        ImmutableSortedMap<Integer, Word> words
    ) {
        replyTo.tell(new DidOrDidnt.Recover.Did());

        final var newState = new State(
            node,
            config,
            key2node,
            words,
            IntObjectMaps.immutable.empty()
        );

        return key2node.size() == config.N()
            ? minimal(newState)
            : redundant(newState);
    }

    static <V> ImmutableList<V> stakeholdersByPriority(Config config, int priority, ImmutableSortedMap<Integer, V> key2value, int key) {
        final var partition = Node.clockwise(key2value, key).take(config.N()).partition(entry -> entry.getOne() == priority);
        return Lists.immutable.with(partition.getSelected(), shuffle(partition.getRejected(), priority))
            .flatCollect(x -> x)
            .collect(Pair::getTwo);
    }

    static <K extends Comparable<K>, V> ImmutableList<Pair<K, V>> clockwise(ImmutableMapIterable<K, V> key2value, K key) {

        final var partition = key2value
            .keyValuesView()
            .partition(x -> x.getOne().compareTo(key) >= 0);

        return Lists.immutable.with(partition.getSelected(), partition.getRejected())
            .flatCollect(x -> x);
    }

    static Stream<Range> ranges(Config config, ImmutableSortedSet<Integer> keys) {
        return clusters(config, keys).map(xs -> new Range(xs.getFirst(), xs.getLast()));
    }

    static Stream<ImmutableList<Integer>> clusters(Config config, ImmutableSortedSet<Integer> keys) {

        // N = 4
        // 10 20 30 40 50

        // [  N - 1  ]
        // 20 30 40 50 10 20 30 40 50
        //  (^^^^^^^^^^^]
        //     (^^^^^^^^^^^]
        //        (^^^^^^^^^^^]
        //           (^^^^^^^^^^^]
        //              (^^^^^^^^^^^]

        return Stream.of(
                keys.drop(keys.size() - config.N()),
                keys
            )
            .flatMap(ImmutableCollection::stream)
            .collect(collectingAndThen(toImmutableList(), xs -> windowed(config.N() + 1, xs)));
    }

}
