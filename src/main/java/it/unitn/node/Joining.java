package it.unitn.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.Config;
import it.unitn.utils.Logging;
import it.unitn.utils.MBehavior;
import it.unitn.utils.MBehaviors;
import it.unitn.utils.Range;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.SortedMaps;
import org.eclipse.collections.api.factory.SortedSets;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;
import org.eclipse.collections.api.map.sorted.MutableSortedMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.api.tuple.primitive.ObjectIntPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;

import java.time.Duration;
import java.util.TreeMap;

import static it.unitn.node.Node.clusters;
import static it.unitn.node.Node.ranges;
import static it.unitn.utils.Ranging.disjoint;
import static it.unitn.utils.Windowing.windowed;
import static org.eclipse.collections.impl.collector.Collectors2.toImmutableList;

public interface Joining {

    sealed interface Msg {}

    record Res4key2node(Config config, ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node) implements Msg {}

    record Res4key2word(int node, ImmutableSortedMap<Integer, Node.Word> key2word) implements Msg {}

    record Failed(Throwable cause) implements Msg {}

    static Behavior<Msg> init(
        ActorRef<Node.Event> parent,
        int node,
        ActorRef<Node.Ask4key2node> ref
    ) {

        record Init(int node, ActorRef<Node.Ask4key2node> ref) {}

        final var state = new Init(node, ref);

        return Behaviors.setup(ctx -> {

            ctx.ask(
                Res4key2node.class,
                ref,
                Duration.ofSeconds(1L),
                Node.Ask4key2node::new,
                (r, t) -> r != null ? r : new Failed(t)
            );

            return Behaviors.receiveMessage(msg -> Logging.logging(ctx.getLog(), state, msg, switch (msg) {

                case Failed x -> MBehaviors.<Msg>stopped(() -> parent.tell(new Node.DidntJoin(x.cause())));

                case Res4key2node x -> precollecting(parent, x.config, node, x.key2node());

                case Res4key2word ignored -> throw new AssertionError("%s only".formatted(Res4key2word.class));

            }));
        });
    }

    static MBehavior<Msg> precollecting(
        ActorRef<Node.Event> parent,
        Config config,
        int node,
        ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node
    ) {

        // N = 4
        // 10 20 30 40 50 60 70 80 90
        // ^]              (^^^^^^^^^^ 10
        // ^^^^]              (^^^^^^^ 20
        // ^^^^^^^]              (^^^^ 30
        // ^^^^^^^^^^]              (^ 40
        //  (^^^^^^^^^^^]              50
        //     (^^^^^^^^^^^]           60
        //        (^^^^^^^^^^^]        70
        //           (^^^^^^^^^^^]     80
        //              (^^^^^^^^^^^]  90

        // 10 20 30 40 45 50 60 70 80 90
        // ^]                 (^^^^^^^^^^ 10
        // ^^^^]                 (^^^^^^^ 20
        // ^^^^^^^]                 (^^^^ 30
        // ^^^^^^^^^^]                 (^ 40
        //  (***********]                 45 *
        //     (^^^^^^^^^^^]              50
        //        (^^^^^^^^^^^]           60
        //           (^^^^^^^^^^^]        70
        //              (^^^^^^^^^^^]     80
        //                 (^^^^^^^^^^^]  90
        //
        // 10 20 30 40 50 60 70 80 90
        //  ]              (           10
        //   **]              (        20 *
        //   *****]              (     30 *
        //   ********]              (  40 *
        //  (***********]              50 *
        //     (********   ]           60 *
        //        (*****      ]        70 *
        //           (**         ]     80 *
        //              (           ]  90

        // 05 10 20 30 40 50 60 70 80 90
        // *]                 (********** 05 *
        // ^^^^]                 (^^^^^^^ 10
        // ^^^^^^^]                 (^^^^ 20
        // ^^^^^^^^^^]                 (^ 30
        //  (^^^^^^^^^^^]                 40
        //     (^^^^^^^^^^^]              50
        //        (^^^^^^^^^^^]           60
        //           (^^^^^^^^^^^]        70
        //              (^^^^^^^^^^^]     80
        //                 (^^^^^^^^^^^]  90
        //
        // 10 20 30 40 50 60 70 80 90
        // *]              (********** 10 *
        // *   ]              (******* 20 *
        // *      ]              (**** 30 *
        // *         ]              (* 40 *
        //  (           ]              50
        //     (           ]           60
        //        (         **]        70 *
        //           (      *****]     80 *
        //              (   ********]  90 *

        // 10 20 30 40 50 60 70 80 90 95
        // ^]                 (^^^^^^^^^^ 10
        // ^^^^]                 (^^^^^^^ 20
        // ^^^^^^^]                 (^^^^ 30
        // ^^^^^^^^^^]                 (^ 40
        //  (^^^^^^^^^^^]                 50
        //     (^^^^^^^^^^^]              60
        //        (^^^^^^^^^^^]           70
        //           (^^^^^^^^^^^]        80
        //              (^^^^^^^^^^^]     90
        //                 (***********]  95 *
        //
        // 10 20 30 40 50 60 70 80 90
        //  ]              (********** 10 *
        //     ]              (******* 20 *
        //        ]              (**** 30 *
        //           ]              (* 40 *
        //  (           ]              50
        //     (           ]           60
        //        (         **]        70 *
        //           (      *****]     80 *
        //              (   ********]  90 *

        final var keys = SortedSets.immutable.withSortedSet(new TreeMap<>(key2node.castToSortedMap()).navigableKeySet());

        final var cluster =
            clusters(config, keys.newWith(node))
                .filter(xs -> node == xs.getLast())
                .findAny()
                .orElseThrow();

        final var range =
            new Range(cluster.getFirst(), cluster.getLast());

        final var units =
            windowed(2, cluster)
                .map(xs -> new Range(xs.getFirst(), xs.getLast()))
                .collect(toImmutableList());

        final var ranges =
            ranges(config, keys)
                .collect(toImmutableList());

        final var overlapping =
            key2node.keyValuesView()
                .toImmutableList()
                .zip(ranges)
                .reject(p -> disjoint(range, p.getTwo()))
                .toImmutableList();

        final var key2range =
            overlapping.toImmutableMap(p -> p.getOne().getOne(), Pair::getTwo);

        record PreCollecting(int node, Config config, ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node, Range range, ImmutableList<Range> ranges) {}

        final var state = new PreCollecting(node, config, key2node, range, ranges);

        return new MBehavior<>(
            state,
            Behaviors.setup(ctx -> Behaviors.withTimers(timer -> {

                overlapping.forEach(p -> p.getOne().getTwo().tell(new Node.Ask4key2word(ctx.getSelf().narrow(), range.gt(), range.lte())));

                timer.startSingleTimer(new Failed(new AssertionError("not enough key2word")), config.T());

                return Logging.logging(ctx.getLog(), state, collecting(
                    parent,
                    config,
                    key2node,
                    key2range,
                    SortedMaps.mutable.empty(),
                    units.collect(x -> PrimitiveTuples.pair(x, config.R()))
                ));
            }))
        );
    }

    static MBehavior<Msg> collecting(
        ActorRef<Node.Event> parent,
        Config config,
        ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node,
        ImmutableMap<Integer, Range> key2range,
        MutableSortedMap<Integer, Node.Word> key2word,
        ImmutableList<ObjectIntPair<Range>> toCover
    ) {

        if (toCover.isEmpty()) {
            return MBehaviors.stopped(() -> parent.tell(new Node.DidJoin(config, key2node, key2word.toImmutable())));
        }

        record Collecting(MutableSortedMap<Integer, Node.Word> key2word, ImmutableList<ObjectIntPair<Range>> toCover) {}

        final var state = new Collecting(key2word, toCover);

        return new MBehavior<>(
            state,
            Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), state, msg, switch (msg) {

                case Failed x -> MBehaviors.stopped(() -> parent.tell(new Node.DidntJoinSafely(x.cause(), config, key2node, key2word.toImmutable())));

                case Res4key2node ignored -> throw new AssertionError("%s only".formatted(Res4key2word.class));

                case Res4key2word x -> collecting(
                    parent,
                    config,
                    key2node,
                    key2range,
                    x.key2word()
                        .keyValuesView()
                        .reduceInPlace(
                            () -> key2word,
                            (acc, k2w) -> acc.merge(
                                k2w.getOne(),
                                k2w.getTwo(),
                                (a, b) -> Lists.immutable.with(a, b).maxBy(Node.Word::version)
                            )
                        ),
                    toCover
                        .collect(p -> disjoint(p.getOne(), key2range.get(x.node()))
                            ? p
                            : PrimitiveTuples.pair(p.getOne(), p.getTwo() - 1)
                        )
                        .reject(p -> p.getTwo() == 0)
                );
            }))
        );
    }

}
