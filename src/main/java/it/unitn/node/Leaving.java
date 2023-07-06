package it.unitn.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.Config;
import org.eclipse.collections.api.collection.ImmutableCollection;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;
import org.eclipse.collections.api.set.primitive.ImmutableIntSet;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.factory.SortedMaps;

import java.util.TreeMap;
import java.util.stream.Stream;

import static it.unitn.utils.Comparing.cmp;
import static it.unitn.utils.Windowing.windowed;
import static java.util.stream.Collectors.collectingAndThen;
import static org.eclipse.collections.impl.collector.Collectors2.toImmutableList;
import static org.eclipse.collections.impl.collector.Collectors2.toImmutableMap;

public interface Leaving {

    sealed interface Msg {}

    record Ack(ActorRef<Node.Cmd> who) implements Msg {}

    record Failed(Throwable cause) implements Msg {}

    static Behavior<Msg> leaving(
        ActorRef<Node.Event> parent,
        Config config,
        int node,
        ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node,
        ImmutableSortedMap<Integer, Node.Word> key2word
    ) {

        final var toContact = key2node.newWithoutKey(node);

        if (key2word.isEmpty()) {
            return Behaviors.setup(ctx -> Behaviors.withTimers(timer -> {

                toContact.forEach(ref -> ref.tell(new Node.Ping(ctx.getSelf().narrow())));

                timer.startSingleTimer(
                    new Failed(new AssertionError("no enough ack")),
                    config.T()
                );

                return Behaviors.receiveMessage(msg -> Behaviors.stopped(() -> {
                    switch (msg) {
                        case Failed x -> parent.tell(new Node.DidntLeave(x.cause()));
                        case Ack ignored -> {
                            toContact.forEach(ref -> ref.tell(new Node.AnnounceLeaving(node, SortedMaps.immutable.empty())));
                            parent.tell(new Node.DidLeave());
                        }
                    }
                }));
            }));
        }

        // N = 4
        // 10 20 30 40 50 60 70 80
        //                      XX
        // ^]       (^ ^^ ^^ ^^ 10
        // ^^ ^]       (^ ^^ ^^ 20
        // ^^ ^^ ^]       (^ ^^ 30
        // ^^ ^^ ^^ ^]       (^ 40
        // (^ ^^ ^^ ^^ ^]       50
        //    (^ ^^ ^^ ^^ ^]    60
        //       (^ ^^ ^^ ^^ ^] 70

        final var keys = toContact.keysView().toImmutableList();
        final var tree = new TreeMap<>(key2word.castToSortedMap());

        final var extracts =
            Stream.of(
                    keys.subList(keys.size() - config.N(), keys.size()),
                    keys
                )
                .flatMap(ImmutableCollection::stream)
                .collect(collectingAndThen(toImmutableList(), xs -> windowed(config.N() + 1, xs)))
                .limit(keys.size())
                .map(xs -> extract(tree, xs.getFirst(), xs.getLast()))
                .collect(toImmutableList());

        final ImmutableMap<ActorRef<Node.Cmd>, ImmutableSortedMap<Integer, Node.Word>> node2words =
            toContact.valuesView()
                .toImmutableList()
                .zip(extracts)
                .reduceInPlace(toImmutableMap(Pair::getOne, Pair::getTwo));

        return Behaviors.setup(ctx -> Behaviors.withTimers(timer -> {

            ctx.getLog().trace(extracts.toString());

            node2words
                .keyValuesView()
                .reject(p -> p.getTwo().isEmpty())
                .forEach(p -> p.getOne().tell(new Node.Ping(ctx.getSelf().narrow())));

            timer.startSingleTimer(
                new Failed(new AssertionError("no enough ack")),
                config.T()
            );

            return tillCovered(parent, node, node2words, IntSets.immutable.withAll(key2word.keysView()));

        }));
    }

    private static Behavior<Msg> tillCovered(
        ActorRef<Node.Event> parent,
        int node,
        ImmutableMap<ActorRef<Node.Cmd>, ImmutableSortedMap<Integer, Node.Word>> node2words,
        ImmutableIntSet toCover
    ) {

        if (toCover.isEmpty()) {
            return Behaviors.stopped(() -> {
                node2words
                    .keyValuesView()
                    .forEach(p -> p.getOne().tell(new Node.AnnounceLeaving(node, p.getTwo())));

                parent.tell(new Node.DidLeave());
            });
        }

        return Behaviors.receiveMessage(msg -> switch (msg) {

            case Failed x -> Behaviors.stopped(() -> parent.tell(new Node.DidntLeave(x.cause())));

            case Ack x -> tillCovered(
                parent,
                node,
                node2words,
                toCover.newWithoutAll(node2words.get(x.who()).keysView().collectInt(Integer::intValue))
            );

        });
    }

    private static <V> ImmutableSortedMap<Integer, V> extract(TreeMap<Integer, V> from, int start, int to) {
        return switch (cmp(start, to)) {
            case LT -> SortedMaps.immutable.withSortedMap(from.subMap(start, false, to, true));
            case EQ -> SortedMaps.immutable.empty();
            case GT -> Lists.immutable.with(
                    from.tailMap(start, false),
                    from.headMap(to, true)
                )
                .collect(SortedMaps.immutable::ofSortedMap)
                .reduce(ImmutableSortedMap::newWithMapIterable)
                .orElseThrow();
        };
    }

}
