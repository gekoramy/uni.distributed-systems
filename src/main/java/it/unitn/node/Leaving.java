package it.unitn.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.Config;
import org.eclipse.collections.api.bag.primitive.MutableIntBag;
import org.eclipse.collections.api.factory.SortedSets;
import org.eclipse.collections.api.factory.primitive.IntBags;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.factory.SortedMaps;

import java.util.TreeMap;

import static it.unitn.node.Node.ranges;
import static it.unitn.utils.Extracting.extract;
import static org.eclipse.collections.impl.collector.Collectors2.toImmutableList;

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
        // ^]        (^^^^^^^^^ 10
        // ^^^^]        (^^^^^^ 20
        // ^^^^^^^]        (^^^ 30
        // ^^^^^^^^^^]        ( 40
        //  (^^^^^^^^^^^]       50
        //     (^^^^^^^^^^^]    60
        //        (^^^^^^^^^^^] 70

        final var tree = new TreeMap<>(key2word.castToSortedMap());

        final var extracts =
            ranges(config, SortedSets.immutable.withSortedSet(new TreeMap<>(toContact.castToSortedMap()).navigableKeySet()))
                .map(r -> extract(tree, r.gt(), r.lte()))
                .collect(toImmutableList());

        final ImmutableMap<ActorRef<Node.Cmd>, ImmutableSortedMap<Integer, Node.Word>> node2words =
            toContact.valuesView()
                .toImmutableList()
                .zip(extracts)
                .toImmutableMap(Pair::getOne, Pair::getTwo);

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

            return tillCovered(
                parent,
                node,
                node2words,
                key2word.keysView().reduceInPlace(IntBags.mutable::empty, (acc, k) -> acc.addOccurrences(k, config.W()))
            );
        }));
    }

    private static Behavior<Msg> tillCovered(
        ActorRef<Node.Event> parent,
        int node,
        ImmutableMap<ActorRef<Node.Cmd>, ImmutableSortedMap<Integer, Node.Word>> node2words,
        MutableIntBag toCover
    ) {

        if (toCover.isEmpty()) {
            return Behaviors.stopped(() -> {
                node2words
                    .keyValuesView()
                    .forEach(p -> p.getOne().tell(new Node.AnnounceLeaving(node, p.getTwo())));

                parent.tell(new Node.DidLeave());
            });
        }

        return Behaviors.receive((ctx, msg) -> {

            ctx.getLog().debug("%s %s".formatted(toCover, msg));

            return switch (msg) {

                case Failed x -> Behaviors.stopped(() -> parent.tell(new Node.DidntLeave(x.cause())));

                case Ack x -> tillCovered(
                    parent,
                    node,
                    node2words,
                    node2words.get(x.who())
                        .keysView()
                        .reduceInPlace(() -> toCover, (acc, k) -> acc.removeOccurrences(k, 1))
                );

            };
        });
    }

}
