package it.unitn.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.Config;
import it.unitn.root.DidOrDidnt;
import it.unitn.utils.Logging;
import it.unitn.utils.MBehavior;
import it.unitn.utils.MBehaviors;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;

import java.util.concurrent.TimeoutException;

import static it.unitn.node.Node.DEFAULT;

public interface Reading {

    sealed interface Msg {}

    record DidRead(Node.Word word) implements Msg {}

    record Failed(Throwable cause) implements Msg {}

    static Behavior<Msg> init(
        ActorRef<DidOrDidnt.Get> replyTo,
        Config config,
        int key,
        ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node
    ) {

        final var toAsk = Node.clockwise(key2node, key).take(config.N());

        record Init(ActorRef<DidOrDidnt.Get> replyTo, Config config, int key, ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node, ImmutableList<ActorRef<Node.Cmd>> toAsk) {}

        return Behaviors.withTimers(timer -> Behaviors.setup(ctx -> {
            timer.startSingleTimer(new Failed(new TimeoutException()), config.T());
            toAsk.forEach(ref -> ref.tell(new Node.Read(ctx.getSelf().narrow(), key)));
            return Logging.logging(ctx.getLog(), new Init(replyTo, config, key, key2node, toAsk), collecting(replyTo, config, 0, DEFAULT));
        }));
    }

    private static MBehavior<Msg> collecting(
        ActorRef<DidOrDidnt.Get> replyTo,
        Config config,
        int heard,
        Node.Word word
    ) {

        if (heard == config.R()) {
            return MBehaviors.stopped(() -> replyTo.tell(new DidOrDidnt.Get.Did(word)));
        }

        record Collecting(int heard, Node.Word word) {}

        final var state = new Collecting(heard, word);

        return new MBehavior<>(
            state,
            Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), state, msg, switch (msg) {

                case DidRead x -> collecting(
                    replyTo,
                    config,
                    heard + 1,
                    Lists.immutable.with(word, x.word()).maxBy(Node.Word::version)
                );

                case Failed x -> MBehaviors.stopped(() -> replyTo.tell(new DidOrDidnt.Get.Didnt(x.cause())));

            }))
        );
    }

}
