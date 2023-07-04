package it.unitn.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.Config;
import it.unitn.root.DidOrDidnt;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;

import java.util.concurrent.TimeoutException;

import static it.unitn.node.Node.DEFAULT;

public interface Reading {

    sealed interface Msg {}

    record DidRead(Node.Word word) implements Msg {}

    record Failed(Throwable cause) implements Msg {}

    static Behavior<Msg> reading(
        ActorRef<DidOrDidnt.Get> replyTo,
        Config config,
        int key,
        ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node
    ) {

        final var toAsk = Node.clockwise(key2node, key).take(config.N());

        return Behaviors.withTimers(timer -> Behaviors.setup(ctx -> {
            timer.startSingleTimer(new Failed(new TimeoutException()), config.T());
            toAsk.forEach(ref -> ref.tell(new Node.Read(ctx.getSelf().narrow(), key)));
            return collecting(replyTo, config, 0, DEFAULT);
        }));
    }

    private static Behavior<Msg> collecting(
        ActorRef<DidOrDidnt.Get> replyTo,
        Config config,
        int heard,
        Node.Word word
    ) {

        if (heard == config.R()) {
            return Behaviors.stopped(() -> replyTo.tell(new DidOrDidnt.Get.Did(word)));
        }

        return Behaviors.receiveMessage(msg -> switch (msg) {

            case DidRead x -> collecting(
                replyTo,
                config,
                heard + 1,
                Lists.immutable.with(word, x.word()).maxBy(Node.Word::version)
            );

            case Failed x -> Behaviors.stopped(() -> replyTo.tell(new DidOrDidnt.Get.Didnt(x.cause())));

        });
    }

}
