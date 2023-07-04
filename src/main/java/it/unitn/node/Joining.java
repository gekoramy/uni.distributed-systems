package it.unitn.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.Config;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;

import java.time.Duration;

public interface Joining {

    sealed interface Msg {}

    record Res4key2node(Config config, ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node) implements Msg {}

    record Failed(Throwable cause) implements Msg {}

    static Behavior<Msg> joining(
        ActorRef<Node.Event> parent,
        ActorRef<Node.Ask4key2node> ref
    ) {
        return Behaviors.setup(ctx -> {

            ctx.ask(
                Res4key2node.class,
                ref,
                Duration.ofSeconds(1L),
                Node.Ask4key2node::new,
                (r, t) -> r != null ? r : new Failed(t)
            );

            return Behaviors.<Msg>receiveMessage(msg -> switch (msg) {

                case Res4key2node x -> {
                    parent.tell(new Node.DidJoin(x.config(), x.key2node()));
                    yield Behaviors.stopped();
                }

                case Failed x -> {
                    parent.tell(new Node.DidntJoin(x.cause()));
                    yield Behaviors.stopped();
                }

            });

        });
    }

}
