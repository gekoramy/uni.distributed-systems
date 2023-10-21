package it.unitn.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.Config;
import it.unitn.utils.Logging;
import it.unitn.utils.MBehaviors;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;

import java.time.Duration;

/**
 *  {@code Recovering} actor ensures that the parent node recovers with up-to-date network topology.
 *  <br>
 *  It will inform the parent node with one of [{@link Node.DidRecover}, {@link Node.DidntRecover}].
 */
public interface Recovering {

    sealed interface Msg {}

    record Res4key2node(Config config, ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node) implements Msg {}

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
                ignore -> new Node.Ask4key2node(ctx.messageAdapter(Node.Res4key2node.class, r -> new Res4key2node(r.config(), r.key2node()))),
                (r, t) -> r != null ? r : new Failed(t)
            );

            return Behaviors.receiveMessage(msg -> Logging.logging(ctx.getLog(), state, msg, MBehaviors.stopped(() -> parent.tell(switch (msg) {

                case Failed x -> new Node.DidntRecover(x.cause());

                case Res4key2node x -> new Node.DidRecover(x.config(), x.key2node());

            }))));
        });
    }

}
