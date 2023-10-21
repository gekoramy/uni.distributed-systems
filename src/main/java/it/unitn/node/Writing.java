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

import java.math.BigInteger;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static it.unitn.node.Node.DEFAULT;
import static it.unitn.node.Node.stakeholdersByPriority;

/**
 * {@link Writing} actor is responsible - on behalf of the coordinator {@link Node} - for the {@link Node.Put} operation.
 * <br>
 * It will directly inform the client with one of [{@link DidOrDidnt.Put.Did}, {@link DidOrDidnt.Put.Didnt}].
 */
public interface Writing {

    sealed interface Msg {}

    sealed interface Cmd extends Msg {}

    sealed interface Event extends Msg {}

    record Ack(ActorRef<Node.Write> replyTo, BigInteger version) implements Cmd {}

    record Skip(BigInteger version) implements Cmd {}

    record Failed(Throwable cause) implements Event {}

    static Behavior<Msg> init(
        ActorRef<DidOrDidnt.Put> replyTo,
        Config config,
        int node,
        int key,
        Optional<String> value,
        ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node
    ) {

        final var toLock = stakeholdersByPriority(config, node, key2node, key);

        record Init(ActorRef<DidOrDidnt.Put> replyTo, Config config, int node, int key, Optional<String> value, ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node, ImmutableList<ActorRef<Node.Cmd>> toLock) {}

        return Behaviors.withTimers(timer -> Behaviors.setup(ctx -> {
            timer.startSingleTimer(new Failed(new TimeoutException()), config.T());
            toLock.forEach(ref -> ref.tell(new Node.Lock(node, ctx.getSelf().narrow(), key)));
            return Logging.logging(ctx.getLog(), new Init(replyTo, config, node, key, value, key2node, toLock), collecting(replyTo, config, node, key, value, Lists.immutable.empty(), DEFAULT.version()));
        }));
    }

    private static MBehavior<Msg> collecting(
        ActorRef<DidOrDidnt.Put> replyTo,
        Config config,
        int node,
        int key,
        Optional<String> value,
        ImmutableList<ActorRef<Node.Write>> locked,
        BigInteger version
    ) {

        if (locked.size() == config.W()) {
            return MBehaviors.stopped(ctx -> {
                final var v = version.add(BigInteger.valueOf(node));
                locked.forEach(ref -> ref.tell(new Node.Write(node, ctx.getSelf().narrow(), key, new Node.Word(value, v))));
                replyTo.tell(new DidOrDidnt.Put.Did(v));
            });
        }

        record Collecting(int node, int key, Optional<String> value, ImmutableList<ActorRef<Node.Write>> locked, BigInteger version) {}

        final var state = new Collecting(node, key, value, locked, version);

        return new MBehavior<>(
            state,
            Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), state, msg, switch (msg) {
                case Ack x -> collecting(replyTo, config, node, key, value, locked.newWith(x.replyTo()), version.max(x.version()));
                case Skip x -> MBehaviors.stopped(() -> replyTo.tell(new DidOrDidnt.Put.Did(x.version().add(BigInteger.valueOf(node)))));
                case Failed x -> MBehaviors.stopped(() -> replyTo.tell(new DidOrDidnt.Put.Didnt(x.cause())));
            }))
        );
    }
}
