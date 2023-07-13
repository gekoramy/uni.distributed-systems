package it.unitn.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
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
        ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node,
        ImmutableList<ActorRef<Void>> toWait
    ) {

        final var toLock = Node.clockwise(key2node, key).take(config.N());

        record Init(ActorRef<DidOrDidnt.Put> replyTo, Config config, int node, int key, Optional<String> value, ImmutableSortedMap<Integer, ActorRef<Node.Cmd>> key2node, ImmutableList<ActorRef<Void>> toWait, ImmutableList<ActorRef<Node.Cmd>> toLock) {}

        return Behaviors.withTimers(timer -> Behaviors.setup(ctx -> {
            timer.startSingleTimer(new Failed(new TimeoutException()), config.T());
            toWait.forEach(ctx::watch);
            return Logging.logging(ctx.getLog(), new Init(replyTo, config, node, key, value, key2node, toWait, toLock), precollecting(replyTo, config, node, key, value, toLock, toWait.size()));
        }));
    }

    private static MBehavior<Msg> precollecting(
        ActorRef<DidOrDidnt.Put> replyTo,
        Config config,
        int node,
        int key,
        Optional<String> value,
        ImmutableList<ActorRef<Node.Cmd>> toLock,
        int toWait
    ) {

        if (toWait == 0) {

            final var collecting = collecting(replyTo, config, node, key, value, toLock, Lists.immutable.empty(), DEFAULT.version());

            return new MBehavior<>(collecting.state(), Behaviors.setup(ctx -> {
                toLock.forEach(ref -> ref.tell(new Node.Lock(ctx.getSelf().narrow(), key, node)));
                return collecting.behavior();
            }));
        }

        record PreCollecting(int node, int key, Optional<String> value, ImmutableList<ActorRef<Node.Cmd>> toLock, int toWait) {}

        final var state = new PreCollecting(node, key, value, toLock, toWait);

        return new MBehavior<>(
            state,
            Behaviors.setup(ctx ->
                Behaviors.receive(Msg.class)
                    .onMessage(Failed.class, msg -> Logging.logging(ctx.getLog(), state, msg, MBehaviors.stopped(() -> replyTo.tell(new DidOrDidnt.Put.Didnt(msg.cause())))))
                    .onSignal(Terminated.class, s -> Logging.logging(ctx.getLog(), state, s, precollecting(replyTo, config, node, key, value, toLock, toWait - 1)))
                    .build()
            )
        );
    }

    private static MBehavior<Msg> collecting(
        ActorRef<DidOrDidnt.Put> replyTo,
        Config config,
        int node,
        int key,
        Optional<String> value,
        ImmutableList<ActorRef<Node.Cmd>> toUnlock,
        ImmutableList<ActorRef<Node.Write>> locked,
        BigInteger version
    ) {

        if (locked.size() == config.W()) {
            return MBehaviors.stopped(() -> {
                final var v = version.add(BigInteger.valueOf(node));
                locked.forEach(ref -> ref.tell(new Node.Write(node, key, new Node.Word(value, v))));
                toUnlock.forEach(ref -> ref.tell(new Node.Unlock(key, node)));
                replyTo.tell(new DidOrDidnt.Put.Did(v));
            });
        }

        record Collecting(int node, int key, Optional<String> value, ImmutableList<ActorRef<Node.Cmd>> toUnlock, ImmutableList<ActorRef<Node.Write>> locked, BigInteger version) {}

        final var state = new Collecting(node, key, value, toUnlock, locked, version);

        return new MBehavior<>(
            state,
            Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), state, msg, switch (msg) {
                case Ack x -> collecting(replyTo, config, node, key, value, toUnlock, locked.newWith(x.replyTo()), version.max(x.version()));
                case Skip x -> MBehaviors.stopped(() -> {
                    toUnlock.forEach(ref -> ref.tell(new Node.Unlock(key, node)));
                    replyTo.tell(new DidOrDidnt.Put.Did(x.version()));
                });
                case Failed x -> MBehaviors.stopped(() -> {
                    toUnlock.forEach(ref -> ref.tell(new Node.Unlock(key, node)));
                    replyTo.tell(new DidOrDidnt.Put.Didnt(x.cause()));
                });
            }))
        );
    }

}
