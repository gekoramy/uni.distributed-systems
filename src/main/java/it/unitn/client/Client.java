package it.unitn.client;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.node.Node;
import it.unitn.root.DidOrDidnt;
import it.unitn.utils.Logging;
import it.unitn.utils.MBehavior;
import it.unitn.utils.MBehaviors;
import org.eclipse.collections.api.list.ImmutableList;

import java.time.Duration;
import java.util.Optional;

/**
 * {@link Client} actor performs a list of operations sequentially.
 * <br>
 * Where:
 * <ul>
 *     <li>the list of operations is defined once</li>
 *     <li>an operation can be any between [{@link Get}, {@link Put}]</li>
 *     <li>every operation has a static timeout</li>
 * </ul>
 * It delegates the operations to on-demand children.
 */
public interface Client {

    sealed interface GetOrPut {}

    record Get(ActorRef<Node.Get> who, int k) implements GetOrPut {}

    record Put(ActorRef<Node.Put> who, int k, Optional<String> value) implements GetOrPut {}

    static MBehavior<Void> sequentially(ImmutableList<GetOrPut> queue) {

        record Sequentially(ImmutableList<GetOrPut> queue) {}

        final var state = new Sequentially(queue);

        return queue
            .getFirstOptional()
            .map(head -> new MBehavior<>(
                state,
                Behaviors.<Void>setup(ctx -> {

                    final var task = switch (head) {
                        case Get x -> get(x.who(), x.k());
                        case Put x -> put(x.who(), x.k(), x.value());
                    };

                    ctx.watch(ctx.spawnAnonymous(task));
                    return Logging.logging(ctx.getLog(), state, blocked(queue));
                })
            ))
            .orElseGet(MBehaviors::stopped);
    }

    private static MBehavior<Void> blocked(ImmutableList<GetOrPut> queue) {

        record Blocked() {}

        final var state = new Blocked();

        return new MBehavior<>(
            state,
            Behaviors.setup(ctx ->
                Behaviors.receive(Void.class)
                    .onSignal(Terminated.class, s -> Logging.logging(ctx.getLog(), state, s, sequentially(queue.drop(1))))
                    .build()
            )
        );
    }

    private static Behavior<DidOrDidnt.Get> get(ActorRef<Node.Get> who, int k) {

        record Get(ActorRef<Node.Get> who, int k) {}

        return Behaviors.setup(ctx -> {

            ctx.ask(
                DidOrDidnt.Get.class,
                who,
                Duration.ofSeconds(2L),
                ref -> new Node.Get(ref, k),
                (r, t) -> r != null ? r : new DidOrDidnt.Get.Didnt(t)
            );

            return Behaviors.receiveMessage(msg ->
                Logging.logging(ctx.getLog(), new Get(who, k), msg, MBehaviors.stopped())
            );
        });
    }

    private static Behavior<DidOrDidnt.Put> put(ActorRef<Node.Put> who, int k, Optional<String> value) {

        record Put(ActorRef<Node.Put> who, int k, Optional<String> value) {}

        return Behaviors.setup(ctx -> {

            ctx.ask(
                DidOrDidnt.Put.class,
                who,
                Duration.ofSeconds(2L),
                ref -> new Node.Put(ref, k, value),
                (r, t) -> r != null ? r : new DidOrDidnt.Put.Didnt(t)
            );

            return Behaviors.receiveMessage(msg ->
                Logging.logging(ctx.getLog(), new Put(who, k, value), msg, MBehaviors.stopped())
            );
        });
    }

}
