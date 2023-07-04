package it.unitn.client;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.node.Node;
import it.unitn.root.DidOrDidnt;
import org.eclipse.collections.api.list.ImmutableList;

import java.time.Duration;
import java.util.Optional;

public interface Client {

    sealed interface GetOrPut {}

    record Get(ActorRef<Node.Get> who, int k) implements GetOrPut {}

    record Put(ActorRef<Node.Put> who, int k, Optional<String> value) implements GetOrPut {}

    static Behavior<Void> sequentially(ImmutableList<GetOrPut> queue) {
        return queue
            .getFirstOptional()
            .map(head -> Behaviors.<Void>setup(ctx -> {

                final var task = switch (head) {
                    case Get x -> get(x.who(), x.k());
                    case Put x -> put(x.who(), x.k(), x.value());
                };

                ctx.watch(ctx.spawnAnonymous(task));
                return blocked(queue);
            }))
            .orElseGet(Behaviors::stopped);
    }

    private static Behavior<Void> blocked(ImmutableList<GetOrPut> queue) {
        return Behaviors.receive(Void.class)
            .onSignal(Terminated.class, ignored -> sequentially(queue.drop(1)))
            .build();
    }

    private static Behavior<DidOrDidnt.Get> get(ActorRef<Node.Get> who, int k) {
        return Behaviors.setup(ctx -> {

            ctx.ask(
                DidOrDidnt.Get.class,
                who,
                Duration.ofSeconds(2L),
                ref -> new Node.Get(ref, k),
                (r, t) -> r != null ? r : new DidOrDidnt.Get.Didnt(t)
            );

            return Behaviors.<DidOrDidnt.Get>receiveMessage(msg -> switch (msg) {

                case DidOrDidnt.Get.Did x -> {
                    ctx.getLog().info(x.toString());
                    yield Behaviors.stopped();
                }

                case DidOrDidnt.Get.Didnt x -> {
                    ctx.getLog().info("didnt", x.cause());
                    yield Behaviors.stopped();
                }

            });
        });
    }

    private static Behavior<DidOrDidnt.Put> put(ActorRef<Node.Put> who, int k, Optional<String> value) {
        return Behaviors.setup(ctx -> {

            ctx.ask(
                DidOrDidnt.Put.class,
                who,
                Duration.ofSeconds(2L),
                ref -> new Node.Put(ref, k, value),
                (r, t) -> r != null ? r : new DidOrDidnt.Put.Didnt(t)
            );

            return Behaviors.<DidOrDidnt.Put>receiveMessage(msg -> switch (msg) {

                case DidOrDidnt.Put.Did x -> {
                    ctx.getLog().info(x.toString());
                    yield Behaviors.stopped();
                }

                case DidOrDidnt.Put.Didnt x -> {
                    ctx.getLog().info("didnt", x.cause());
                    yield Behaviors.stopped();
                }

            });
        });
    }

}
