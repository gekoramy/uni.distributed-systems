package it.unitn.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.root.Task;
import org.eclipse.collections.api.map.primitive.ImmutableIntObjectMap;

import java.util.Objects;

public interface Node {

    record State(
        int k,
        ImmutableIntObjectMap<ActorRef<Msg>> key2node

    ) {}

    sealed interface Msg {}

    sealed interface Cmd extends Msg {}

    sealed interface Event extends Msg {}

    record Setup(ImmutableIntObjectMap<ActorRef<Msg>> key2node) implements Cmd {}

    record Ask4key2node(ActorRef<Joining.Res4key2node> replyTo) implements Cmd {}

    record DidJoin(ImmutableIntObjectMap<ActorRef<Msg>> key2node) implements Event {}

    record DidntJoin(Throwable cause) implements Event {}

    record Announce(int key, ActorRef<Msg> ref) implements Event {}

    static Behavior<Msg> newbie(int k) {
        return Behaviors.receive((ctx, msg) -> {

            ctx.getLog().info("newbie\n\t%d\n\t%s".formatted(k, msg));

            return switch (msg) {

                case Setup x -> minimal(new State(k, x.key2node()));

                default -> Behaviors.stopped(() -> {
                    throw new AssertionError("only %s".formatted(Setup.class.getName()));
                });

            };
        });
    }

    static Behavior<Msg> newbie(ActorRef<Task.Result> replyTo, int k, ActorRef<Msg> ref) {
        return Behaviors.withStash(
            1_000_000,
            stash -> Behaviors.setup(ctx -> {

                if (Objects.equals(ref, ctx.getSelf()))
                    throw new AssertionError("cannot join myself...");

                ctx.spawn(Joining.joining(ctx.getSelf(), ref), "joining");

                return Behaviors.<Msg>receiveMessage(msg -> {

                    ctx.getLog().info("newbie\n\t%d\n\t%s".formatted(k, msg));

                    return switch (msg) {

                        case DidJoin x -> {
                            x.key2node().forEachValue(node -> node.tell(new Announce(k, ctx.getSelf())));
                            replyTo.tell(new Task.Right());

                            final var key2node = x.key2node().newWithKeyValue(k, ctx.getSelf());
                            yield stash.unstashAll(redundant(new State(k, key2node)));
                        }

                        case DidntJoin x -> {
                            replyTo.tell(new Task.Left(x.cause()));
                            yield Behaviors.stopped();
                        }

                        default -> {
                            stash.stash(msg);
                            yield Behaviors.same();
                        }

                    };
                });
            })
        );
    }

    private static Behavior<Msg> minimal(State s) {
        return Behaviors.receive((ctx, msg) -> {

            ctx.getLog().info("minimal\n\t%s\n\t%s".formatted(s, msg));

            return switch (msg) {

                case Ask4key2node x -> {
                    x.replyTo().tell(new Joining.Res4key2node(s.key2node()));
                    yield Behaviors.same();
                }

                case Announce x -> {
                    final var key2node = s.key2node().newWithKeyValue(x.key(), x.ref());
                    yield redundant(new State(s.k(), key2node));
                }

                default -> Behaviors.same();

            };

        });
    }

    private static Behavior<Msg> redundant(State s) {
        return Behaviors.receive((ctx, msg) -> {

            ctx.getLog().info("redundant\n\t%s\n\t%s".formatted(s, msg));

            return switch (msg) {

                case Ask4key2node x -> {
                    x.replyTo().tell(new Joining.Res4key2node(s.key2node()));
                    yield Behaviors.same();
                }

                case Announce x -> {
                    final var key2node = s.key2node().newWithKeyValue(x.key(), x.ref());
                    yield redundant(new State(s.k(), key2node));
                }

                default -> Behaviors.same();

            };

        });
    }

}
