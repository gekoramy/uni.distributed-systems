package it.unitn.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.Config;
import it.unitn.root.DidOrDidnt;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.map.primitive.ImmutableIntObjectMap;

import java.util.Objects;

public interface Node {

    record State(
        int k,
        Config config,
        ImmutableIntObjectMap<ActorRef<Cmd>> key2node
    ) {}

    sealed interface Msg {}

    sealed interface Cmd extends Msg {}

    sealed interface Event extends Msg {}

    record Setup(Config config, ImmutableIntObjectMap<ActorRef<Cmd>> key2node) implements Cmd {}

    record Ask4key2node(ActorRef<Joining.Res4key2node> replyTo) implements Cmd {}

    record Crash() implements Cmd {}

    record Recover(ActorRef<DidOrDidnt.Recover> replyTo, ActorRef<Node.Cmd> ref) implements Cmd {}

    record Leave(ActorRef<DidOrDidnt.Leave.Didnt> replyTo) implements Cmd {}

    record DidJoin(Config config, ImmutableIntObjectMap<ActorRef<Cmd>> key2node) implements Event {}

    record DidntJoin(Throwable cause) implements Event {}

    record Announce(int key, ActorRef<Cmd> ref) implements Cmd {}

    record AnnounceLeaving(ActorRef<Leaving.Ack> replyTo, int key) implements Cmd {}

    record DidLeave() implements Event {}

    record DidntLeave(Throwable cause) implements Event {}

    static Behavior<Msg> newbie(int k) {
        return Behaviors.receive((ctx, msg) -> {

            ctx.getLog().info("newbie\n\t%d\n\t%s".formatted(k, msg));

            return switch (msg) {

                case Setup x && x.key2node().size() > x.config().N() ->
                    redundant(new State(k, x.config(), x.key2node()));

                case Setup x && x.key2node().size() == x.config().N() ->
                    minimal(new State(k, x.config(), x.key2node()));

                case Setup x && x.key2node().size() < x.config().N() -> Behaviors.stopped(() -> {
                    throw new AssertionError("|key2node| >= N");
                });

                default -> Behaviors.stopped(() -> {
                    throw new AssertionError("only %s".formatted(Setup.class.getName()));
                });

            };
        });
    }

    static Behavior<Msg> newbie(ActorRef<DidOrDidnt.Join> replyTo, int k, ActorRef<Cmd> ref) {
        return Behaviors.withStash(
            1_000_000,
            stash -> Behaviors.setup(ctx -> {

                if (Objects.equals(ref, ctx.getSelf()))
                    throw new AssertionError("cannot join myself...");

                ctx.spawn(Joining.joining(ctx.getSelf().narrow(), ref.narrow()), "joining");

                return Behaviors.<Msg>receiveMessage(msg -> {

                    ctx.getLog().info("newbie\n\t%d\n\t%s".formatted(k, msg));

                    return switch (msg) {

                        case DidJoin x -> {
                            x.key2node().forEachValue(node -> node.tell(new Announce(k, ctx.getSelf().narrow())));
                            replyTo.tell(new DidOrDidnt.Join.Did(ctx.getSelf().narrow()));

                            final var key2node = x.key2node().newWithKeyValue(k, ctx.getSelf().narrow());
                            yield stash.unstashAll(redundant(new State(k, x.config(), key2node)));
                        }

                        case DidntJoin x -> {
                            replyTo.tell(new DidOrDidnt.Join.Didnt(x.cause()));
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
                    x.replyTo().tell(new Joining.Res4key2node(s.config(), s.key2node()));
                    yield Behaviors.same();
                }

                case Announce x -> {
                    final var key2node = s.key2node().newWithKeyValue(x.key(), x.ref());
                    yield redundant(new State(s.k(), s.config(), key2node));
                }

                case Crash ignored -> crashed(s.k());

                case Recover x -> {
                    x.replyTo().tell(new DidOrDidnt.Recover.Didnt(new AssertionError("not crashed")));
                    yield Behaviors.same();
                }

                case Leave x -> {
                    x.replyTo().tell(new DidOrDidnt.Leave.Didnt(new AssertionError("cannot leave")));
                    yield Behaviors.same();
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
                    x.replyTo().tell(new Joining.Res4key2node(s.config(), s.key2node()));
                    yield Behaviors.same();
                }

                case Announce x -> {
                    final var key2node = s.key2node().newWithKeyValue(x.key(), x.ref());
                    yield redundant(new State(s.k(), s.config(), key2node));
                }

                case Crash ignored -> crashed(s.k());

                case Recover x -> {
                    x.replyTo().tell(new DidOrDidnt.Recover.Didnt(new AssertionError("not crashed")));
                    yield Behaviors.same();
                }

                case Leave x -> leaving(x.replyTo(), s);

                case AnnounceLeaving x -> {
                    x.replyTo().tell(new Leaving.Ack());

                    final var key2node = s.key2node().newWithoutKey(x.key());
                    final var newState = new State(s.k(), s.config(), key2node);

                    yield key2node.size() == s.config().N()
                        ? minimal(newState)
                        : redundant(newState);
                }

                default -> Behaviors.same();

            };

        });
    }

    private static Behavior<Msg> crashed(int k) {
        return Behaviors.receive((ctx, msg) -> {

            ctx.getLog().info("crashed\n\t%d\n\t%s".formatted(k, msg));

            return switch (msg) {

                case Recover x -> recovering(x.replyTo(), k, x.ref());

                default -> Behaviors.same();

            };
        });
    }

    private static Behavior<Msg> recovering(ActorRef<DidOrDidnt.Recover> replyTo, int k, ActorRef<Cmd> ref) {
        return Behaviors.withStash(
            1_000_000,
            stash -> Behaviors.setup(ctx -> {

                if (Objects.equals(ref, ctx.getSelf()))
                    throw new AssertionError("cannot recover w/ myself...");

                ctx.spawn(Joining.joining(ctx.getSelf().narrow(), ref.narrow()), "recovering");

                return Behaviors.<Msg>receiveMessage(msg -> {

                    ctx.getLog().info("recovering\n\t%d\n\t%s".formatted(k, msg));

                    return switch (msg) {

                        case DidJoin x -> {
                            replyTo.tell(new DidOrDidnt.Recover.Did());

                            final var key2node = x.key2node().newWithKeyValue(k, ctx.getSelf().narrow());
                            final var newState = new State(k, x.config(), key2node);

                            yield stash.unstashAll(
                                key2node.size() == x.config().N()
                                    ? minimal(newState)
                                    : redundant(newState)
                            );
                        }

                        case DidntJoin x -> {
                            replyTo.tell(new DidOrDidnt.Recover.Didnt(x.cause()));
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

    private static Behavior<Msg> leaving(ActorRef<DidOrDidnt.Leave.Didnt> replyTo, State s) {
        return Behaviors.withStash(
            1_000_000,
            stash -> Behaviors.setup(ctx -> {

                final ImmutableList<ActorRef<AnnounceLeaving>> nodes =
                    s.key2node()
                        .newWithoutKey(s.k())
                        .collect(ActorRef::<AnnounceLeaving>narrow, Lists.mutable.empty())
                        .toImmutable();

                ctx.spawn(Leaving.leaving(ctx.getSelf().narrow(), s.k(), nodes), "leaving");

                return Behaviors.<Msg>receiveMessage(msg -> {

                    ctx.getLog().info("leaving\n\t%s\n\t%s".formatted(s, msg));

                    return switch (msg) {

                        case DidLeave ignored -> Behaviors.stopped();

                        case DidntLeave x -> {
                            replyTo.tell(new DidOrDidnt.Leave.Didnt(x.cause()));
                            yield s.key2node().size() == s.config().N()
                                ? minimal(s)
                                : redundant(s);
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

}
