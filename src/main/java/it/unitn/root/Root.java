package it.unitn.root;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.node.Node;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.map.primitive.ImmutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.ImmutableIntSet;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static it.unitn.node.Node.newbie;

public interface Root {

    record State(
        ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node
    ) {}

    sealed interface Msg {}

    sealed interface Cmd extends Msg {}

    sealed interface Event extends Msg {}

    record Stop() implements Cmd {}

    record Join(int who, int with) implements Cmd {}

    record Crash(int... who) implements Cmd {}

    record Recover(int who, int with) implements Cmd {}

    record Leave(int who) implements Cmd {}

    record Resume(State s) implements Event {}

    static Behavior<Msg> init(ImmutableIntSet keys) {

        if (keys.isEmpty()) {
            throw new AssertionError("init cannot be empty");
        }

        return Behaviors.setup(ctx -> {

            final var key2node =
                keys.injectInto(
                    IntObjectMaps.immutable.<ActorRef<Node.Cmd>>empty(),
                    (acc, k) -> acc.newWithKeyValue(k, ctx.spawn(newbie(k).narrow(), Integer.toString(k)))
                );

            key2node.forEach(ref -> ref.tell(new Node.Setup(key2node)));

            return available(new State(key2node));

        });
    }

    private static Behavior<Msg> available(State s) {
        return Behaviors.receive((ctx, msg) -> {

            ctx.getLog().info("\n\t%s\n\t%s".formatted(s, msg));

            return switch (msg) {

                case Stop ignored -> Behaviors.stopped();

                case Join x -> {

                    if (s.key2node().containsKey(x.who())) {
                        ctx.getLog().debug("node %2d already present".formatted(x.who()));
                        yield Behaviors.same();
                    }

                    final var with = s.key2node().get(x.with());

                    if (with == null) {
                        ctx.getLog().debug("node %2d missing".formatted(x.with()));
                        yield Behaviors.same();
                    }

                    final var task = ctx.spawnAnonymous(onDDJoin(ctx.getSelf(), s, x.who()));
                    ctx.spawn(newbie(task, x.who(), with), Integer.toString(x.who()));

                    yield blocked();

                }

                case Crash x -> {

                    final var who = IntSets.immutable.with(x.who());
                    final var missing = who.difference(s.key2node().keySet());

                    if (!missing.isEmpty()) {
                        ctx.getLog().debug("missing nodes : %s".formatted(missing));
                        yield Behaviors.same();
                    }

                    who
                        .collect(s.key2node()::get, Lists.mutable.empty())
                        .forEach(node -> node.tell(new Node.Crash()));

                    yield Behaviors.same();

                }

                case Recover x -> {

                    final var who = s.key2node().get(x.who());
                    final var with = s.key2node().get(x.with());

                    if (who == null) {
                        ctx.getLog().debug("node %2d missing".formatted(x.who()));
                        yield Behaviors.same();
                    }

                    if (with == null) {
                        ctx.getLog().debug("node %2d missing".formatted(x.with()));
                        yield Behaviors.same();
                    }

                    final var task = ctx.spawnAnonymous(onDDRecover(ctx.getSelf(), s));

                    who.tell(new Node.Recover(task, with));

                    yield blocked();

                }

                case Leave x -> {

                    final var who = s.key2node().get(x.who());

                    if (who == null) {
                        ctx.getLog().debug("node %2d missing".formatted(x.who()));
                        yield Behaviors.same();
                    }

                    final var task = ctx.spawnAnonymous(onDDLeave(ctx.getSelf(), s, x.who(), who));

                    who.tell(new Node.Leave(task.narrow()));

                    yield blocked();

                }

                case Resume x -> {

                    ctx.getLog().error("unexpected %s".formatted(x));
                    yield Behaviors.same();

                }

            };
        });
    }

    private static Behavior<Msg> blocked() {
        return Behaviors.withStash(1_000_000, buffer -> Behaviors.receive(Msg.class)
            .onMessage(Resume.class, msg -> buffer.unstashAll(available(msg.s())))
            .onAnyMessage(msg -> {
                buffer.stash(msg);
                return Behaviors.same();
            })
            .build()
        );
    }

    private static Behavior<DidOrDidnt.Join> onDDJoin(
        ActorRef<Msg> root,
        State s,
        int k
    ) {
        return Behaviors.receive((ctx, msg) -> switch (msg) {

            case DidOrDidnt.Join.Did x -> {
                ctx.getLog().info("yay");
                root.tell(new Resume(new State(s.key2node().newWithKeyValue(k, x.who()))));
                yield Behaviors.stopped();
            }

            case DidOrDidnt.Join.Didnt x -> {
                ctx.getLog().info("ouch", x.cause());
                root.tell(new Resume(s));
                yield Behaviors.stopped();
            }

        });
    }

    private static Behavior<DidOrDidnt.Leave> onDDLeave(
        ActorRef<Msg> root,
        State s,
        int k,
        ActorRef<Node.Cmd> who
    ) {
        return Behaviors.withTimers(timer -> {
            timer.startSingleTimer(new DidOrDidnt.Leave.Didnt(new TimeoutException()), Duration.ofSeconds(2L));
            return Behaviors.setup(ctx -> {
                ctx.watchWith(who, new DidOrDidnt.Leave.Did());
                return Behaviors.<DidOrDidnt.Leave>receiveMessage(msg -> switch (msg) {

                    case DidOrDidnt.Leave.Did ignored -> {
                        ctx.getLog().info("yay");
                        root.tell(new Resume(new State(s.key2node().newWithoutKey(k))));
                        yield Behaviors.stopped();
                    }

                    case DidOrDidnt.Leave.Didnt x -> {
                        ctx.getLog().info("ouch", x.cause());
                        root.tell(new Resume(s));
                        yield Behaviors.stopped();
                    }

                });
            });
        });
    }

    private static Behavior<DidOrDidnt.Recover> onDDRecover(
        ActorRef<Msg> root,
        State s
    ) {
        return Behaviors.receive((ctx, msg) -> switch (msg) {

            case DidOrDidnt.Recover.Did ignored -> {
                ctx.getLog().info("yay");
                root.tell(new Resume(s));
                yield Behaviors.stopped();
            }

            case DidOrDidnt.Recover.Didnt x -> {
                ctx.getLog().info("ouch", x.cause());
                root.tell(new Resume(s));
                yield Behaviors.stopped();
            }

        });
    }

}
