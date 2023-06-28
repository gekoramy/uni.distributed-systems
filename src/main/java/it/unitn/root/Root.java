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

import static it.unitn.node.Node.newbie;

public interface Root {

    record State(
        ImmutableIntObjectMap<ActorRef<Node.Msg>> key2node
    ) {}

    sealed interface Msg {}

    sealed interface Cmd extends Msg {}

    sealed interface Event extends Msg {}

    record Join(int who, int with) implements Cmd {}

    record Crash(int... who) implements Cmd {}

    record Recover(int who, int with) implements Cmd {}

    record Leave(int who) implements Cmd {}

    record Gone(int k) implements Event {}

    static Behavior<Msg> init(ImmutableIntSet keys) {

        if (keys.isEmpty()) {
            throw new AssertionError("init cannot be empty");
        }

        return Behaviors.setup(ctx -> {

            final var key2node =
                keys.injectInto(
                    IntObjectMaps.immutable.<ActorRef<Node.Msg>>empty(),
                    (acc, k) -> acc.newWithKeyValue(k, ctx.spawn(newbie(k), Integer.toString(k)))
                );

            key2node.forEachKeyValue((k, ref) -> {
                ctx.watchWith(ref, new Gone(k));
                ref.tell(new Node.Setup(key2node));
            });

            return available(new State(key2node));

        });
    }

    private static Behavior<Msg> available(State s) {
        return Behaviors.receive((ctx, msg) -> {

            ctx.getLog().info("\n\t%s\n\t%s".formatted(s, msg));

            return switch (msg) {

                case Gone x -> {

                    final var key2node =
                        s.key2node().newWithoutKey(x.k());

                    yield available(new State(key2node));

                }

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

                    final var listener = ctx.spawn(Task.listener(), "task");
                    ctx.watch(listener);

                    final var who = ctx.spawn(newbie(listener, x.who(), with), Integer.toString(x.who()));
                    ctx.watchWith(who, new Gone(x.who()));

                    final var key2node = s.key2node().newWithKeyValue(x.who(), who);
                    yield busy(new State(key2node));

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

                    yield available(s);

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

                    final var listener = ctx.spawn(Task.listener(), "task");
                    ctx.watch(listener);

                    who.tell(new Node.Recover(listener, with));

                    yield busy(s);

                }

                case Leave x -> {

                    final var who = s.key2node().get(x.who());

                    if (who == null) {
                        ctx.getLog().debug("node %2d missing".formatted(x.who()));
                        yield Behaviors.same();
                    }

                    final var listener = ctx.spawn(Task.listener(Duration.ofSeconds(2L)), "task");
                    ctx.watch(listener);

                    who.tell(new Node.Leave(listener));

                    yield busy(s);

                }

            };
        });
    }

    private static Behavior<Msg> busy(State s) {
        return Behaviors.withStash(
            1_000_000,
            stash -> Behaviors.receive(Msg.class)
                .onAnyMessage(msg -> {
                    stash.stash(msg);
                    return Behaviors.same();
                })
                .onSignal(akka.actor.typed.Terminated.class, ignored -> stash.unstashAll(available(s)))
                .build()
        );
    }

}
