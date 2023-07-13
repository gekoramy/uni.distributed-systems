package it.unitn.root;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.Behaviors;
import it.unitn.Config;
import it.unitn.client.Client;
import it.unitn.node.Node;
import it.unitn.utils.Logging;
import it.unitn.utils.MBehavior;
import it.unitn.utils.MBehaviors;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.map.primitive.ImmutableIntObjectMap;
import org.eclipse.collections.api.set.ImmutableSet;
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

    record Crash(ImmutableIntSet who) implements Cmd {

        public Crash(int... who) {
            this(IntSets.immutable.with(who));
        }

    }

    record Recover(int who, int with) implements Cmd {}

    record Leave(int who) implements Cmd {}

    record Clients(ImmutableList<ImmutableList<GetOrPut>> clients) implements Cmd {}

    record Resume(State s) implements Event {}

    static Behavior<Msg> init(Config config, ImmutableIntSet keys) {

        record Init(Config config, ImmutableIntSet keys) {}

        if (keys.size() < config.N())
            throw new AssertionError("|key2node| >= N");

        if (config.N() < config.R())
            throw new AssertionError("N >= R");

        if (config.N() < config.W())
            throw new AssertionError("N >= W");

        if (config.N() / 2 >= config.W())
            throw new AssertionError("N/2 < W");

        if (config.N() >= config.R() + config.W())
            throw new AssertionError("N < R + W");

        if (config.T().isNegative() || config.T().isZero())
            throw new AssertionError("T > 0");

        if (keys.anySatisfy(k -> k <= 0))
            throw new AssertionError("keys > 0");

        return Behaviors.setup(ctx -> {

            final var key2node =
                keys.injectInto(
                    IntObjectMaps.immutable.<ActorRef<Node.Cmd>>empty(),
                    (acc, k) -> acc.newWithKeyValue(k, ctx.spawn(newbie(k).narrow(), Integer.toString(k)))
                );

            key2node.forEach(ref -> ref.tell(new Node.Setup(config, key2node)));

            return Logging.logging(ctx.getLog(), new Init(config, keys), available(new State(key2node)));

        });
    }

    private static MBehavior<Msg> available(State s) {

        record Available(State s) {}

        final var state = new Available(s);

        return new MBehavior<>(
            state,
            Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), state, msg, switch (msg) {

                case Stop ignored -> MBehaviors.stopped();

                case Join x -> {

                    if (x.who() <= 0)
                        throw new AssertionError("who > 0");

                    if (s.key2node().containsKey(x.who())) {
                        ctx.getLog().debug("node %2d already present".formatted(x.who()));
                        yield available(s);
                    }

                    final var with = s.key2node().get(x.with());

                    if (with == null) {
                        ctx.getLog().debug("node %2d missing".formatted(x.with()));
                        yield available(s);
                    }

                    final var task = ctx.spawnAnonymous(onDDJoin(ctx.getSelf(), s, x.who()));
                    ctx.spawn(newbie(task, x.who(), with).narrow(), Integer.toString(x.who()));

                    yield blocked();

                }

                case Crash x -> {

                    final var missing = x.who().difference(s.key2node().keySet());

                    if (!missing.isEmpty()) {
                        ctx.getLog().debug("missing nodes : %s".formatted(missing));
                        yield available(s);
                    }

                    x.who()
                        .collect(s.key2node()::get, Lists.mutable.empty())
                        .forEach(node -> node.tell(new Node.Crash()));

                    yield available(s);

                }

                case Recover x -> {

                    final var who = s.key2node().get(x.who());
                    final var with = s.key2node().get(x.with());

                    if (who == null) {
                        ctx.getLog().debug("node %2d missing".formatted(x.who()));
                        yield available(s);
                    }

                    if (with == null) {
                        ctx.getLog().debug("node %2d missing".formatted(x.with()));
                        yield available(s);
                    }

                    final var task = ctx.spawnAnonymous(onDDRecover(ctx.getSelf(), s));

                    who.tell(new Node.Recover(task, with));

                    yield blocked();

                }

                case Leave x -> {

                    final var who = s.key2node().get(x.who());

                    if (who == null) {
                        ctx.getLog().debug("node %2d missing".formatted(x.who()));
                        yield available(s);
                    }

                    final var task = ctx.spawnAnonymous(onDDLeave(ctx.getSelf(), s, x.who(), who));

                    who.tell(new Node.Leave(task.narrow()));

                    yield blocked();

                }

                case Clients x -> {

                    final var missing = x.clients()
                        .flatCollectInt(gps -> gps.collectInt(GetOrPut::who), IntSets.mutable.empty())
                        .difference(s.key2node().keySet());

                    if (!missing.isEmpty()) {
                        ctx.getLog().debug("missing nodes : %s".formatted(missing));
                        yield available(s);
                    }

                    final var clients = x.clients()
                        .collect(gps -> gps.collect(gp -> convert(s, gp)))
                        .collect(queue -> ctx.spawnAnonymous(Client.sequentially(queue).behavior()), Sets.mutable.empty())
                        .toImmutable();

                    ctx.spawnAnonymous(tillTerminated(ctx.getSelf(), s, clients));

                    yield blocked();

                }

                case Resume x -> {

                    ctx.getLog().error("unexpected %s".formatted(x));
                    yield available(s);

                }

            }))
        );
    }

    private static MBehavior<Msg> blocked() {

        record Blocked() {}

        final var state = new Blocked();

        return new MBehavior<>(
            state,
            Behaviors.withStash(
                1_000_000,
                buffer -> Behaviors.receive((ctx, msg) -> switch (msg) {

                    case Resume x -> buffer.unstashAll(Logging.logging(ctx.getLog(), state, msg, available(x.s())));

                    default -> {
                        buffer.stash(msg);
                        yield Behaviors.<Msg>same();
                    }

                }))
        );
    }

    private static Behavior<DidOrDidnt.Join> onDDJoin(
        ActorRef<Msg> root,
        State s,
        int k
    ) {

        record OnDDJoin(ActorRef<Msg> root, State s, int k) {}

        return Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), new OnDDJoin(root, s, k), msg, MBehaviors.stopped(() ->
            root.tell(new Resume(switch (msg) {

                case DidOrDidnt.Join.Did x -> new State(s.key2node().newWithKeyValue(k, x.who()));

                case DidOrDidnt.Join.Didnt ignored -> s;

            }))
        )));
    }

    private static Behavior<DidOrDidnt.Leave> onDDLeave(
        ActorRef<Msg> root,
        State s,
        int k,
        ActorRef<Node.Cmd> who
    ) {

        record OnDDLeave(ActorRef<Msg> root, State s, int k, ActorRef<Node.Cmd> who) {}

        return Behaviors.withTimers(timer -> Behaviors.setup(ctx -> {

            timer.startSingleTimer(new DidOrDidnt.Leave.Didnt(new TimeoutException()), Duration.ofSeconds(2L));
            ctx.watchWith(who, new DidOrDidnt.Leave.Did());

            return Behaviors.receiveMessage(msg -> Logging.logging(ctx.getLog(), new OnDDLeave(root, s, k, who), msg, MBehaviors.stopped(() ->
                root.tell(new Resume(switch (msg) {

                    case DidOrDidnt.Leave.Did ignored -> new State(s.key2node().newWithoutKey(k));

                    case DidOrDidnt.Leave.Didnt ignored -> s;

                }))
            )));

        }));
    }

    private static Behavior<DidOrDidnt.Recover> onDDRecover(
        ActorRef<Msg> root,
        State s
    ) {

        record OnDDRecover(ActorRef<Msg> root, State s) {}

        return Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), new OnDDRecover(root, s), msg, MBehaviors.stopped(() -> root.tell(new Resume(s)))));
    }

    private static Behavior<Void> tillTerminated(
        ActorRef<Msg> root,
        State s,
        ImmutableSet<ActorRef<Void>> refs
    ) {

        record TillTerminated(ActorRef<Msg> root, State s, ImmutableSet<ActorRef<Void>> refs) {}

        return Behaviors.setup(ctx -> {
            refs.forEach(ctx::watch);
            return Logging.logging(ctx.getLog(), new TillTerminated(root, s, refs), monotonically(root, s, refs));
        });
    }

    private static MBehavior<Void> monotonically(
        ActorRef<Msg> root,
        State s,
        ImmutableSet<ActorRef<Void>> refs
    ) {

        record Monotonically(ActorRef<Msg> root, State s, ImmutableSet<ActorRef<Void>> refs) {}

        return refs.isEmpty()
            ? MBehaviors.stopped(() -> root.tell(new Resume(s)))
            : new MBehavior<>(new Monotonically(root, s, refs), Behaviors.setup(ctx -> Behaviors.receive(Void.class)
            .onSignal(Terminated.class, t -> Logging.logging(ctx.getLog(), new Monotonically(root, s, refs), t, monotonically(root, s, refs.newWithout(t.getRef()))))
            .build()));
    }

    private static Client.GetOrPut convert(State s, GetOrPut gp) {
        return switch (gp) {
            case GetOrPut.Get get -> new Client.Get(s.key2node().get(get.who()).narrow(), get.k());
            case GetOrPut.Put put -> new Client.Put(s.key2node().get(put.who()).narrow(), put.k(), put.value());
        };
    }

}
