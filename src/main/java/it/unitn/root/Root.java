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

    record Resume(ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node) implements Event {}

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

            return Logging.logging(ctx.getLog(), new Init(config, keys), available(key2node));

        });
    }

    private static MBehavior<Msg> available(ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node) {

        record Available(ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node) {}

        final var state = new Available(key2node);

        return new MBehavior<>(
            state,
            Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), state, msg, switch (msg) {

                case Stop ignored -> MBehaviors.stopped();

                case Join x -> {

                    if (x.who() <= 0)
                        throw new AssertionError("who > 0");

                    if (key2node.containsKey(x.who())) {
                        ctx.getLog().debug("node %2d already present".formatted(x.who()));
                        yield available(key2node);
                    }

                    final var with = key2node.get(x.with());

                    if (with == null) {
                        ctx.getLog().debug("node %2d missing".formatted(x.with()));
                        yield available(key2node);
                    }

                    final var task = ctx.spawnAnonymous(onDDJoin(ctx.getSelf(), key2node, x.who()));
                    ctx.spawn(newbie(task, x.who(), with).narrow(), Integer.toString(x.who()));

                    yield blocked();

                }

                case Crash x -> {

                    final var missing = x.who().difference(key2node.keySet());

                    if (!missing.isEmpty()) {
                        ctx.getLog().debug("missing nodes : %s".formatted(missing));
                        yield available(key2node);
                    }

                    x.who()
                        .collect(key2node::get, Lists.mutable.empty())
                        .forEach(node -> node.tell(new Node.Crash()));

                    yield available(key2node);

                }

                case Recover x -> {

                    final var who = key2node.get(x.who());
                    final var with = key2node.get(x.with());

                    if (who == null) {
                        ctx.getLog().debug("node %2d missing".formatted(x.who()));
                        yield available(key2node);
                    }

                    if (with == null) {
                        ctx.getLog().debug("node %2d missing".formatted(x.with()));
                        yield available(key2node);
                    }

                    final var task = ctx.spawnAnonymous(onDDRecover(ctx.getSelf(), key2node));

                    who.tell(new Node.Recover(task, with));

                    yield blocked();

                }

                case Leave x -> {

                    final var who = key2node.get(x.who());

                    if (who == null) {
                        ctx.getLog().debug("node %2d missing".formatted(x.who()));
                        yield available(key2node);
                    }

                    final var task = ctx.spawnAnonymous(onDDLeave(ctx.getSelf(), key2node, x.who(), who));

                    who.tell(new Node.Leave(task.narrow()));

                    yield blocked();

                }

                case Clients x -> {

                    final var missing = x.clients()
                        .flatCollectInt(gps -> gps.collectInt(GetOrPut::who), IntSets.mutable.empty())
                        .difference(key2node.keySet());

                    if (!missing.isEmpty()) {
                        ctx.getLog().debug("missing nodes : %s".formatted(missing));
                        yield available(key2node);
                    }

                    final var clients = x.clients()
                        .collect(gps -> gps.collect(gp -> convert(key2node, gp)))
                        .collect(queue -> ctx.spawnAnonymous(Client.sequentially(queue).behavior()), Sets.mutable.empty())
                        .toImmutable();

                    ctx.spawnAnonymous(tillTerminated(ctx.getSelf(), key2node, clients));

                    yield blocked();

                }

                case Resume x -> {

                    ctx.getLog().error("unexpected %s".formatted(x));
                    yield available(key2node);

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

                    case Resume x -> buffer.unstashAll(Logging.logging(ctx.getLog(), state, msg, available(x.key2node())));

                    default -> {
                        buffer.stash(msg);
                        yield Behaviors.<Msg>same();
                    }

                }))
        );
    }

    private static Behavior<DidOrDidnt.Join> onDDJoin(
        ActorRef<Msg> root,
        ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node,
        int k
    ) {

        record OnDDJoin(ActorRef<Msg> root, ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node, int k) {}

        return Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), new OnDDJoin(root, key2node, k), msg, MBehaviors.stopped(() ->
            root.tell(new Resume(switch (msg) {

                case DidOrDidnt.Join.Did x -> key2node.newWithKeyValue(k, x.who());

                case DidOrDidnt.Join.Didnt ignored -> key2node;

            }))
        )));
    }

    private static Behavior<DidOrDidnt.Leave> onDDLeave(
        ActorRef<Msg> root,
        ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node,
        int k,
        ActorRef<Node.Cmd> who
    ) {

        record OnDDLeave(ActorRef<Msg> root, ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node, int k, ActorRef<Node.Cmd> who) {}

        return Behaviors.withTimers(timer -> Behaviors.setup(ctx -> {

            timer.startSingleTimer(new DidOrDidnt.Leave.Didnt(new TimeoutException()), Duration.ofSeconds(2L));
            ctx.watchWith(who, new DidOrDidnt.Leave.Did());

            return Behaviors.receiveMessage(msg -> Logging.logging(ctx.getLog(), new OnDDLeave(root, key2node, k, who), msg, MBehaviors.stopped(() ->
                root.tell(new Resume(switch (msg) {

                    case DidOrDidnt.Leave.Did ignored -> key2node.newWithoutKey(k);

                    case DidOrDidnt.Leave.Didnt ignored -> key2node;

                }))
            )));

        }));
    }

    private static Behavior<DidOrDidnt.Recover> onDDRecover(
        ActorRef<Msg> root,
        ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node
    ) {

        record OnDDRecover(ActorRef<Msg> root, ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node) {}

        return Behaviors.receive((ctx, msg) -> Logging.logging(ctx.getLog(), new OnDDRecover(root, key2node), msg, MBehaviors.stopped(() -> root.tell(new Resume(key2node)))));
    }

    private static Behavior<Void> tillTerminated(
        ActorRef<Msg> root,
        ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node,
        ImmutableSet<ActorRef<Void>> refs
    ) {

        record TillTerminated(ActorRef<Msg> root, ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node, ImmutableSet<ActorRef<Void>> refs) {}

        return Behaviors.setup(ctx -> {
            refs.forEach(ctx::watch);
            return Logging.logging(ctx.getLog(), new TillTerminated(root, key2node, refs), monotonically(root, key2node, refs));
        });
    }

    private static MBehavior<Void> monotonically(
        ActorRef<Msg> root,
        ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node,
        ImmutableSet<ActorRef<Void>> refs
    ) {

        record Monotonically(ActorRef<Msg> root, ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node, ImmutableSet<ActorRef<Void>> refs) {}

        return refs.isEmpty()
            ? MBehaviors.stopped(() -> root.tell(new Resume(key2node)))
            : new MBehavior<>(new Monotonically(root, key2node, refs), Behaviors.setup(ctx -> Behaviors.receive(Void.class)
            .onSignal(Terminated.class, t -> Logging.logging(ctx.getLog(), new Monotonically(root, key2node, refs), t, monotonically(root, key2node, refs.newWithout(t.getRef()))))
            .build()));
    }

    static Client.GetOrPut convert(ImmutableIntObjectMap<ActorRef<Node.Cmd>> key2node, GetOrPut gp) {
        return switch (gp) {
            case GetOrPut.Get get -> new Client.Get(key2node.get(get.who()).narrow(), get.k());
            case GetOrPut.Put put -> new Client.Put(key2node.get(put.who()).narrow(), put.k(), put.value());
        };
    }

}
