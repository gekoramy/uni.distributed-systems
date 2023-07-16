package it.unitn.root;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import it.unitn.Config;
import it.unitn.node.Node;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.map.primitive.ImmutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.ImmutableIntSet;

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
        return RootImpl.init(
            config,
            keys,
            RootImpl::onDDJoin,
            RootImpl::onDDLeave,
            RootImpl::onDDRecover,
            RootImpl::tillTerminated
        );
    }

}
