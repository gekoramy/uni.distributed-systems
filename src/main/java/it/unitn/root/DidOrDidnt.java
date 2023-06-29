package it.unitn.root;

import akka.actor.typed.ActorRef;
import it.unitn.node.Node;

public interface DidOrDidnt {

    sealed interface Join {

        record Did(ActorRef<Node.Cmd> who) implements Join {}

        record Didnt(Throwable cause) implements Join {}

    }

    sealed interface Leave {

        record Did() implements Leave {}

        record Didnt(Throwable cause) implements Leave {}

    }

    sealed interface Recover {

        record Did() implements Recover {}

        record Didnt(Throwable cause) implements Recover {}

    }

}
