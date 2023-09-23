package it.unitn.root;

import akka.actor.typed.ActorRef;
import it.unitn.node.Node;

import java.math.BigInteger;

public interface DidOrDidnt {

    sealed interface Setup {

        record Did(int who) implements Setup {}

    }

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

    sealed interface Get {

        record Did(Node.Word word) implements Get {}

        record Didnt(Throwable cause) implements Get {}

    }

    sealed interface Put {

        record Did(BigInteger version) implements Put {}

        record Didnt(Throwable cause) implements Put {}

    }

}
