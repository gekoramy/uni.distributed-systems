package it.unitn.extensions;

import akka.actor.testkit.typed.javadsl.ActorTestKit;

public record ActorTestKitExt(ActorTestKit kit) implements AutoCloseable {

    @Override
    public void close() {
        kit.shutdownTestKit();
    }

}
