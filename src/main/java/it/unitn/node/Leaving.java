package it.unitn.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import org.eclipse.collections.api.list.ImmutableList;

import java.time.Duration;

public interface Leaving {

    sealed interface Msg {}

    record Ack() implements Msg {}

    record Failed(Throwable cause) implements Msg {}

    static Behavior<Msg> leaving(
        ActorRef<Node.Event> parent,
        int k,
        ImmutableList<ActorRef<Node.AnnounceLeaving>> refs
    ) {
        return Behaviors.setup(ctx -> Behaviors.withTimers(timer -> {

            refs.forEach(ref -> ref.tell(new Node.AnnounceLeaving(ctx.getSelf().narrow(), k)));

            timer.startSingleTimer(
                new Failed(new AssertionError("no ack")),
                Duration.ofSeconds(1L)
            );

            return Behaviors.<Msg>receiveMessage(msg -> switch (msg) {

                case Ack ignored -> {
                    parent.tell(new Node.DidLeave());
                    yield Behaviors.stopped();
                }

                case Failed x -> {
                    parent.tell(new Node.DidntLeave(x.cause()));
                    yield Behaviors.stopped();
                }

            });

        }));
    }

}
