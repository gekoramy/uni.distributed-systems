package it.unitn.root;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public interface Task {

    sealed interface Result {}

    record Left(Throwable cause) implements Result {}

    record Right() implements Result {}

    static Behavior<Result> listener() {
        return Behaviors.receive((ctx, r) -> {

            switch (r) {
                case Left l -> ctx.getLog().debug("L", l.cause());
                case Right ignored -> ctx.getLog().debug("LGTM");
            }

            return Behaviors.stopped();
        });
    }

    static Behavior<Result> listener(Duration timeout) {
        return Behaviors.withTimers(timer -> {
            timer.startSingleTimer(new Left(new TimeoutException()), timeout);
            return listener();
        });
    }

}
