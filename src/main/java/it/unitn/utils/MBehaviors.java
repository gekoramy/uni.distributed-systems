package it.unitn.utils;

import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.japi.Procedure;
import akka.japi.function.Effect;

public interface MBehaviors {

    static <T> MBehavior<T> stopped(Procedure<ActorContext<T>> effect) {
        return new MBehavior<>("stopped", Behaviors.setup(ctx -> Behaviors.stopped(() -> effect.apply(ctx))));
    }

    static <T> MBehavior<T> stopped(Effect effect) {
        return new MBehavior<>("stopped", Behaviors.stopped(effect));
    }

    static <T> MBehavior<T> stopped() {
        return new MBehavior<>("stopped", Behaviors.stopped());
    }

}
