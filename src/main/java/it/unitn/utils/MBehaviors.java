package it.unitn.utils;

import akka.actor.typed.javadsl.Behaviors;
import akka.japi.function.Effect;

public interface MBehaviors {

    static <T> MBehavior<T> stopped(Effect effect) {
        return new MBehavior<>("stopped", Behaviors.stopped(effect));
    }

    static <T> MBehavior<T> stopped() {
        return new MBehavior<>("stopped", Behaviors.stopped());
    }

}
