package it.unitn.utils;

import akka.actor.typed.Behavior;
import org.slf4j.Logger;

public interface Logging {

    static <S, E, T> Behavior<T> logging(Logger logger, S state, E event, MBehavior<T> m) {
        logger.debug("\n\t[{}]\n  ,\t[{}]\n ->\t[{}]", state, event, m.state());
        return m.behavior();
    }

    static <S, T> Behavior<T> logging(Logger logger, S state, MBehavior<T> m) {
        logger.debug("\n\t[{}]\n ->\t[{}]", state, m.state());
        return m.behavior();
    }

}
