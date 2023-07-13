package it.unitn.utils;

import akka.actor.typed.Behavior;

public record MBehavior<T>(Object state, Behavior<T> behavior) {}
