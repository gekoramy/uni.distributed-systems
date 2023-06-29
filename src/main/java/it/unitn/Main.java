package it.unitn;

import akka.actor.typed.ActorSystem;
import it.unitn.root.Root;
import org.eclipse.collections.api.factory.primitive.IntSets;

import java.time.Duration;

public interface Main {
    static void main(String[] args) {

        final ActorSystem<Root.Cmd> root = ActorSystem.create(
            Root.init(new Config(4, 3, 2, Duration.ofSeconds(1L)), IntSets.immutable.with(10, 20, 30, 40))
                .narrow(),
            "root"
        );

        root.tell(new Root.Join(50, 40));
        root.tell(new Root.Join(60, 50));
        root.tell(new Root.Join(70, 60));
        root.tell(new Root.Crash(10, 30, 50, 70));
        root.tell(new Root.Recover(70, 60));
        root.tell(new Root.Join(80, 70));
        root.tell(new Root.Recover(70, 60));
        root.tell(new Root.Leave(70));
        root.tell(new Root.Join(70, 80));
        root.tell(new Root.Leave(50));
        root.tell(new Root.Crash(10, 20, 30, 40, 50, 60, 70));
        root.tell(new Root.Leave(80));
        root.tell(new Root.Recover(10, 80));
        root.tell(new Root.Recover(20, 80));
        root.tell(new Root.Recover(30, 80));
        root.tell(new Root.Recover(40, 80));
        root.tell(new Root.Recover(50, 80));
        root.tell(new Root.Recover(60, 80));
        root.tell(new Root.Recover(70, 80));
        root.tell(new Root.Leave(40));
        root.tell(new Root.Leave(50));
        root.tell(new Root.Leave(60));
        root.tell(new Root.Leave(70));
        root.tell(new Root.Leave(80));
        root.tell(new Root.Stop());
    }
}
