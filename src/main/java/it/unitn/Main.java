package it.unitn;

import akka.actor.typed.ActorSystem;
import it.unitn.root.Root;
import org.eclipse.collections.api.factory.primitive.IntSets;

public interface Main {
    static void main(String[] args) {

        final ActorSystem<Root.Cmd> root = ActorSystem.create(
            Root.init(IntSets.immutable.with(10, 20, 30)).narrow(),
            "root"
        );

        root.tell(new Root.Join(40, 30));
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
        root.tell(new Root.Stop());
    }
}
