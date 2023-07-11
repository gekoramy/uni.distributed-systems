package it.unitn;

import akka.actor.typed.ActorSystem;
import it.unitn.root.GetOrPut.Get;
import it.unitn.root.GetOrPut.Put;
import it.unitn.root.Root;
import org.eclipse.collections.api.factory.Lists;
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
        root.tell(new Root.Join(40, 10));
        root.tell(new Root.Join(50, 10));
        root.tell(new Root.Join(60, 10));
        root.tell(new Root.Join(70, 10));
        root.tell(new Root.Clients(Lists.immutable.with(
            Lists.immutable.with(new Put(10, 1, "a"), new Get(10, 1)),
            Lists.immutable.with(new Put(10, 1, "b"), new Put(10, 1, "j")),
            Lists.immutable.with(new Put(30, 1, "c"), new Get(10, 1)),
            Lists.immutable.with(new Put(10, 1, "d"), new Get(10, 1)),
            Lists.immutable.with(new Put(10, 1, "e"), new Put(10, 1, "k")),
            Lists.immutable.with(new Put(20, 1, "f"), new Get(10, 1)),
            Lists.immutable.with(new Put(10, 1, "g"), new Get(10, 1)),
            Lists.immutable.with(new Put(10, 1, "h"), new Put(10, 1, "l")),
            Lists.immutable.with(new Put(10, 1, "i"), new Get(10, 1))
        )));
        root.tell(new Root.Clients(Lists.immutable.with(
            Lists.immutable.with(new Put(10, 50, "a"), new Put(10, 50, "e"), new Get(10, 50)),
            Lists.immutable.with(new Put(20, 50, "b"), new Put(10, 50)),
            Lists.immutable.with(new Put(30, 50, "c"), new Get(10, 50)),
            Lists.immutable.with(new Put(80, 50, "d"), new Put(10, 50, "f"))
        )));
        root.tell(new Root.Crash(50, 70));
        root.tell(new Root.Clients(Lists.immutable.with(
            Lists.immutable.with(new Put(10, 50, "g"), new Put(10, 50, "k"), new Get(10, 50)),
            Lists.immutable.with(new Put(20, 50, "h"), new Put(10, 50)),
            Lists.immutable.with(new Put(30, 50, "i"), new Get(10, 50)),
            Lists.immutable.with(new Put(80, 50, "j"), new Put(10, 50, "l"))
        )));
        root.tell(new Root.Crash(10, 50, 60, 70));
        root.tell(new Root.Leave(80));
        root.tell(new Root.Recover(10, 20));
        root.tell(new Root.Leave(80));
        root.tell(new Root.Recover(50, 20));
        root.tell(new Root.Recover(70, 20));
        root.tell(new Root.Leave(80));
        root.tell(new Root.Stop());
    }
}
