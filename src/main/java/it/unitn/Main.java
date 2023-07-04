package it.unitn;

import akka.actor.typed.ActorSystem;
import it.unitn.root.GetOrPut;
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
        root.tell(new Root.Clients(Lists.immutable.with(
            Lists.immutable.with(new GetOrPut.Put(10, 1, "a"), new GetOrPut.Get(10, 1)),
            Lists.immutable.with(new GetOrPut.Put(10, 1, "b"), new GetOrPut.Put(10, 1, "j")),
            Lists.immutable.with(new GetOrPut.Put(30, 1, "c"), new GetOrPut.Get(10, 1)),
            Lists.immutable.with(new GetOrPut.Put(10, 1, "d"), new GetOrPut.Get(10, 1)),
            Lists.immutable.with(new GetOrPut.Put(10, 1, "e"), new GetOrPut.Put(10, 1, "k")),
            Lists.immutable.with(new GetOrPut.Put(20, 1, "f"), new GetOrPut.Get(10, 1)),
            Lists.immutable.with(new GetOrPut.Put(10, 1, "g"), new GetOrPut.Get(10, 1)),
            Lists.immutable.with(new GetOrPut.Put(10, 1, "h"), new GetOrPut.Put(10, 1, "l")),
            Lists.immutable.with(new GetOrPut.Put(10, 1, "i"), new GetOrPut.Get(10, 1))
        )));
        root.tell(new Root.Clients(Lists.immutable.with(
            Lists.immutable.with(new GetOrPut.Put(10, 90, "a"), new GetOrPut.Put(10, 90, "e"), new GetOrPut.Get(10, 90)),
            Lists.immutable.with(new GetOrPut.Put(20, 90, "b"), new GetOrPut.Put(10, 90)),
            Lists.immutable.with(new GetOrPut.Put(30, 90, "c"), new GetOrPut.Get(10, 90)),
            Lists.immutable.with(new GetOrPut.Put(80, 90, "d"), new GetOrPut.Put(10, 90, "f"))
        )));
        root.tell(new Root.Stop());
    }
}
