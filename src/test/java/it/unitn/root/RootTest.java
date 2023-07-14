package it.unitn.root;

import akka.actor.typed.ActorRef;
import it.unitn.client.Client;
import it.unitn.node.Node;
import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RootTest {

    @TestFactory
    Stream<DynamicTest> convert() {

        interface ActorRef4Cmd extends ActorRef<Node.Cmd> {}

        final ActorRef<Node.Cmd> mock = mock(ActorRef4Cmd.class);

        when(mock.narrow()).thenReturn(mock);

        return Stream.of(
            dynamicTest(
                "get to get",
                () -> assertEquals(
                    new Client.Get(mock.narrow(), 10),
                    Root.convert(IntObjectMaps.immutable.of(5, mock), new GetOrPut.Get(5, 10))
                )
            ),
            dynamicTest(
                "put to put",
                () -> assertEquals(
                    new Client.Put(mock.narrow(), 10, Optional.of("")),
                    Root.convert(IntObjectMaps.immutable.of(5, mock), new GetOrPut.Put(5, 10, Optional.of("")))
                )
            ),
            dynamicTest(
                "it doesn't handle missing actors",
                () -> assertThrows(
                    NullPointerException.class,
                    () -> Root.convert(IntObjectMaps.immutable.empty(), new GetOrPut.Get(5, 10))
                )
            )
        );
    }

}
