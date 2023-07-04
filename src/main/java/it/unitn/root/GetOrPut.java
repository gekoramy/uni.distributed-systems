package it.unitn.root;

import java.util.Optional;

public sealed interface GetOrPut {

    int who();

    record Get(int who, int k) implements GetOrPut {}

    record Put(int who, int k, Optional<String> value) implements GetOrPut {

        public Put(int who, int k) {
            this(who, k, Optional.empty());
        }

        public Put(int who, int k, String value) {
            this(who, k, Optional.of(value));
        }

    }

}
