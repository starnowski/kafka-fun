package com.github.starnowski.kafka.fun;

import java.util.stream.Stream;

public class RecoverableErrorPredicate {

    public <T extends Throwable> boolean test(Throwable throwable, int depth, Class<T>... throwables) {
        if (throwable == null) {
            return false;
        }
        if (depth < 0) {
            return false;
        }
        if (throwables != null && Stream.of(throwables).anyMatch(t -> t.isAssignableFrom(throwable.getClass()))) {
            return true;
        }
        return test(throwable.getCause(), depth - 1, throwables);
    }

    public static <T extends Throwable> boolean isErrorRecoverable(Throwable throwable, int depth, Class<T>... throwables) {
        return new RecoverableErrorPredicate().test(throwable, depth, throwables);
    }
}
