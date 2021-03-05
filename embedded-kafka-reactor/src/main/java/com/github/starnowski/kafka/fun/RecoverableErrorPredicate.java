package com.github.starnowski.kafka.fun;

import java.util.stream.Stream;

public class RecoverableErrorPredicate {

    public boolean test(Throwable throwable, int depth, Throwable... throwables) {
        if (throwable == null) {
            return false;
        }
        if (depth < 0) {
            return false;
        }
        if (Stream.of(throwables).anyMatch(t -> t.getClass().isAssignableFrom(throwable.getClass()))) {
            return true;
        }
        return test(throwable.getCause(), depth - 1, throwables);
    }

    public static boolean isErrorRecoverable(Throwable throwable, int depth, Throwable... throwables) {
        return new RecoverableErrorPredicate().test(throwable, depth, throwables);
    }
}
