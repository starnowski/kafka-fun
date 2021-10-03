package com.github.starnowski.kafka.fun;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class OnErrorResumeDemoTest {

    @Test
    public void firstTestCase() {
        // GIVEN
        Flux<Integer> stream = Flux.range(1, 5)
                .doOnNext(i -> System.out.println("input=" + i))
                .map(i -> i == 3 ? i / 0 : i)
                .map(i -> i);

        // WHEN
        StepVerifier.FirstStep<Integer> stepVerifier = StepVerifier.create(stream);

        // THEN
        stepVerifier
                .expectNext(1)
                .expectNext(2)
                .expectError(ArithmeticException.class)
                .verify();
    }

    @Test
    public void secondTestCase() {
        // GIVEN
        Flux<Integer> stream = Flux.range(1, 5)
                .doOnNext(i -> System.out.println("input=" + i))
                .map(i -> i == 3 ? i / 0 : i)
                .map(i -> i)
                .onErrorResume(err -> {
                    System.out.println("onErrorResume");
                    return Flux.empty();
                });

        // WHEN
        StepVerifier.FirstStep<Integer> stepVerifier = StepVerifier.create(stream);

        // THEN
        stepVerifier
                .expectNext(1)
                .expectNext(2)
                .verifyComplete();
    }
}
