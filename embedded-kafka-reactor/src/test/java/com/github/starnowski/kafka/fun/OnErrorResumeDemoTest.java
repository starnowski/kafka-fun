package com.github.starnowski.kafka.fun;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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

    @Test
    public void thirdTestCase() {
        // GIVEN
        Flux<Integer> stream = Flux.range(1, 5)
                .doOnNext(i -> System.out.println("input=" + i))
                .map(i -> i == 3 ? i / 0 : i)
                .map(i -> i)
                .onErrorContinue((err, i) -> {
                    System.out.println("onErrorContinue");
                });

        // WHEN
        StepVerifier.FirstStep<Integer> stepVerifier = StepVerifier.create(stream);

        // THEN
        stepVerifier
                .expectNext(1)
                .expectNext(2)
                .expectNext(4)
                .expectNext(5)
                .verifyComplete();
    }

    @Test
    public void fourthTestCase() {
        // GIVEN
        Mono<Integer> stream = Flux.range(1, 5)
                .doOnNext(i -> System.out.println("input=" + i))
                .map(i -> i == 3 ? i / 0 : i)
                .map(i -> i)
                .onErrorResume(err -> {
                    System.out.println("onErrorResume");
                    return Flux.empty();
                }).reduce((i,j) -> i+j).onErrorResume(err -> {
            System.out.println("onErrorResume");
            return Mono.empty();
        });


        // WHEN
        StepVerifier.FirstStep<Integer> stepVerifier = StepVerifier.create(stream);

        // THEN
        stepVerifier
                .expectNext(3)
                .verifyComplete();
    }

    @Test
    public void fifthTestCase() {
        // GIVEN
        Mono<Integer> stream = Flux.range(1, 5)
                .doOnNext(i -> System.out.println("input=" + i))
                .map(i -> i == 3 ? i / 0 : i)
                .map(i -> i)
                .reduce((i,j) -> i+j).onErrorResume(err -> {
                    System.out.println("onErrorResume + 10");
                    return Mono.just(10);
                });


        // WHEN
        StepVerifier.FirstStep<Integer> stepVerifier = StepVerifier.create(stream);

        // THEN
        stepVerifier
                .expectNext(10)
                .verifyComplete();
    }
}
