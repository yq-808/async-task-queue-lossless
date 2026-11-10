package io.github.yq808.taskqueue.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.fasterxml.jackson.core.JsonParseException;
import io.github.yq808.taskqueue.domain.ErrorType;
import io.github.yq808.taskqueue.handler.NonRetryableTaskException;
import io.github.yq808.taskqueue.handler.RecoverableSystemException;
import io.github.yq808.taskqueue.handler.RetryableTaskException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;

class FailureClassifierTest {

    @Test
    void classifiesBusinessAndSystemFailures() {
        assertThat(FailureClassifier.classify(new RetryableTaskException("429"))).isEqualTo(ErrorType.BUSINESS_RETRYABLE);
        assertThat(FailureClassifier.classify(new TimeoutException())).isEqualTo(ErrorType.BUSINESS_RETRYABLE);
        assertThat(FailureClassifier.classify(new NonRetryableTaskException("bad"))).isEqualTo(ErrorType.BUSINESS_FATAL);
        assertThat(FailureClassifier.classify(new RecoverableSystemException("503"))).isEqualTo(ErrorType.SYSTEM_RECOVERABLE);
        assertThat(FailureClassifier.classify(new IOException("reset"))).isEqualTo(ErrorType.SYSTEM_RECOVERABLE);
        assertThat(FailureClassifier.classify(new QueryTimeoutException("db"))).isEqualTo(ErrorType.SYSTEM_RECOVERABLE);
        assertThat(FailureClassifier.classify(new NullPointerException())).isEqualTo(ErrorType.SYSTEM_FATAL);
        assertThat(FailureClassifier.classify(new OutOfMemoryError())).isEqualTo(ErrorType.SYSTEM_FATAL);
    }

    @Test
    void wrappedIoErrorsAreRecoverableAndUnparsableAnswersAreRetryable() {
        assertThat(FailureClassifier.classify(new UncheckedIOException(new IOException("reset"))))
                .isEqualTo(ErrorType.SYSTEM_RECOVERABLE);
        assertThat(FailureClassifier.classify(new IllegalStateException("call failed", new ConnectException("refused"))))
                .isEqualTo(ErrorType.SYSTEM_RECOVERABLE);
        assertThat(FailureClassifier.classify(new JsonParseException(null, "unexpected token")))
                .isEqualTo(ErrorType.BUSINESS_RETRYABLE);
        assertThat(FailureClassifier.classify(new UncheckedIOException(new JsonParseException(null, "bad answer"))))
                .as("UncheckedIOException is a pure wrapper").isEqualTo(ErrorType.BUSINESS_RETRYABLE);
        assertThat(FailureClassifier.classify(new IllegalStateException("bug", new JsonParseException(null, "x"))))
                .as("a parse error deep inside a bug is still a bug").isEqualTo(ErrorType.SYSTEM_FATAL);
    }

    @Test
    void looksThroughExecutionWrappers() {
        var wrapped = new ExecutionException(new RetryableTaskException("429"));
        assertThat(FailureClassifier.classify(wrapped)).isEqualTo(ErrorType.BUSINESS_RETRYABLE);
        assertThat(FailureClassifier.describe(wrapped)).isEqualTo("RetryableTaskException: 429");

        // A cause chain that loops back on itself must not hang the worker classifying it.
        var outer = new IllegalStateException("outer");
        outer.initCause(new IllegalStateException("inner", outer));
        var execution = new ExecutionException("wrapper") { };
        execution.initCause(new CompletionException(execution));
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            assertThat(FailureClassifier.classify(outer)).isEqualTo(ErrorType.SYSTEM_FATAL);
            assertThat(FailureClassifier.classify(execution)).isEqualTo(ErrorType.SYSTEM_FATAL);
            assertThat(FailureClassifier.describe(execution)).startsWith("CompletionException");
        });
    }
}
