package io.github.yq808.taskqueue.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.yq808.taskqueue.domain.ErrorType;
import io.github.yq808.taskqueue.handler.NonRetryableTaskException;
import io.github.yq808.taskqueue.handler.RecoverableSystemException;
import io.github.yq808.taskqueue.handler.RetryableTaskException;
import java.io.IOException;
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
    void looksThroughExecutionWrappers() {
        var wrapped = new ExecutionException(new RetryableTaskException("429"));
        assertThat(FailureClassifier.classify(wrapped)).isEqualTo(ErrorType.BUSINESS_RETRYABLE);
        assertThat(FailureClassifier.describe(wrapped)).isEqualTo("RetryableTaskException: 429");
    }
}
