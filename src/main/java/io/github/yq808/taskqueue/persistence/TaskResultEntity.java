package io.github.yq808.taskqueue.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "task_result")
public class TaskResultEntity {

    @Id
    @Column(name = "task_id")
    private String taskId;

    @Column(nullable = false)
    private String result;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected TaskResultEntity() {
    }

    public TaskResultEntity(String taskId, String result, Instant createdAt) {
        this.taskId = taskId;
        this.result = result;
        this.createdAt = createdAt;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getResult() {
        return result;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
