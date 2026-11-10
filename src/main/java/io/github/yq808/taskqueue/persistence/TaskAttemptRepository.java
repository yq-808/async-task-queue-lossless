package io.github.yq808.taskqueue.persistence;

import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface TaskAttemptRepository extends JpaRepository<TaskAttemptEntity, Long> {

    List<TaskAttemptEntity> findByTaskIdOrderByIdAsc(String taskId);

    @Modifying
    @Query("delete from TaskAttemptEntity a where a.taskId in :ids")
    int deleteByTaskIds(@Param("ids") Collection<String> ids);
}
