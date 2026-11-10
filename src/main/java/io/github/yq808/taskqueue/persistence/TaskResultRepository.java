package io.github.yq808.taskqueue.persistence;

import java.util.Collection;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface TaskResultRepository extends JpaRepository<TaskResultEntity, String> {

    @Modifying
    @Query("delete from TaskResultEntity r where r.taskId in :ids")
    int deleteByTaskIds(@Param("ids") Collection<String> ids);
}
