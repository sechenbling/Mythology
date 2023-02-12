package cn.scut.app.repository;

import cn.scut.app.entity.sql.ProjectEntity;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProjectRepository extends JpaRepository<ProjectEntity, Integer> {

  Optional<ProjectEntity> findByProjectName(String name);

  Page<ProjectEntity> findByProjectNameLikeOrDomainLikeOrOwnersLike(String projectName,
      String domain, String owner, Pageable pageable);

  Long countByProjectNameLikeOrDomainLikeOrOwnersLike(String projectName,
      String domain, String owner);
}
