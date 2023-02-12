package cn.scut.app.entity.sql;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.Version;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "project", indexes = {
    @Index(name = "projectUniqueIndex", columnList = "projectName", unique = true)
})
public class ProjectEntity extends AbstractBaseEntity {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(columnDefinition = "INTEGER UNSIGNED")
  private Long projectId;

  @Column(nullable = false, columnDefinition = "VARCHAR(80)")
  private String projectName;

  @Column(nullable = false, columnDefinition = "VARCHAR(32)")
  private String version;

  @Column(columnDefinition = "TEXT")
  @Basic(fetch = FetchType.LAZY)
  private String description;

  @Column(nullable = false, columnDefinition = "VARCHAR(30)")
  private String domain;

  @Column(columnDefinition = "TEXT")
  private String owners;

  @Version
  @Column(columnDefinition = "INTEGER UNSIGNED")
  private Long versionFormat = 0L;

  private Set<String> getOwnersSet() {
    return Arrays.stream(owners.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
  }

  public Boolean hasPermission(String userName) {
    Set<String> ownersSet = getOwnersSet();
    return ownersSet.contains(userName);
  }

  public void updateOwners(Set<String> usernames) {
    this.owners = String.join(",", usernames);
  }
}

