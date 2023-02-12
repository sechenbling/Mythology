package cn.scut.app.entity.sql;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.vladmihalcea.hibernate.type.json.JsonType;
import java.time.LocalDateTime;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;
import lombok.Data;
import org.hibernate.annotations.TypeDef;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.format.annotation.DateTimeFormat;

@Data
@MappedSuperclass
@TypeDef(name = "json", typeClass = JsonType.class)
public abstract class AbstractBaseEntity {

  @Transient
  private static final String FORMAT = "yyyy-MM-dd HH:mm:ss";

  @Column(nullable = false, columnDefinition = "VARCHAR(30)")
  private String createBy;

  @CreatedDate
  @DateTimeFormat(pattern = FORMAT)
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = FORMAT)
  @Column(nullable = false, columnDefinition = "DATETIME DEFAULT CURRENT_TIMESTAMP")
  private LocalDateTime createTime = LocalDateTime.now();

  @LastModifiedDate
  @DateTimeFormat(pattern = FORMAT)
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = FORMAT)
  @Column(columnDefinition = "DATETIME DEFAULT CURRENT_TIMESTAMP")
  private LocalDateTime updateTime;
}
