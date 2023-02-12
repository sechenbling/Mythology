package cn.scut.app.util.converter;

import cn.scut.app.entity.request.ProjectRequest;
import cn.scut.app.entity.response.ProjectResponse;
import cn.scut.app.entity.sql.ProjectEntity;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;

@Mapper(componentModel = "spring")
public interface ProjectConverter {

  @Mapping(target = "owners", source = "request.owners", qualifiedByName = "ownersSetToString")
  ProjectEntity convert(String projectName, ProjectRequest request);

  @Mapping(target = "owners", source = "entity.owners", qualifiedByName = "ownersStringToSet")
  ProjectResponse convert(ProjectEntity entity);

  @Named("ownersSetToString")
  static String setToString(Set<String> owners) {
    return String.join(",", owners);
  }

  @Named("ownersStringToSet")
  static Set<String> stringToSet(String owners) {
    return Arrays.stream(owners.split(","))
        .collect(Collectors.toSet());
  }
}
