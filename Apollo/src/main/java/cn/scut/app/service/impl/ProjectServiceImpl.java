package cn.scut.app.service.impl;

import static cn.scut.app.constant.ExceptionMessage.alreadyExistsMessage;
import static cn.scut.app.constant.ExceptionMessage.noPermissionMessage;
import static cn.scut.app.constant.ExceptionMessage.notFoundMessage;
import static cn.scut.app.util.FuzzySearchKeyWrapper.wrap;

import cn.scut.app.entity.request.ProjectRequest;
import cn.scut.app.entity.response.PageResultResponse;
import cn.scut.app.entity.response.ProjectResponse;
import cn.scut.app.exception.CustomException;
import cn.scut.app.repository.ProjectRepository;
import cn.scut.app.service.ProjectService;
import cn.scut.app.util.converter.ProjectConverter;
import java.time.LocalDateTime;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.transaction.Transactional;
import javax.ws.rs.ForbiddenException;
import org.mapstruct.factory.Mappers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class ProjectServiceImpl implements ProjectService {

  private ProjectRepository projectRepository;

  private final ProjectConverter projectConverter = Mappers.getMapper(ProjectConverter.class);

  @Autowired
  public void setProjectRepository(ProjectRepository projectRepository) {
    this.projectRepository = projectRepository;
  }

  @Override
  @Transactional(rollbackOn = Exception.class)
  public ProjectResponse createProject(String projectName, ProjectRequest request) {
    projectRepository.findByProjectName(projectName)
        .ifPresent(projectEntity -> {
          throw new CustomException(alreadyExistsMessage(projectName));
        });
    return projectConverter.convert(
        projectRepository.save(projectConverter.convert(projectName, request)));
  }

  @Override
  public ProjectResponse updateProject(String projectName, ProjectRequest request) {
    return projectRepository.findByProjectName(projectName)
        .map(oldProjectEntity -> {
          if (!oldProjectEntity.hasPermission(request.getCreateBy())) {
            throw new ForbiddenException(noPermissionMessage(request.getCreateBy()));
          }
          oldProjectEntity.setDomain(request.getDomain());
          oldProjectEntity.setDescription(request.getDescription());
          oldProjectEntity.setUpdateTime(LocalDateTime.now());
          oldProjectEntity.updateOwners(request.getOwners());
          oldProjectEntity.setVersion(request.getVersion());
          return projectConverter.convert(oldProjectEntity);
        })
        .orElseThrow(() -> new NoSuchElementException(notFoundMessage(projectName)));
  }

  @Override
  public ProjectResponse getProject(String projectName) {
    return projectRepository
        .findByProjectName(projectName)
        .map(projectConverter::convert)
        .orElseThrow(() -> new NoSuchElementException(notFoundMessage(projectName)));
  }

  @Override
  public PageResultResponse<ProjectResponse> getPageableProjects(Pageable pageable) {

    List<ProjectResponse> res = projectRepository.findAll(pageable)
        .stream()
        .map(projectConverter::convert)
        .collect(Collectors.toList());
    Long total = projectRepository.count();
    return new PageResultResponse<>(total, pageable.getPageNumber(), pageable.getPageSize(), res);

  }

  @Override
  public PageResultResponse<ProjectResponse> searchProjects(String projectName, String domain,
      String owner, Pageable pageable) {
    String projectNameWrap = wrap(projectName);
    String domainWrap      = wrap(domain);
    String ownerWrap       = wrap(owner);
    if (Stream.of(projectNameWrap, domainWrap, ownerWrap).noneMatch(Objects::nonNull)) {
      return getPageableProjects(pageable);
    } else {
      Long total = projectRepository
          .countByProjectNameLikeOrDomainLikeOrOwnersLike(projectNameWrap, domainWrap, ownerWrap);
      List<ProjectResponse> res = projectRepository
          .findByProjectNameLikeOrDomainLikeOrOwnersLike(projectNameWrap, domainWrap, ownerWrap,
              pageable)
          .stream()
          .map(projectConverter::convert)
          .collect(Collectors.toList());
      return new PageResultResponse<>(total, pageable.getPageNumber(), pageable.getPageSize(), res);
    }
  }

  @Override
  public ProjectResponse deleteProject(String projectName, String createBy) {
    return projectRepository
        .findByProjectName(projectName)
        .map(entity -> {
          if (!entity.hasPermission(createBy)) {
            throw new ForbiddenException(noPermissionMessage(createBy));
          }
          projectRepository.delete(entity);
          return projectConverter.convert(entity);
        })
        .orElseThrow(() -> new NoSuchElementException(notFoundMessage(projectName)));
  }
}
