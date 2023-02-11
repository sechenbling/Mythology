package cn.scut.app.service.impl;

import cn.scut.app.request.ProjectRequest;
import cn.scut.app.response.PageResultResponse;
import cn.scut.app.response.ProjectResponse;
import cn.scut.app.service.ProjectService;
import javax.transaction.Transactional;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class ProjectServiceImpl implements ProjectService {

  @Override
  @Transactional(rollbackOn = Exception.class)
  public ProjectResponse createProject(String projectName, ProjectRequest request){
    return new ProjectResponse(1L, projectName, request.getDescription(),
        request.getVersion(),
        request.getDomain(), request.getOwners());
  }

  @Override
  public ProjectResponse updateProject(String projectName, ProjectRequest request) {
    return null;
  }

  @Override
  public ProjectResponse getProject(String projectName) {
    return null;
  }

  @Override
  public PageResultResponse<ProjectResponse> getPageableProjects(Pageable pageable) {
    return null;
  }

  @Override
  public PageResultResponse<ProjectResponse> searchProjects(String projectName, String domain,
      String owner, Pageable pageable) {
    return null;
  }

  @Override
  public ProjectResponse deleteProject(String projectName, String createBy) {
    return null;
  }
}
