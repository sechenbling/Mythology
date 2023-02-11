package cn.scut.app.service;

import cn.scut.app.request.ProjectRequest;
import cn.scut.app.response.CommonResponse;
import cn.scut.app.response.PageResultResponse;
import cn.scut.app.response.ProjectResponse;
import org.springframework.data.domain.Pageable;

public interface ProjectService {

  ProjectResponse createProject(String projectName, ProjectRequest request);

  ProjectResponse updateProject(String projectName, ProjectRequest request);

  ProjectResponse getProject(String projectName);

  PageResultResponse<ProjectResponse> getPageableProjects(Pageable pageable);

  PageResultResponse<ProjectResponse> searchProjects(String projectName, String domain,
      String owner,
      Pageable pageable);

  ProjectResponse deleteProject(String projectName, String createBy);
}
