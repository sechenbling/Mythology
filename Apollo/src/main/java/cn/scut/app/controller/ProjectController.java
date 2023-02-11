package cn.scut.app.controller;

import cn.scut.app.request.ProjectRequest;
import cn.scut.app.response.CommonResponse;
import cn.scut.app.response.ProjectResponse;
import cn.scut.app.service.ProjectService;
import cn.scut.app.util.handler.ResponseHandler;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/projects")
@Validated
public class ProjectController {

  private ProjectService projectService;

  @Autowired
  public void setDependencyB(ProjectService projectService) {
    this.projectService=projectService;
  }

  @PostMapping("/{projectName}")
  public ResponseEntity<CommonResponse> createProject(
      @PathVariable("projectName") String projectName,
      @RequestBody ProjectRequest request) {
    return ResponseHandler.handle(projectService.createProject(projectName,request));
  }

  @PutMapping(value = "/{projectName}")
  public ResponseEntity<CommonResponse> updateProject(
      @PathVariable("projectName") String projectName,
      @RequestBody @Valid ProjectRequest request) {
    return ResponseHandler.handle(projectService.updateProject(projectName,request));
  }

  @GetMapping("/{projectName}")
  public ResponseEntity<CommonResponse> getProject(
      @PathVariable("projectName") String projectName) {
    return ResponseHandler.handle(projectService.getProject(projectName));
  }

  @GetMapping("/")
  public ResponseEntity<CommonResponse> getPageableProjects(Pageable pageable) {
    return ResponseHandler.handle(projectService.getPageableProjects(pageable));
  }

  @GetMapping("/search")
  public ResponseEntity<CommonResponse> searchProjects(
      @RequestParam(value = "projectName", required = false) String projectName,
      @RequestParam(value = "domain", required = false) String domain,
      @RequestParam(value = "owner", required = false) String owner, Pageable pageable) {
    return ResponseHandler.handle(projectService.searchProjects(projectName, domain, owner, pageable));
  }

  @DeleteMapping("/{projectName}")
  public ResponseEntity<CommonResponse> deleteProject(
      @PathVariable("projectName") String projectName,
      @RequestParam("createBy") String createBy) {
    return ResponseHandler.handle(projectService.deleteProject(projectName, createBy));
  }
}
