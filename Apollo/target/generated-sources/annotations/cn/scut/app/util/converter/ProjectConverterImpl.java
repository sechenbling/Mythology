package cn.scut.app.util.converter;

import cn.scut.app.entity.request.ProjectRequest;
import cn.scut.app.entity.response.ProjectResponse;
import cn.scut.app.entity.sql.ProjectEntity;
import javax.annotation.Generated;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2023-02-12T15:41:07+0800",
    comments = "version: 1.5.2.Final, compiler: javac, environment: Java 1.8.0_301 (Oracle Corporation)"
)
@Component
public class ProjectConverterImpl implements ProjectConverter {

    @Override
    public ProjectEntity convert(String projectName, ProjectRequest request) {
        if ( projectName == null && request == null ) {
            return null;
        }

        ProjectEntity projectEntity = new ProjectEntity();

        if ( request != null ) {
            projectEntity.setOwners( ProjectConverter.setToString( request.getOwners() ) );
            projectEntity.setCreateBy( request.getCreateBy() );
            projectEntity.setVersion( request.getVersion() );
            projectEntity.setDescription( request.getDescription() );
            projectEntity.setDomain( request.getDomain() );
        }
        projectEntity.setProjectName( projectName );

        return projectEntity;
    }

    @Override
    public ProjectResponse convert(ProjectEntity entity) {
        if ( entity == null ) {
            return null;
        }

        ProjectResponse projectResponse = new ProjectResponse();

        projectResponse.setOwners( ProjectConverter.stringToSet( entity.getOwners() ) );
        projectResponse.setCreateBy( entity.getCreateBy() );
        projectResponse.setCreateTime( entity.getCreateTime() );
        projectResponse.setUpdateTime( entity.getUpdateTime() );
        projectResponse.setProjectId( entity.getProjectId() );
        projectResponse.setProjectName( entity.getProjectName() );
        projectResponse.setDescription( entity.getDescription() );
        projectResponse.setVersion( entity.getVersion() );
        projectResponse.setDomain( entity.getDomain() );

        return projectResponse;
    }
}
