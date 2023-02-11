package cn.scut.util;

import cn.scut.app.util.TransferUtil;
import cn.scut.app.util.transfer.entity.output.TransferYaml;
import io.vavr.control.Either;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class TransferUtilTest {
  @Test
  public void metaTest() throws IOException {
    String yamlContent = FileUtils.readFileToString(
        new File("data/meta.yaml"),
        Charsets.toCharset("UTF-8"));
    Either<Throwable, TransferYaml> transform = TransferUtil.transform(yamlContent);
    transform.fold(
        fail -> {
          System.out.println(fail.getMessage());
          return null;
        },
        success -> {
          System.out.println("transform success");
          try {
            FileUtils.writeStringToFile(
                new File("data/meta_dynamic_dag.yaml"),
                success.getYamlContentString(),
                Charsets.toCharset("UTF-8"));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return null;
        });
  }
}
