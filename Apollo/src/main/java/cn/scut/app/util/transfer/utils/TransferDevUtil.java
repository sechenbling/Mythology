package cn.scut.app.util.transfer.utils;

import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.hubspot.jinjava.Jinjava;
import java.util.Arrays;
import java.util.HashSet;
import org.yaml.snakeyaml.Yaml;

public class TransferDevUtil {

  public static final YAMLMapper      yamlMapper     =
      new YAMLMapper()
          .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
          .disable(YAMLGenerator.Feature.SPLIT_LINES);
  // .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);
  public static final Yaml            yaml           = new Yaml();
  public static final Jinjava         jinjava        = new Jinjava();
  public static       HashSet<String> strSpecialChar =
      new HashSet<>(Arrays.asList("\\|", "\\_", "\\+", "\\."));
}
