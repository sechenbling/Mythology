package cn.scut.app.util;


import java.util.Optional;

public class FuzzySearchKeyWrapper {

  private FuzzySearchKeyWrapper() {
  }

  public static final String wrap(String key) {
    return Optional.ofNullable(key)
        .map(k -> k.isEmpty() ? null : "%" + k + "%")
        .orElse(null);
  }
}
