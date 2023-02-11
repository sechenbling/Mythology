package cn.scut.app.util.transfer.context.entity;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class TransferModels {

  public LinkedHashMap<String, ArrayList<String>> model2instance;
  public LinkedHashMap<String, String>            instance2model;

  public TransferModels() {
    this.model2instance = new LinkedHashMap<>();
    this.instance2model = new LinkedHashMap<>();
  }

  public LinkedHashMap<String, ArrayList<String>> getModel2instance() {
    return model2instance;
  }

  public LinkedHashMap<String, String> getInstance2model() {
    return instance2model;
  }
}
