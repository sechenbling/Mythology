package cn.scut.app.util.transfer.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.commons.lang3.StringUtils;

public class TransferRenderUtil {

  private String delimiter        = ",";
  private String containDelimiter = ",";

  public static ArrayList<HashMap<String, Object>> render(
      JsonNode renderParamsNode, String delimiter) throws Exception {
    TransferRenderUtil yamlDelimiterUtil = new TransferRenderUtil();
    yamlDelimiterUtil.delimiter        = delimiter;
    yamlDelimiterUtil.containDelimiter =
        TransferDevUtil.strSpecialChar.contains(delimiter) ? delimiter.substring(1) : delimiter;
    return yamlDelimiterUtil.renderParamsFormat(renderParamsNode);
  }

  private ArrayList<HashMap<String, Object>> renderParamsFormat(JsonNode renderParamsNode)
      throws Exception {
    Iterator<String> iter           = renderParamsNode.fieldNames();
    HashSet<String>  singleParams   = new HashSet<>();
    String           listParam      = "UNDEFINED";
    int              listParamCount = 0;
    while (iter.hasNext()) {
      String   renderParam    = iter.next();
      JsonNode renderJsonNode = renderParamsNode.get(renderParam);
      if (renderJsonNode.isArray() && renderJsonNode.size() > 1) {
        listParam = renderParam;
        listParamCount++;
      } else {
        singleParams.add(renderParam);
      }
    }
    if (listParamCount > 1) {
      throw new RuntimeException("Render params illegal error! The number of list param > 1!");
    }
    if (singleParams.size() == 0 && listParamCount == 0) {
      throw new Exception("Render params illegal error! Render is empty!");
    }
    if (singleParams.size() != 0 && listParamCount == 0) {
      ArrayList<HashMap<String, Object>> renderParamsList = new ArrayList<>();
      renderParamsList.add(this.renderSingleParamsFormat(renderParamsNode, singleParams));
      return renderParamsList;
    } else if (singleParams.size() == 0) {
      return this.renderListParamsFormat(renderParamsNode, listParam);
    } else {
      return this.renderCombineParamsFormat(renderParamsNode, singleParams, listParam);
    }
  }

  private HashMap<String, Object> renderSingleParamsFormat(
      JsonNode renderParamsNode, HashSet<String> singleParams) throws Exception {
    HashMap<String, Object> baseParamsMap = new HashMap<>();
    // System.out.println(renderParamsNode);
    for (String singleParam : singleParams) {
      if (singleParam.contains(this.containDelimiter)) {
        String[] paramNames = singleParam.split(delimiter);
        String[] params;
        if (renderParamsNode.get(singleParam) instanceof ArrayNode) {
          params = renderParamsNode.get(singleParam).get(0).asText().split(delimiter);
        } else {
          params = renderParamsNode.get(singleParam).asText().split(delimiter);
        }
        this.renderParamsDelimiter(baseParamsMap, paramNames, params);
      } else {
        baseParamsMap.put(singleParam, renderParamsNode.get(singleParam));
      }
    }
    return baseParamsMap;
  }

  private ArrayList<HashMap<String, Object>> renderListParamsFormat(
      JsonNode renderParamsNode, String listParam) throws Exception {
    ArrayList<HashMap<String, Object>> listParamsList = new ArrayList<>();
    ArrayNode                          paramsList     = (ArrayNode) renderParamsNode.get(listParam);
    if (listParam.contains(containDelimiter)) {
      String[] listParamNames = listParam.split(delimiter);
      int      length         = listParamNames.length;
      for (JsonNode paramsElem : paramsList) {
        // String paramsElemStr = paramsElem.toString();
        String                  paramsElemStr = paramsElem.asText();
        String[]                params        = paramsElemStr.split(delimiter);
        HashMap<String, Object> listParamsMap = new HashMap<>();
        if (length != params.length) {
          throw new Exception(
              "Render list params format failed: param_name and paramValue can not match");
        }
        this.renderParamsDelimiter(listParamsMap, listParamNames, params);
        listParamsList.add(listParamsMap);
      }
    } else {
      for (JsonNode paramsElem : paramsList) {
        HashMap<String, Object> listParamsMap = new HashMap<>();
        String                  param         = paramsElem.asText();
        if (StringUtils.isNumeric(param)) {
          double paramValue = Double.parseDouble(param);
          if (Math.floor(paramValue) == Math.ceil(paramValue)) {
            listParamsMap.put(listParam, (int) paramValue);
          } else {
            listParamsMap.put(listParam, paramValue);
          }
        } else {
          listParamsMap.put(listParam, param);
        }
        listParamsList.add(listParamsMap);
      }
    }
    return listParamsList;
  }

  private ArrayList<HashMap<String, Object>> renderCombineParamsFormat(
      JsonNode renderParamsNode, HashSet<String> singleParams, String listParam)
      throws Exception {
    HashMap<String, Object> leftParams =
        this.renderSingleParamsFormat(renderParamsNode, singleParams);
    ArrayList<HashMap<String, Object>> rightParamsList =
        this.renderListParamsFormat(renderParamsNode, listParam);
    ArrayList<HashMap<String, Object>> renderParamsList = new ArrayList<>();
    for (HashMap<String, Object> rightParams : rightParamsList) {
      HashMap<String, Object> renderParamsElem = new HashMap<>(leftParams);
      renderParamsElem.putAll(rightParams);
      renderParamsList.add(renderParamsElem);
    }
    return renderParamsList;
  }

  private void renderParamsDelimiter(
      HashMap<String, Object> map, String[] paramNames, String[] params) throws Exception {
    if (paramNames.length != params.length) {
      throw new Exception(
          "Render list params format failed: param_name and paramValue can not match");
    }
    for (int i = 0; i < params.length; i++) {
      String param = params[i];
      if (StringUtils.isNumeric(param)) {
        double paramValue = Double.parseDouble(param);
        if (Math.floor(paramValue) == Math.ceil(paramValue)) {
          map.put(paramNames[i], (int) paramValue);
        } else {
          map.put(paramNames[i], paramValue);
        }
      } else {
        map.put(paramNames[i], param);
      }
    }
  }
}
