package app.metatron.flows.bistel.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import app.metatron.flows.bistel.model.SplitDataVO;

public class FlatMapSplitter implements FlatMapFunction<String, SplitDataVO> {
  final static DateFormat DATEFORMATTER = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");

  public void flatMap(String input, Collector<SplitDataVO> out) throws Exception {
    String[] inputSplit = input.split(",");

    for (int i = 1; i < inputSplit.length; i++) {
      SplitDataVO splitDataVO = new SplitDataVO(inputSplit[0].length() > 1 ? DATEFORMATTER.parse(inputSplit[0]) : new Date(),
              inputSplit[1].length() > 1 ? "P" + i                            : "",
              inputSplit[2].length() > 1 ? Double.parseDouble(inputSplit[i])  : 0);
      out.collect(splitDataVO);
    }
  }
}
