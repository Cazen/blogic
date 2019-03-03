package app.metatron.flows.bistel.function;

import org.apache.flink.api.common.functions.MapFunction;

import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import app.metatron.flows.bistel.model.RawDataVO;

public class CustomMapSplitter implements MapFunction<String, RawDataVO> {
  final static DateFormat DATEFORMATTER = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss.SSS");

  Double p1SensorValue = 0d;
  Double p2SensorValue = 0d;
  Double p3SensorValue = 0d;
  Double p4SensorValue = 0d;
  Double p5SensorValue = 0d;
  Double p6SensorValue = 0d;
  Double p7SensorValue = 0d;
  Double p8SensorValue = 0d;
  Double p9SensorValue = 0d;

  @Override
  public RawDataVO map(String input) throws Exception {
    String[] inputSplit = input.split(",");
//    System.out.println(inputSplit[0]);
//    Date date = DATEFORMATTER.parse(inputSplit[0]);

    RawDataVO rawDataVO = new RawDataVO(inputSplit[0].length() > 10 ? DATEFORMATTER.parse(inputSplit[0]) : new Date(),
//    RawDataVO rawDataVO = new RawDataVO(date,
                                        p1SensorValue, p2SensorValue, p3SensorValue, p4SensorValue, p5SensorValue,
                                        p6SensorValue, p7SensorValue, p8SensorValue, p9SensorValue);

    for (int i = 1; i < inputSplit.length; i++) {
      try {
        Field field = RawDataVO.class.getField("p" + i + "SensorValue");
        field.setAccessible(true);
        field.setDouble(rawDataVO, Double.parseDouble(inputSplit[i]));
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      } catch (NoSuchFieldException e) {
        e.printStackTrace();
      }
    }

    return rawDataVO;
  }
}
