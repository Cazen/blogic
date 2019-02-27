package app.metatron.flows.bistel.model;

import java.util.Date;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class SplitDataVO {
  Date timestamp;
  String sensorName;
  Double sensorValue;

  public SplitDataVO(Date timestamp, String sensorName, Double sensorValue) {
    this.timestamp = timestamp;
    this.sensorName = sensorName;
    this.sensorValue = sensorValue;
  }
}
