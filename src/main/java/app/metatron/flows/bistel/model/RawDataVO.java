package app.metatron.flows.bistel.model;

import java.util.Date;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class RawDataVO {
  Date timestamp;
  Double p1SensorValue;
  Double p2SensorValue;
  Double p3SensorValue;
  Double p4SensorValue;
  Double p5SensorValue;
  Double p6SensorValue;
  Double p7SensorValue;
  Double p8SensorValue;
  Double p9SensorValue;

  public RawDataVO(Date timestamp, Double p1SensorValue, Double p2SensorValue, Double p3SensorValue,
                   Double p4SensorValue, Double p5SensorValue, Double p6SensorValue, Double p7SensorValue,
                   Double p8SensorValue, Double p9SensorValue) {
    this.timestamp = timestamp;
    this.p1SensorValue = p1SensorValue;
    this.p2SensorValue = p2SensorValue;
    this.p3SensorValue = p3SensorValue;
    this.p4SensorValue = p4SensorValue;
    this.p5SensorValue = p5SensorValue;
    this.p6SensorValue = p6SensorValue;
    this.p7SensorValue = p7SensorValue;
    this.p8SensorValue = p8SensorValue;
    this.p9SensorValue = p9SensorValue;
  }
}
