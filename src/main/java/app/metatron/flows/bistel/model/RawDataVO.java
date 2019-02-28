package app.metatron.flows.bistel.model;

import java.util.Date;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class RawDataVO {
  public Date timestamp;
  public double p1SensorValue;
  public double p2SensorValue;
  public double p3SensorValue;
  public double p4SensorValue;
  public double p5SensorValue;
  public double p6SensorValue;
  public double p7SensorValue;
  public double p8SensorValue;
  public double p9SensorValue;

  public RawDataVO(Date timestamp, double p1SensorValue, double p2SensorValue, double p3SensorValue,
                   double p4SensorValue, double p5SensorValue, double p6SensorValue, double p7SensorValue,
                   double p8SensorValue, double p9SensorValue) {
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
