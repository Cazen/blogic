package app.metatron.flows.bistel.function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.List;

import app.metatron.flows.bistel.model.RawDataVO;

public class AlarmLimitAverage extends RichFlatMapFunction<RawDataVO, Double> {
  private ValueState<List<RawDataVO>> rawDataListState = null;
  private ValueState<Boolean> previousState = null;

  @Override
  public void open(Configuration config) {
    ValueStateDescriptor<List<RawDataVO>> rawDataListStateDescriptor = new ValueStateDescriptor<>(
        // state name
        "Rawdata-List",
        // type information of state
        TypeInformation.of(new TypeHint<List<RawDataVO>>() {
        }));
    ValueStateDescriptor<Boolean> previousStateDescriptor = new ValueStateDescriptor<>("Previous-State", Boolean.class);

    rawDataListState = getRuntimeContext().getState(rawDataListStateDescriptor);
    previousState = getRuntimeContext().getState(previousStateDescriptor);
  }

  @Override
  public void flatMap(RawDataVO value, Collector<Double> out) throws Exception {

    boolean previous = previousState.value();
    if(previous == null) {
      
    }
  }
}