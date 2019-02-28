package app.metatron.flows.bistel.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;

import app.metatron.flows.bistel.model.RawDataVO;
import app.metatron.flows.bistel.model.SplitDataVO;

public class AlarmLimitAverage extends RichCoFlatMapFunction<SplitDataVO, SplitDataVO, Double> {
  private ValueState<List<SplitDataVO>> splitDataListState;
  private ValueState<Boolean> previousState;

  @Override
  public void open(Configuration config) {
    ValueStateDescriptor<List<SplitDataVO>> splitDataListStateDescriptor = new ValueStateDescriptor<>(
        // state name
        "Rawdata-List",
        // type information of state
        TypeInformation.of(new TypeHint<List<SplitDataVO>>() {
        }));
    ValueStateDescriptor<Boolean> previousStateDescriptor = new ValueStateDescriptor<>("Previous-State", Boolean.class);

    splitDataListState = getRuntimeContext().getState(splitDataListStateDescriptor);
    previousState = getRuntimeContext().getState(previousStateDescriptor);
  }

  @Override
  public void flatMap1(SplitDataVO splitDataVO, Collector<Double> out) throws Exception {
    Boolean previous = previousState.value();
    List<SplitDataVO> rawDataList = splitDataListState.value();

    Boolean current = splitDataVO.getSensorValue() > 0.5;

    if(previous && current) {// True -> True
      //Do Nothing
    } else if(previous && !current) {// True -> False
      //Calculate
      out.collect(calcAverageFromList(rawDataList));
    } else if(!previous && current) {// False -> True
      //Do Nothing

    } else if(!previous && !current) {// False -> False
      //Remove current list(not needed data)
      splitDataListState.clear();
    }
    previousState.update(current);
  }

  private Double calcAverageFromList(List<SplitDataVO> rawDataList) {
    long warnCount = rawDataList.stream().map(SplitDataVO::getSensorValue).filter(t -> t > 0.5).count();
    if(warnCount > 0) {
      return rawDataList.stream().mapToDouble(SplitDataVO::getSensorValue).sum() / rawDataList.size();
    } else {
      return rawDataList.stream().filter(t -> t.getSensorValue() > 0.5).mapToDouble(SplitDataVO::getSensorValue).sum() / warnCount;
    }
  }

  @Override
  public void flatMap2(SplitDataVO splitDataVO, Collector<Double> out) throws Exception {
    List<SplitDataVO> rawDataList = splitDataListState.value();
    if (rawDataList == null) {
      rawDataList = Collections.emptyList();
    }
    rawDataList.add(splitDataVO);
    splitDataListState.update(rawDataList);
  }
}