package app.metatron.flows.bistel.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import app.metatron.flows.bistel.model.RawDataVO;
import app.metatron.flows.bistel.model.SplitDataVO;

public class AlarmLimitAverage extends CoProcessFunction<SplitDataVO, SplitDataVO, Double> {
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
  public void processElement1(SplitDataVO splitDataVO, Context context, Collector<Double> out) throws Exception {
    TimerService timerService = context.timerService();
    Boolean previous = previousState.value();
    if(previous == null) {
      previous = false;
    }
    List<SplitDataVO> rawDataList = splitDataListState.value();

    if(rawDataList == null) {
      rawDataList = new ArrayList<>();
    }
    Boolean current = splitDataVO.getSensorValue() > 0.5;

//    if(previous && current) {// True -> True
//      //Do Nothing
//      System.out.println("True -> True(" + splitDataVO.getSensorValue() + ")");
//    } else if(previous && !current) {// True -> False
//      //Calculate
//      System.out.println("True -> True(" + splitDataVO.getSensorValue() + ")");
//      if(!rawDataList.isEmpty()) {
//        out.collect(calcAverageFromList(rawDataList));
//      }
//    } else if(!previous && current) {// False -> True
//      //Do Nothing
//      System.out.println("False -> True(" + splitDataVO.getSensorValue() + ")");
//    } else if(!previous && !current) {// False -> False
//      //Remove current list(not needed data)
//      System.out.println("False -> False(" + splitDataVO.getSensorValue() + ")");
//      splitDataListState.clear();
//    }
    previousState.update(current);
    timerService.registerEventTimeTimer(splitDataVO.getTimestamp().getTime());
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
  public void processElement2(SplitDataVO splitDataVO, Context context, Collector<Double> out) throws Exception {
    List<SplitDataVO> rawDataList = splitDataListState.value();
    if (rawDataList == null) {
      rawDataList = new ArrayList<>();
    }
    rawDataList.add(splitDataVO);
    splitDataListState.update(rawDataList);
  }

  @Override
  public void onTimer(long t, OnTimerContext context, Collector<Double> out) throws Exception {

  }

}