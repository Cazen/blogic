package app.metatron.flows.bistel.function;

import app.metatron.flows.bistel.model.RawDataVO;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class AlarmLimitAverageProcess extends KeyedProcessFunction<String, RawDataVO, Double> {
    private ValueState<List<RawDataVO>> splitDataListState;
    private ValueState<Boolean> previousState;

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<List<RawDataVO>> splitDataListStateDescriptor = new ValueStateDescriptor<>(
                // state name
                "Rawdata-List",
                // type information of state
                TypeInformation.of(new TypeHint<List<RawDataVO>>() {
                }));

        ValueStateDescriptor<Boolean> previousStateDescriptor = new ValueStateDescriptor<>("Previous-State", Boolean.class);

        splitDataListState = getRuntimeContext().getState(splitDataListStateDescriptor);
        previousState = getRuntimeContext().getState(previousStateDescriptor);
    }

    @Override
    public void processElement(RawDataVO rawDataVO, Context context, Collector<Double> collector) throws Exception {
        List<RawDataVO> rawDataList = splitDataListState.value();

        if(rawDataList == null) {
            rawDataList = new ArrayList<>();
        }
        System.out.println("Process : " +  rawDataVO);
        rawDataList.add(rawDataVO);
        splitDataListState.update(rawDataList);
        context.timerService().registerEventTimeTimer(rawDataVO.getTimestamp().getTime() + 1000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<Double> out) throws Exception {
        TimerService timerService = context.timerService();

        List<RawDataVO> rawDataList = splitDataListState.value();
        System.out.println(rawDataList);
        if(rawDataList == null) {
            rawDataList = new ArrayList<>();
        }

        Boolean previous = previousState.value();
        if(previous == null) {
            previous = false;
        }
    }
}
