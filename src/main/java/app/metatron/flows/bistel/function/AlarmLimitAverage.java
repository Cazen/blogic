package app.metatron.flows.bistel.function;

import app.metatron.flows.bistel.model.RawDataVO;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.List;

public class AlarmLimitAverage extends RichFlatMapFunction<RawDataVO, Double> {
    private ValueState<List<RawDataVO>> rawDataListState;
    private ValueState<Boolean> previousState;

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
    public void flatMap(RawDataVO rawDataVO, Collector<Double> out) throws Exception {
        Boolean previous = previousState.value();
        List<RawDataVO> rawDataList = rawDataListState.value();
        Boolean current = rawDataVO.getP1SensorValue() > 0.5 ? true : false;

        rawDataList.add(rawDataVO);
        if (previous != null) {
            //will be different when false -> true and true -> false
            if(current != previous) {
                Double sum = rawDataList.stream().mapToDouble(RawDataVO::getP1SensorValue).sum();
                out.collect(sum / rawDataList.size());
            } else { // means that true -> true and false -> false

            }
        }
    }
}