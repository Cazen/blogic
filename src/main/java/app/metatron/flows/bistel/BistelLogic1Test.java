package app.metatron.flows.bistel;

import app.metatron.flows.bistel.function.AlarmLimitAverage;
import app.metatron.flows.bistel.function.AlarmLimitAverageProcess;
import app.metatron.flows.bistel.function.CustomMapSplitter;
import app.metatron.flows.bistel.model.RawDataVO;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import app.metatron.flows.bistel.function.FlatMapSplitter;
import app.metatron.flows.bistel.model.SplitDataVO;

public class BistelLogic1Test {
  public static void main(String[] args) throws Exception {
    //Init Streaming Environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //Properties properties = GetAzureProperties.createProperties();

    //Get InputStream from selected topic
    //    DataStream<String> stream = env.addSource(new FlinkKafkaConsumer011(
    //        "pdm-input-trace", new SimpleStringSchema(), properties));

    DataStream<String> stream = env.fromElements(
            "2018-10-20 18:22:33.433,0.43439,234,100,0.4443",//OFF
            "2018-10-20 18:22:34.433,0.43434,123,100,0.4443",//OFF
            "2018-10-20 18:22:35.433,0.53421,244,100,0.4443",//OFF
            "2018-10-20 18:22:36.433,0.56756,100,100,0.4443",//ON
            "2018-10-20 18:22:37.433,0.56755,200,100,0.4443",//ON
            "2018-10-20 18:22:38.433,0.87657,300,100,0.4443",//ON
            "2018-10-20 18:22:39.433,0.57675,400,100,0.4443",//ON
            "2018-10-20 18:22:40.433,0.50001,500,100,0.4443",//ON
            "2018-10-20 18:22:41.433,0.48897,600,100,0.4443",//OFF
            "2018-10-20 18:22:42.433,0.43395,700,100,0.4443"//OFF
    );

    //Split into multiple stream by sensor name
    //SingleOutputStreamOperator<SplitDataVO> inputStream = stream.flatMap(new FlatMapSplitter());

//    DataStream<SplitDataVO> p1Stream = inputStream.filter(x -> x.getSensorName().equals("P1")).keyBy(x -> x.getSensorName());
//    DataStream<SplitDataVO> p2Stream = inputStream.filter(x -> x.getSensorName().equals("P2")).keyBy(x -> x.getSensorName());
//    DataStream<SplitDataVO> p3Stream = inputStream.filter(x -> x.getSensorName().equals("P3")).keyBy(x -> x.getSensorName());
//    DataStream<SplitDataVO> p4Stream = inputStream.filter(x -> x.getSensorName().equals("P4")).keyBy(x -> x.getSensorName());

    DataStream<RawDataVO> rawDataStream = stream.map(new CustomMapSplitter());
    rawDataStream.keyBy(rawDataVO -> "NoKey").process(new AlarmLimitAverageProcess()).print();

    //DataStream<RawDataVO> rawDataStream = stream.map(new CustomMapSplitter());


    env.execute("BistelLogic1Test");
  }


}
