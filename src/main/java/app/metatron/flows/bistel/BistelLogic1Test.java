package app.metatron.flows.bistel;

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

    DataStream<String> stream = env.fromElements("2018-10-20 18:22:33.433,0.43434,234,100,0.4443");

    //Split into multiple stream by sensor name
    SingleOutputStreamOperator<SplitDataVO> inputStream = stream.flatMap(new FlatMapSplitter());

    DataStream<SplitDataVO> p1Stream = inputStream.filter(x -> x.getSensorName().equals("P1")).keyBy(x -> x.getTimestamp());
    DataStream<SplitDataVO> p2Stream = inputStream.filter(x -> x.getSensorName().equals("P2")).keyBy(x -> x.getTimestamp());
    DataStream<SplitDataVO> p3Stream = inputStream.filter(x -> x.getSensorName().equals("P3")).keyBy(x -> x.getTimestamp());
    DataStream<SplitDataVO> p4Stream = inputStream.filter(x -> x.getSensorName().equals("P4")).keyBy(x -> x.getTimestamp());

    //DataStream<RawDataVO> rawDataStream = stream.map(new CustomMapSplitter());


    env.execute("BistelLogic1Test");
  }


}
