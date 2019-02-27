package app.metatron.flows.bistel;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

import app.metatron.flows.bistel.function.CustomMapSplitter;
import app.metatron.flows.bistel.model.RawDataVO;
import app.metatron.flows.bistel.util.GetAzureProperties;

public class BistelLogic1Test {
  public static void main(String[] args) throws Exception {
    //Init Streaming Environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Properties properties = GetAzureProperties.createProperties();

    //Get InputStream from selected topic
    DataStream<String> stream = env.addSource(new FlinkKafkaConsumer011(
        "pdm-input-trace", new SimpleStringSchema(), properties));

//    //Split into multiple stream by sensor name
//    SingleOutputStreamOperator<SplitDataVO> inputStream = stream.flatMap(new FlatMapSplitter());
//
//    DataStream<SplitDataVO> p1Stream = inputStream.filter(x -> x.getSensorName().equals("P1"));
//    DataStream<SplitDataVO> p2Stream = inputStream.filter(x -> x.getSensorName().equals("P2"));

    DataStream<RawDataVO> rawDataStream = stream.map(new CustomMapSplitter());

  }


}
