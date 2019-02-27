package app.metatron.flows.bistel.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class GetAzureProperties {
   //Connection Information
  public static Properties createProperties() {
    final Properties properties = new Properties();

    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    properties.put("bootstrap.servers", "sb://apm-eventhub-01.servicebus.windows.net:9093");
    properties.put("group.id", "flink");
    properties.put("sasl.mechanism", "PLAIN");
    properties.put("security.protocol", "SASL_SSL");
    properties.put(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\"  password=\"Endpoint=sb://apm-eventhub-01.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=3/R07ohe3xsNV2lS3xQlaQ/3D/Kgu0Z10n5ZjqrcHNY=\";\n"
    );
    return properties;
  }
}
