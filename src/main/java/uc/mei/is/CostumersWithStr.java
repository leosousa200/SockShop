package uc.mei.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uc.mei.is.models.RecordInfo;

import java.util.*;

public class CostumersWithStr {
    private static final Logger log = LoggerFactory.getLogger(CostumersWithStr.class);

    public static void main(String[] args) throws InterruptedException, JsonProcessingException {

        String bootstrapServers = "127.0.0.1:29092";
        String inputTopic = "db_info_sales";
        String outputTopic = "result_topic";

        // ==============================================================================
        // KAFKA STREAMS
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "costumers_app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        ObjectMapper objectMapper = new ObjectMapper();
        Random rand = new Random();

        // Get from a Topic
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> purchasesOrders = builder.stream(inputTopic);
        purchasesOrders.filter((key,value) -> rand.nextInt(10) == 5).mapValues((value) -> {
                    JsonNode jsonNode = null;
                    try {
                        jsonNode = objectMapper.readTree(value);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    JsonNode payloadNode = jsonNode.get("payload");
                    RecordInfo recordInfo = new RecordInfo();

                    recordInfo.setName(payloadNode.get("name").asText());
                    recordInfo.setType(payloadNode.get("type").asText());
                    recordInfo.setPrice(payloadNode.get("sell_price").floatValue());
                    int quant = rand.nextInt(5) + 1;
                    return (recordInfo.getName() + ";" + recordInfo.getType()
                            + ";" + recordInfo.getPrice() + ";" + quant + ";" + (quant* recordInfo.getPrice()));
                }
        ).to(outputTopic);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    }
}
