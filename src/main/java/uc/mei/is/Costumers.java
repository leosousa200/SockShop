package uc.mei.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uc.mei.is.models.RecordInfo;

import java.time.Duration;
import java.util.*;

public class Costumers {
    private static final Logger log = LoggerFactory.getLogger(Costumers.class);
    private static final int MAX_PERC_PROFIT = 50;
    public static void main(String[] args) {
        //general properties
        String brokersIP = "127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094";
        String inputTopic = "db_info_sales";
        String outputTopic = "sock_sales_topic";
        // String groupId = "purchaseGroup";

        //consumer properties
        Properties prodProperties = new Properties();
        prodProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersIP);
        prodProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  StringSerializer.class.getName());
        prodProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<>(prodProperties);


        //consumer properties
        Properties consProperties = new Properties();
        consProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersIP);
        consProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProperties);
        consumer.subscribe(Arrays.asList(inputTopic));

        ObjectMapper objectMapper = new ObjectMapper();
        Random rand = new Random();
        List<RecordInfo> supList = null;


        while(true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(12000));

            supList = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
                String rcv = record.value();
                JsonNode jsonNode = null;
                try {
                    jsonNode = objectMapper.readTree(rcv);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

                JsonNode payloadNode = jsonNode.get("payload");
                RecordInfo recordInfo = new RecordInfo();

                // set strings
                recordInfo.setName(payloadNode.get("name").asText());
                recordInfo.setType(payloadNode.get("type").asText());

                // set IDs
                recordInfo.setSockId(payloadNode.get("id").asInt());
                recordInfo.setSupplierId(payloadNode.get("supplier_id").asInt());

                // random sale price 1.00 * MAX_PERC_PROFIT
                recordInfo.setPrice(payloadNode.get("buy_price").floatValue()*( (float) 1 + ((float) rand.nextInt(MAX_PERC_PROFIT) + 1) /100 ));

                supList.add(recordInfo);
            }

            int nrSocks = supList.size();
            if (nrSocks > 0) {
                int quant = rand.nextInt(5) + 1;
                try {
                    RecordInfo chosen = supList.get(rand.nextInt(nrSocks));
                    ProducerRecord<String, String> producerRecord =
                            new ProducerRecord<>(outputTopic, chosen.getSockId() + ";" +chosen.getSupplierId() +
                                    ";" + chosen.getName() + ";" + chosen.getType()
                                    + ";" + String.format(java.util.Locale.US,"%.2f", chosen.getPrice())  + ";" + quant);

                    producer.send(producerRecord);

                    // flush data - synchronous
                    producer.flush();
                } catch (Exception e) {
                }
                supList.clear();
            }
        }

    }
}
