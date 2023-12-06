package uc.mei.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.Random;

public class FlowHandler {
    private static final Logger log = LoggerFactory.getLogger(FlowHandler.class);

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:29092";
        String inputTopic_sales = "sock_sales_topic";
        String inputTopic_purchases = "sock_purchases_topic";
        String outputTopic = "result_topic";

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "flow_handler");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        ObjectMapper objectMapper = new ObjectMapper();
        Random rand = new Random();



        // Get from a Topic
        StreamsBuilder builder = new StreamsBuilder();

        // STRING SALE RECEBIDA:
        // TAM: 6
        // 0 -> SOCK ID
        // 1 -> SUPPLIER ID
        // 2 -> NAME
        // 3 -> TYPE
        // 4 -> SALE/PURCHASE PRICE
        // 5 -> QUANTITY

        //stream setup
        KStream<String, String> salesOrders = builder.stream(inputTopic_sales)
                .filter((key, value) -> String.valueOf(value).split(";").length == 5)
                .groupBy((key, value) -> String.valueOf(value).split(";")[0] + ';' + String.valueOf(value).split(";")[1])
                .aggregate(
                        // Initiate the aggregate value
                        () -> null,
                        // adder (doing nothing, just passing the user through as the value)
                        (applicationId, user, aggValue) -> user
                ).mapValues((values) -> String.valueOf(values))
                .toStream();

        KStream<String, String> purchasesOrders = builder.stream(inputTopic_purchases).filter((key, value) -> String.valueOf(value).split(";").length == 5)
                .groupBy((key, value) -> String.valueOf(value).split(";")[0] + ';' + String.valueOf(value).split(";")[1])
                .aggregate(
                        // Initiate the aggregate value
                        () -> null,
                        // adder (doing nothing, just passing the user through as the value)
                        (applicationId, user, aggValue) -> user
                ).mapValues((values) -> String.valueOf(values))
                .toStream();

        KStream<String, String> joinedOrders = salesOrders
                .join(purchasesOrders,
                        (leftValue, rightValue) -> leftValue + ';' + rightValue,
                        JoinWindows.of(Duration.ofSeconds(15)));


        //Requirements
        //5. Get the revenue per sock pair sale.
        salesOrders
                .mapValues((key, value) -> key + " : " + String.valueOf(value).split(";")[2])
                .to("topic-5");

        //6. Get the expenses per sock pair sale.
        purchasesOrders
                .mapValues((key, value) -> key + " : " + String.valueOf(value).split(";")[2])
                .to("topic-6");

        //7. Get the profit per sock pair sale.
        joinedOrders
                .filter((key, value) -> value.split(";").length == 10)
                .groupBy((key, value) -> value.split(";")[0] + ';' + value.split(";")[1])
                .aggregate(
                        // Initiate the aggregate value
                        () -> null,
                        // adder (doing nothing, just passing the user through as the value)
                        (applicationId, user, aggValue) -> user
                ).toStream()
                .mapValues((key, value) -> String.format("%s : %.2f", key, (Float.valueOf(String.valueOf(value).split(";")[2]) - Float.valueOf(String.valueOf(value).split(";")[7]))))
                .to("topic-7");


        //8. Get the total revenues.
        salesOrders
                .mapValues((value) -> value.split(";")[4])
                .groupBy((key, value) -> "total")
                .reduce((total, current) -> String.valueOf(Float.valueOf(total) + Float.valueOf(current)))
                .toStream()
                .to("topic-8");


        //9. Get the total expenses.
        purchasesOrders
                .mapValues((value) -> value.split(";")[4])
                .groupBy((key, value) -> "total")
                .reduce((total, current) -> String.valueOf(Float.valueOf(total) + Float.valueOf(current)))
                .toStream()
                .to("topic-9");

        //10. Get the total profit.
        joinedOrders
                .mapValues((value) -> {
                    log.error(value);
                    return String.valueOf(Float.valueOf(value.split(";")[4]) - Float.valueOf(value.split(";")[9]));
                })
                .groupBy((key, value) -> "total")
                .reduce((total, current) -> String.valueOf(Float.valueOf(total) + Float.valueOf(current)))
                .toStream()
                .to("topic-10");

        //11. Get the average amount spent in each purchase (separated by sock type).


        //12. Get the average amount spent in each purchase (aggregated for all socks).
        //13. Get the sock type with the highest profit of all (only one if there is a tie).
        //14. Get the total revenue in the last hour1 (use a tumbling time window).
        //15. Get the total expenses in the last hour (use a tumbling time window).
        //16. Get the total profits in the last hour (use a tumbling time window).
        //17. Get the name of the sock supplier generating the highest profit sales. Include the value of such sales.


        // start kafkaStreams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    }
}
