package uc.mei.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.Random;

import static java.lang.ProcessBuilder.Redirect.to;

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
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
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
        KStream<Integer, String> salesOrders = builder.stream(inputTopic_sales)
                .filter((key, value) -> String.valueOf(value).split(";").length == 6)
                .groupBy((key, value) -> Integer.valueOf(String.valueOf(value).split(";")[0]))
                .aggregate(
                        // Initiate the aggregate value
                        () -> null,
                        // adder (doing nothing, just passing the user through as the value)
                        (key, value, total) -> value
                ).mapValues((values) -> String.valueOf(values))
                .toStream();


        KStream<Integer, String> purchasesOrders = builder.stream(inputTopic_purchases).filter((key, value) -> String.valueOf(value).split(";").length == 6)
                .groupBy((key, value) -> Integer.valueOf(String.valueOf(value).split(";")[0]))
                .aggregate(
                        // Initiate the aggregate value
                        () -> null,
                        // adder (doing nothing, just passing the user through as the value)
                        (key, value, total) -> value
                ).mapValues((values) -> String.valueOf(values))
                .toStream();

        KStream<Integer, String> joinedOrders = salesOrders
                .join(purchasesOrders,
                        (leftValue, rightValue) -> leftValue + ';' + rightValue,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)));

        // STREAMS FOR SOCK TYPE
        KStream<Integer, String> purchasesOrdersTypes= builder.stream(inputTopic_sales)
                .filter((key, value) -> String.valueOf(value).split(";").length == 6)
                .groupBy((key, value) -> convertStringToNumber(String.valueOf(value).split(";")[3]))
                .aggregate(() -> "0.0",
                        (key, value, total) -> total)
                .toStream();

        //Requirements
        //5. Get the revenue per sock pair sale.
        salesOrders
                .mapValues((key, value) -> value.split(";")[2] + '-' +  value.split(";")[3] + '[' +  key + "] teve uma venda no valor de " + String.format(java.util.Locale.US,"%.2f",Float.parseFloat(value.split(";")[4]) * Float.parseFloat(value.split(";")[5])) + "€")
                .to("topic-5");

        //6. Get the expenses per sock pair sale.
        purchasesOrders
                .mapValues((key, value) -> value.split(";")[2] + '-' +  value.split(";")[3] + '[' +  key + "] foi realizada uma compra no valor de " + String.format(java.util.Locale.US,"%.2f",Float.parseFloat(value.split(";")[4]) * Float.parseFloat(value.split(";")[5])) + "€")
                .to("topic-6");


        //7. Get the profit per sock pair sale.
        joinedOrders
                .filter((key, value) -> value.split(";").length == 12)
                .mapValues((key, value) -> String.format(java.util.Locale.US, "%s : %.2f€", value.split(";")[2] + '-' +  value.split(";")[3] + '[' +  key + "] teve um lucro de ", ((Float.valueOf(String.valueOf(value).split(";")[4]) * Float.valueOf(String.valueOf(value).split(";")[5])) - (Float.valueOf(String.valueOf(value).split(";")[10]) * Float.valueOf(String.valueOf(value).split(";")[11]))) ))
                .to("topic-7");


        //8. Get the total revenues.
        salesOrders
                .mapValues((value) -> String.valueOf(Float.parseFloat(value.split(";")[4]) * Integer.parseInt(value.split(";")[5])))
                .groupBy((key, value) -> 100)
                .reduce((total, current) -> String.format(java.util.Locale.US,"%.2f", Float.parseFloat(total) + Float.parseFloat(current)))
                .toStream()
                .to("topic-8");



        //9. Get the total expenses.
        purchasesOrders
                .mapValues((value) -> String.valueOf(Float.parseFloat(value.split(";")[4]) * Integer.parseInt(value.split(";")[5])))
                .groupBy((key, value) -> 100)
                .reduce((total, current) -> String.format(java.util.Locale.US,"%.2f", Float.valueOf(total) + Float.valueOf(current)))
                .toStream()
                .to("topic-9");


        //10. Get the total profit.
        joinedOrders
                .mapValues((value) -> String.valueOf((Float.parseFloat(value.split(";")[4]) * Integer.parseInt(value.split(";")[5])) - (Float.parseFloat(value.split(";")[10]) * Integer.parseInt(value.split(";")[11])) ))
                .groupBy((key, value) -> 100)
                .reduce((total, current) -> String.format(java.util.Locale.US ,"%.2f",Float.parseFloat(total) + Float.parseFloat(current)))
                .toStream()
                .to("topic-10");

        //11. Get the average amount spent in each purchase (separated by sock type).
        purchasesOrders
                .groupBy((key,value) -> convertStringToNumber(value.split(";")[3]))
                .aggregate(
                        // Initiate the aggregate value
                        () -> null,
                        // adder (doing nothing, just passing the user through as the value)
                        (applicationId, user, aggValue) -> user
                ).mapValues((values) ->String.valueOf(Float.valueOf(String.valueOf(values).split(";")[4]) * Integer.parseInt(String.valueOf(values).split(";")[5])))
                .toStream()
                .to("topic-11");

        //12. Get the average amount spent in each purchase (aggregated for all socks).

        //13. Get the sock type with the highest profit of all (only one if there is a tie).

        //14. Get the total revenue in the last hour1 (use a tumbling time window).
        Duration windowDurationSales = Duration.ofSeconds(120);
        TimeWindows tumbWindowSales = TimeWindows.ofSizeWithNoGrace(windowDurationSales);

        salesOrders
                .mapValues((key,value) -> String.format(java.util.Locale.US, "%.2f", Float.parseFloat(value.split(";")[4]) * Float.parseFloat(value.split(";")[5])))
                .groupBy((key, value) -> 100)
                .windowedBy(tumbWindowSales)
                .reduce((total, current) -> String.format(java.util.Locale.US,"%.2f", Float.parseFloat(total) + Float.parseFloat(current)))
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(), value.toString()))
                .to("topic-14");

        //15. Get the total expenses in the last hour (use a tumbling time window).
        Duration windowDurationPurchases= Duration.ofSeconds(120);
        TimeWindows tumbWindowPurchases= TimeWindows.ofSizeWithNoGrace(windowDurationPurchases);

        purchasesOrders
                .mapValues((key,value) -> String.format(java.util.Locale.US, "%.2f", Float.parseFloat(value.split(";")[4]) * Float.parseFloat(value.split(";")[5])))
                .groupBy((key, value) -> 100)
                .windowedBy(tumbWindowPurchases)
                .reduce((total, current) -> String.format(java.util.Locale.US,"%.2f", Float.parseFloat(total) + Float.parseFloat(current)))
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(), value.toString()))
                .to("topic-15");

        //16. Get the total profits in the last hour (use a tumbling time window).
        Duration windowDurationProfit = Duration.ofSeconds(120);
        TimeWindows tumbWindowProfit = TimeWindows.ofSizeWithNoGrace(windowDurationProfit);
        joinedOrders
                .mapValues((key,value) -> String.format(java.util.Locale.US, "%.2f", (Float.parseFloat(value.split(";")[4]) * Float.parseFloat(value.split(";")[5]) - (Float.parseFloat(value.split(";")[10]) * Float.parseFloat(value.split(";")[11])))))
                .groupBy((key, value) -> 100)
                .windowedBy(tumbWindowProfit)
                .reduce((total, current) -> String.format(java.util.Locale.US,"%.2f", Float.parseFloat(total) + Float.parseFloat(current)))
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(), value.toString()))
                .to("topic-16");

        //17. Get the name of the sock supplier generating the highest profit sales. Include the value of such sales.
        joinedOrders
                .groupBy((key,value) -> Integer.parseInt(value.split(";")[1]));
        // start kafkaStreams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    }

    private static int convertStringToNumber(String str) {
        int result = 0;
        for (int i = 0; i < str.length(); i++) {
            result += (int) str.charAt(i);
        }
        return result;
    }
}
