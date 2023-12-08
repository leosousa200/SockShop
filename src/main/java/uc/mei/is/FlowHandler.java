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
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

public class FlowHandler {
    private static final Logger log = LoggerFactory.getLogger(FlowHandler.class);

    public static void main(String[] args) {

        //setting the 3 brokers available
        String bootstrapServers = "127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094";

        //input topics
        String inputTopic_sales = "sock_sales_topic";
        String inputTopic_purchases = "sock_purchases_topic";

        //defining properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "flow_handler");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        // STRING SALE RECEIVED FROM INPUT TOPICS:
        // TAM: 6
        // 0 -> SOCK ID
        // 1 -> SUPPLIER ID
        // 2 -> NAME
        // 3 -> TYPE
        // 4 -> SALE/PURCHASE PRICE
        // 5 -> QUANTITY

        //streams setup
        StreamsBuilder builder = new StreamsBuilder();

        // sales stream , id -> ref da meia
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

        // purchases stream, id -> ref da meia
        KStream<Integer, String> purchasesOrders = builder.stream(inputTopic_purchases).filter((key, value) -> String.valueOf(value).split(";").length == 6)
                .groupBy((key, value) -> Integer.valueOf(String.valueOf(value).split(";")[0]))
                .aggregate(
                        // Initiate the aggregate value
                        () -> null,
                        // adder (doing nothing, just passing the user through as the value)
                        (key, value, total) -> value
                ).mapValues((values) -> String.valueOf(values))
                .toStream();

        // joined stream (sales vs purchases)
        KStream<Integer, String> joinedOrders = salesOrders
                .join(purchasesOrders,
                        (leftValue, rightValue) -> leftValue + ';' + rightValue,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)));



        //Requirements --------------------------------------------------------------------



        //5. Get the revenue per sock pair sale.
        // Faz um map dos valores para colocar o vendedor, tipo de meia e valor total da venda
        salesOrders
                .mapValues((key, value) -> value.split(";")[2] + '-' + value.split(";")[3] + '[' + key + "] teve uma venda no valor de " + String.format(java.util.Locale.US, "%.2f", Float.parseFloat(value.split(";")[4]) * Float.parseFloat(value.split(";")[5])) + "€")
                .to("topic-5");

        //6. Get the expenses per sock pair sale.
        // Faz um map dos valores para colocar de onde foi comprado, tipo de meia e valor total da compra
        purchasesOrders
                .mapValues((key, value) -> value.split(";")[2] + '-' + value.split(";")[3] + '[' + key + "] foi realizada uma compra no valor de " + String.format(java.util.Locale.US, "%.2f", Float.parseFloat(value.split(";")[4]) * Float.parseFloat(value.split(";")[5])) + "€")
                .to("topic-6");


        //7. Get the profit per sock pair sale.
        // Faz um map dos valores para verificar o lucro em si (ganhos-gastos)
        joinedOrders
                .filter((key, value) -> value.split(";").length == 12)
                .mapValues((key, value) -> String.format(java.util.Locale.US, "%s : %.2f€", value.split(";")[2] + '-' + value.split(";")[3] + '[' + key + "] teve um lucro de ", ((Float.valueOf(String.valueOf(value).split(";")[4]) * Float.valueOf(String.valueOf(value).split(";")[5])) - (Float.valueOf(String.valueOf(value).split(";")[10]) * Float.valueOf(String.valueOf(value).split(";")[11])))))
                .to("topic-7");


        //8. Get the total revenues.
        // Faz um map dos valores para obter ganhos totais, junta todos no mesmo grupo e junta com o já existente.
        salesOrders
                .mapValues((value) -> String.valueOf(Float.parseFloat(value.split(";")[4]) * Integer.parseInt(value.split(";")[5])))
                .groupBy((key, value) -> 100)
                .reduce((total, current) -> String.format(java.util.Locale.US, "%.2f", Float.parseFloat(total) + Float.parseFloat(current)))
                .toStream()
                .to("topic-8");


        //9. Get the total expenses.
        // Faz um map dos valores para obter gastos totais, junta todos no mesmo grupo e junta com o já existente.
        purchasesOrders
                .mapValues((value) -> String.valueOf(Float.parseFloat(value.split(";")[4]) * Integer.parseInt(value.split(";")[5])))
                .groupBy((key, value) -> 100)
                .reduce((total, current) -> String.format(java.util.Locale.US, "%.2f", Float.valueOf(total) + Float.valueOf(current)))
                .toStream()
                .to("topic-9");


        //10. Get the total profit.
        // Faz um map dos valores para obter lucros (ganhos-gastos), junta todos no mesmo grupo e junta com o já existente.
        joinedOrders
                .mapValues((value) -> String.valueOf((Float.parseFloat(value.split(";")[4]) * Integer.parseInt(value.split(";")[5])) - (Float.parseFloat(value.split(";")[10]) * Integer.parseInt(value.split(";")[11]))))
                .groupBy((key, value) -> 100)
                .reduce((total, current) -> String.format(java.util.Locale.US, "%.2f", Float.parseFloat(total) + Float.parseFloat(current)))
                .toStream()
                .to("topic-10");

        //11. Get the average amount spent in each purchase (separated by sock type).
        // Junta todas as meias que tiverem o mesmo tipo, para cada grupo junta o nome do tipo, quantidade total e
        // gastos totais, faz map dos valores para organizar a informação.
        purchasesOrders
                .groupBy((key, value) -> convertStringToNumber(value.split(";")[3]))
                .aggregate(
                        // Initiate the aggregate value
                        () -> "0;0.0",
                        // adder (doing nothing, just passing the user through as the value)
                        (key, value, total) -> String.format(Locale.US,"%s;%s;%s",(String.valueOf(Integer.valueOf(total.split(";")[0]) + Integer.valueOf(value.split(";")[5]))),
                                (String.valueOf(Float.valueOf(total.split(";")[1]) + Integer.valueOf(value.split(";")[5]) * Float.valueOf(value.split(";")[4]))),value.split(";")[3]))
                        .mapValues((key, value) -> String.format(Locale.US,"Tipo %s , preco medio : %.2f€",value.split(";")[2],Float.valueOf(value.split(";")[1])/Integer.valueOf(value.split(";")[0])))
                    .toStream().to("topic-11");


        //12. Get the average amount spent in each purchase (aggregated for all socks).
        // Junta todas as meias, agrega a quantidade total e
        // gastos totais, faz map dos valores para organizar a informação.
        purchasesOrders
                .groupBy((key, value) -> 100)
                .aggregate(
                        // Initiate the aggregate value
                        () -> "0;0.0",
                        // adder (doing nothing, just passing the user through as the value)
                        (key, value, total) -> String.format(Locale.US,"%s;%s", (String.valueOf(Integer.valueOf(total.split(";")[0]) + Integer.valueOf(value.split(";")[5]))),
                                (String.valueOf(Float.valueOf(total.split(";")[1]) + Integer.valueOf(value.split(";")[5]) * Float.valueOf(value.split(";")[4])))))
                        .mapValues((key, value) -> String.format(Locale.US,"Preco medio de um par de meias: %.2f€",Float.valueOf(value.split(";")[1])/Integer.valueOf(value.split(";")[0])))
                .toStream().to("topic-12");

        //13. Get the sock type with the highest profit of all (only one if there is a tie).
        // Junta todas as meias que teem o mesmo tipo, para cada grupo junta o lucro e o nome
        // do tipo de meia, junta todas para conseguir encontrar a que teve maior lucro,
        // faz map dos valores para personalizar o texto

        joinedOrders
                .groupBy((key, value) -> convertStringToNumber(value.split(";")[3]))
                .aggregate(
                        // Initiate the aggregate value
                        () -> "0.0;a",
                        // adder (doing nothing, just passing the user through as the value)
                        (key, value, total) -> String.format(Locale.US,"%s;%s",
                                (String.valueOf(Float.valueOf(total.split(";")[0]) + ((Integer.valueOf(value.split(";")[5]) * Float.valueOf(value.split(";")[4]))) - (Integer.valueOf(value.split(";")[11]) * Float.valueOf(value.split(";")[10]))) ), value.split(";")[3]))
                .toStream()
                .groupBy((key, value) -> 100)
                .aggregate(
                        // Initiate the aggregate value
                        () -> "0.0;a",
                        // adder (doing nothing, just passing the user through as the value)
                        (key, value, total) -> {
                            if(Float.valueOf(total.split(";")[0]) < Float.valueOf(value.split(";")[0]))
                                return value;
                            return total;
                        })
                .mapValues((key, value) -> String.format(Locale.US,"Tipo mais lucrativo %s: %.2f€",value.split(";")[1],Float.valueOf(value.split(";")[0])))
                .toStream().to("topic-13");


        //14. Get the total revenue in the last hour1 (use a tumbling time window).
        // Map dos valores para ter o ganho, junta todas as meias no mesmo grupo
        // aplica uma tumbling de 1 hora e junta os valores

        Duration windowDurationSales = Duration.ofSeconds(120);
        TimeWindows tumbWindowSales = TimeWindows.ofSizeWithNoGrace(windowDurationSales);

        salesOrders
                .mapValues((key, value) -> String.format(java.util.Locale.US, "%.2f", Float.parseFloat(value.split(";")[4]) * Float.parseFloat(value.split(";")[5])))
                .groupBy((key, value) -> 100)
                .windowedBy(tumbWindowSales)
                .reduce((total, current) -> String.format(java.util.Locale.US, "%.2f", Float.parseFloat(total) + Float.parseFloat(current)))
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(), value.toString()))
                .to("topic-14");

        //15. Get the total expenses in the last hour (use a tumbling time window).
        // Map dos valores para ter o gasto, junta todas as meias no mesmo grupo
        // aplica uma tumbling de 1 hora e junta os valores

        Duration windowDurationPurchases = Duration.ofSeconds(120);
        TimeWindows tumbWindowPurchases = TimeWindows.ofSizeWithNoGrace(windowDurationPurchases);

        purchasesOrders
                .mapValues((key, value) -> String.format(java.util.Locale.US, "%.2f", Float.parseFloat(value.split(";")[4]) * Float.parseFloat(value.split(";")[5])))
                .groupBy((key, value) -> 100)
                .windowedBy(tumbWindowPurchases)
                .reduce((total, current) -> String.format(java.util.Locale.US, "%.2f", Float.parseFloat(total) + Float.parseFloat(current)))
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(), value.toString()))
                .to("topic-15");

        //16. Get the total profits in the last hour (use a tumbling time window).
        // Map dos valores para ter o lucro, junta todas as meias no mesmo grupo
        // aplica uma tumbling de 1 hora e junta os valores

        Duration windowDurationProfit = Duration.ofSeconds(120);
        TimeWindows tumbWindowProfit = TimeWindows.ofSizeWithNoGrace(windowDurationProfit);
        joinedOrders
                .mapValues((key, value) -> String.format(java.util.Locale.US, "%.2f", (Float.parseFloat(value.split(";")[4]) * Float.parseFloat(value.split(";")[5]) - (Float.parseFloat(value.split(";")[10]) * Float.parseFloat(value.split(";")[11])))))
                .groupBy((key, value) -> 100)
                .windowedBy(tumbWindowProfit)
                .reduce((total, current) -> String.format(java.util.Locale.US, "%.2f", Float.parseFloat(total) + Float.parseFloat(current)))
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(), value.toString()))
                .to("topic-16");

        //17. Get the name of the sock supplier generating the highest profit sales. Include the value of such sales.
        // Junta as meias que veem do mesmo supplier, para cada grupo junta o lucro e o nome
        // do supplier, junta todas para conseguir encontrar o supplier que teve maior lucro,
        // faz map dos valores para personalizar o texto

        joinedOrders
                .groupBy((key, value) -> Integer.valueOf(value.split(";")[1]))
                .aggregate(
                        // Initiate the aggregate value
                        () -> "0.0;a",
                        // adder (doing nothing, just passing the user through as the value)
                        (key, value, total) -> String.format(Locale.US,"%s;%s",
                                (String.valueOf(Float.valueOf(total.split(";")[0]) + ((Integer.valueOf(value.split(";")[5]) * Float.valueOf(value.split(";")[4]))) - (Integer.valueOf(value.split(";")[11]) * Float.valueOf(value.split(";")[10]))) ), value.split(";")[2]))
                .toStream()
                .groupBy((key, value) -> 100)
                .aggregate(
                        // Initiate the aggregate value
                        () -> "0.0;a",
                        // adder (doing nothing, just passing the user through as the value)
                        (key, value, total) -> {
                            if(Float.valueOf(total.split(";")[0]) < Float.valueOf(value.split(";")[0]))
                                return value;
                            return total;
                        })
                .mapValues((key, value) -> String.format(Locale.US,"Supplier gerando mais lucro %s: %.2f€",value.split(";")[1],Float.valueOf(value.split(";")[0])))
                .toStream().to("topic-17");


        // start kafkaStreams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    }

    // transforma uma string num número único
    private static int convertStringToNumber(String str) {
        int result = 0;
        for (int i = 0; i < str.length(); i++) {
            result += (int) str.charAt(i);
        }
        return result;
    }

}
