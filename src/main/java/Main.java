import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;


public class Main {
    private final static String server = "localhost";
    private final static String port = "1883";
    private final static String publisherId = UUID.randomUUID().toString();
    private final static String topic = "newOne";
    private static int counter = 0;

    public static void main(String[] args) throws MqttException, IOException {
        IMqttClient mqttClient = new MqttClient("tcp://"+server+":"+port, publisherId);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);
        mqttClient.connect(options);

        try (Stream<Path> traces = Files.walk(Paths.get("src/main/resources/traces"))) {
            traces.filter(Files::isRegularFile).filter(f -> f.toString().endsWith(".json")).map(t -> {
                try {
                    counter++;
                    System.out.println("TRACE "+counter+".");
                    System.out.println(t);
                    return Files.readAllBytes(t);
                } catch (IOException e) {
                    e.printStackTrace();
                    return "".getBytes();
                }
            }).forEach( t -> {
                System.out.println(new String(t));
                System.out.println();
                try {
                    mqttClient.publish(topic, t, 0, false);
                } catch (MqttException e) {
                    e.printStackTrace();
                }
            });
        }

        System.out.println("FINISHED!");
    }
}
