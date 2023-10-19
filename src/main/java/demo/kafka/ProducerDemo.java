// Kafka Producer probably unnecessary for our task but for simplicity

package demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) throws InterruptedException {
        log.info("I am a Kafka Producer");

        String bootstrapservers = "127.0.0.1:9092"; //List of bootstrap severs to be added later

        // create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // create new Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // List of animal names to send
        String[] animals = {"Lion", "Tiger", "Bear", "Elephant", "Giraffe"};

        try {
            while (true){
                for (String animal : animals){
                // create a producer record with a topic and the animal name as the value 
                    ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", animal);
                    producer.send(record, (RecordMetadata, e) -> {
                        if (e == null) {
                            log.info("Received new metadata. \n" + animal);

                        } else {
                            log.error("Error while producing ", e); 
                        }
                    });
            
            // Sleep for 10 seconds before sending next set of animal names
            TimeUnit.SECONDS.sleep(10);
        }
    } 
       
    }finally  {
    // flush data - synchronous
        producer.flush();
        producer.close();}
    }
}


    

