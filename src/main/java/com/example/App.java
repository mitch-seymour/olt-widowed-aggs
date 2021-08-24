package com.example;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

class App {

  public static Topology getTopology() {
    Topology topology;
    String step = System.getenv().getOrDefault("show_answer", "tumbling");
    switch (step) {
      case "hopping":
        System.out.println("Running hopping window version");
        topology = com.example.hopping.MyTopology.build();
        break;

      case "session":
        System.out.println("Running session window version");
        topology = com.example.session.MyTopology.build();
        break;

      case "sliding":
        System.out.println("Running sliding window version");
        topology = com.example.sliding.MyTopology.build();
        break;

      default:
        System.out.println("Running tumbling window version");
        topology = com.example.tumbling.MyTopology.build();
        break;
    }
    return topology;
  }

  public static void main(String[] args) {

    // set the required properties for running Kafka Streams
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev-group");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

    // set some optional properties
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // build the topology and start streaming!
    KafkaStreams streams = new KafkaStreams(getTopology(), config);

    // close the Kafka Streams threads on shutdown
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    System.out.println("Starting Kafka Streams application");
    streams.start();
  }
}
