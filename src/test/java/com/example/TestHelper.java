package com.example;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;

class TestHelper {

  private static TopologyTestDriver testDriver;
  private static TestInputTopic<String, String> inputTopic;
  private static TestOutputTopic<String, String> outputTopic;

  static void setup() {
    // build the topology with a dummy client
    Topology topology = App.getTopology();

    // create a test driver. we will use this to pipe data to our topology
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    testDriver = new TopologyTestDriver(topology, props);

    // create the test input topics
    inputTopic =
        testDriver.createInputTopic(
            "reddit-posts", Serdes.String().serializer(), Serdes.String().serializer());

    // create the test output topic
    outputTopic =
        testDriver.createOutputTopic(
            "alerts", Serdes.String().deserializer(), Serdes.String().deserializer());
  }

  static void teardown() {
    // make sure resources are cleaned up properly
    testDriver.close();
  }

  static TestInputTopic<String, String> inputTopic() {
    return inputTopic;
  }

  static TestOutputTopic<String, String> outputTopic() {
    return outputTopic;
  }

  static void printHelpfulMessages(List<TestRecord<String, String>> outRecords) {
    String msg = String.format("%n%n%d record(s) in output topic%n%n", outRecords.size());
    for (var record : outRecords) {
      msg += String.format("Record: %s%n", record.value());
    }
    System.out.println(msg);
  }
}
