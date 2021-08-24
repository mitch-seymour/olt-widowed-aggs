package com.example;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TopologyTest {

  @BeforeEach
  void setup() {
    TestHelper.setup();
  }

  @AfterEach
  void teardown() {
    TestHelper.teardown();
  }

  @Test
  void testWindowedAgg() {
    Instant time = Instant.EPOCH;
    Instant time2 = time.plus(2, ChronoUnit.MINUTES);
    Instant time3 = time.plus(3, ChronoUnit.MINUTES);
    TestHelper.inputTopic().pipeInput("user123", "Hello there (post 1)", time);
    TestHelper.inputTopic().pipeInput("user123", "Hello there (post 2)", time2);
    TestHelper.inputTopic().pipeInput("user123", "Hello there (post 3)", time3);

    List<TestRecord<String, String>> outRecords = TestHelper.outputTopic().readRecordsToList();
    assertThat(outRecords).hasSize(1);
    assertThat(outRecords.get(0).value()).isEqualTo("user123 has exceeded the 5 minute post limit");
    TestHelper.printHelpfulMessages(outRecords);
  }
}
