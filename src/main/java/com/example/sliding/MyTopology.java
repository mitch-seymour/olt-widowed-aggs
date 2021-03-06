package com.example.sliding;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Windowed;

public class MyTopology {

  public static Topology build() {
    StreamsBuilder builder = new StreamsBuilder();

    // answer
    SlidingWindows window =
        SlidingWindows.withTimeDifferenceAndGrace(
            Duration.ofMinutes(5), // max time diff between 2 records
            Duration.ofMinutes(5)); // grace period

    Consumed<String, String> consumerParams = Consumed.with(Serdes.String(), Serdes.String());
    KStream<String, String> redditPosts = builder.stream("reddit-posts", consumerParams);

    KTable<Windowed<String>, Long> counts = redditPosts.groupByKey().windowedBy(window).count();

    // write the alerts to a topic
    counts
        .filter((key, value) -> value == 3)
        .toStream()
        .map((windowKey, value) -> createAlert(windowKey, value))
        .to("alerts", Produced.with(Serdes.String(), Serdes.String()));
    ;

    return builder.build();
  }

  private static KeyValue<String, String> createAlert(Windowed<String> windowKey, Long value) {
    String userId = windowKey.key();
    String alert = String.format("%s has exceeded the 5 minute post limit", userId, value);
    return KeyValue.pair(userId, alert);
  }
}
