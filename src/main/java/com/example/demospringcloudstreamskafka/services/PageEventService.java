package com.example.demospringcloudstreamskafka.services;

import com.example.demospringcloudstreamskafka.entities.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static javax.swing.text.html.HTML.Tag.OL;

@Service
public class PageEventService {
    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return (input) -> {
            System.out.println("********");
            System.out.println(input.toString());
            System.out.println("********");
        };

    }

    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> new PageEvent(
                Math.random() > 0.5 ? "P1" : "P2",
                Math.random() > 0.5 ? "U1" : "U2",
                new Date(), new Random().nextInt(9000));
    }

    @Bean
    public Function<PageEvent, PageEvent> pageEventFunction() {
        return (input) -> {
            input.setName("Page Event");
            input.setUser("UUUUUU");
            return input;
        };
    }

    @Bean
    public Function<KStream<String,PageEvent>,KStream<String,Long>> kStreamFuntion(){
        return (input) -> {
            return input
                    .filter((K,v)->v.getDuration() > 100)
            .map((K,v)->new KeyValue<>(v.getName(), 0L))
            .groupBy((K,v)->K,Grouped.with(Serdes.String(), Serdes.Long()))
                    .windowedBy(TimeWindows.of(Duration.ofDays(5000)))
            .count(Materialized.as("page-count"))
                    .toStream()
                    .map((K,v)->new KeyValue<>("=>"+K.window().startTime()+K.window().endTime()+K.key(),v));
        };
    }
}
