package com.UST.Kinesis.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

@Service
public class KinesisConsumer {

    private final KinesisClient kinesisClient;
    private final String streamName;
    private final String shardId;

    public KinesisConsumer(
            KinesisClient kinesisClient,
            @Value("${kinesis.stream.name}") String streamName,
            @Value("${kinesis.shard.id}") String shardId
    ) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        this.shardId = shardId;
    }

    @Scheduled(fixedRateString = "${kinesis.polling.interval-ms}")
    public void readData() {
        String shardIterator = getShardIterator();
        if (shardIterator == null) return;

        GetRecordsRequest request = GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .limit(10)
                .build();

        GetRecordsResponse response = kinesisClient.getRecords(request);
        response.records().forEach(record -> {
            String data = new String(record.data().asByteArray());
            System.out.println("Received data: " + data);
        });
    }

    private String getShardIterator() {
        GetShardIteratorRequest request = GetShardIteratorRequest.builder()
                .streamName(streamName)
                .shardId(shardId)
                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .build();

        GetShardIteratorResponse response = kinesisClient.getShardIterator(request);
        return response.shardIterator();
    }
}
