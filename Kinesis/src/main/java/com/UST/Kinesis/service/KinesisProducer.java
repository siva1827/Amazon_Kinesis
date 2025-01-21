package com.UST.Kinesis.service;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

public class KinesisProducer {
    public static void main(String[] args) {
        // Create the Kinesis client
        KinesisClient kinesisClient = KinesisClient.builder()
                .region(Region.US_EAST_1) // specify the region
                .credentialsProvider(DefaultCredentialsProvider.create()) // use default credentials provider
                .build();

        // Define the stream name
        String streamName = "Kinesis_Trial";

        // Create a PutRecord request
        PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey("partitionKey")  // use a unique partition key
                .data(SdkBytes.fromUtf8String("your-record-data"))  // data to send to the stream
                .build();

        // Send the record to Kinesis
        kinesisClient.putRecord(putRecordRequest);

        System.out.println("Record sent successfully.");
    }
}
