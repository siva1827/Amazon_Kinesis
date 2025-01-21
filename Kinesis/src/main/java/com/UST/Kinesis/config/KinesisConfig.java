package com.UST.Kinesis.config;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;

@Configuration
public class KinesisConfig {

    @Value("${aws.region}")
    private String region;

    @Value("${aws.credentials.accessKey}")
    private String accessKey;

    @Value("${aws.credentials.secretKey}")
    private String secretKey;

    @Value("${aws.kinesis.stream.name}")
    private String streamName;

    @Value("${aws.kinesis.shard.id}")
    private String shardId;

    @Value("${aws.kinesis.polling.interval-ms}")
    private long pollingIntervalMs;

    @Bean
    public KinesisClient kinesisClient() {
        AwsCredentials credentials = new AwsCredentials() {
            @Override
            public String accessKeyId() {
                return accessKey;
            }

            @Override
            public String secretAccessKey() {
                return secretKey;
            }
        };

        AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);

        return KinesisClient.builder()
                .region(Region.of(region))
                .credentialsProvider(credentialsProvider)
                .build();
    }

    @Bean
    public AmazonKinesis amazonKinesisClient() {
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        return AmazonKinesisClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();
    }

    @Bean
    public String streamName() {
        return streamName;
    }

    @Bean
    public String shardId() {
        return shardId;
    }

    @Bean
    public long pollingIntervalMs() {
        return pollingIntervalMs;
    }

    @Bean
    public void createKinesisStream(AmazonKinesis amazonKinesis) {
        try {
            // Create Kinesis Stream if it does not exist
            amazonKinesis.createStream(new CreateStreamRequest()
                    .withStreamName(streamName)
                    .withShardCount(1)); // Set the number of shards
        } catch (Exception e) {
            // Stream might already exist
            System.out.println("Stream already exists or unable to create: " + e.getMessage());
        }
    }
}
