package com.UST.Kinesis.controller;

import com.UST.Kinesis.service.KinesisProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KinesisController {

    private final KinesisProducer kinesisProducer;

    @Autowired
    public KinesisController(KinesisProducer kinesisProducer) {
        this.kinesisProducer = kinesisProducer;
    }

    @GetMapping("/send-record")
    public String sendRecord() {
        String partitionKey = "samplePartitionKey";
        String data = "sampleData";
        kinesisProducer.sendRecord(partitionKey, data);
        return "Record sent to Kinesis stream!";
    }
}
