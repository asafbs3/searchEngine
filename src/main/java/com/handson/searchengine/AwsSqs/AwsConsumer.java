package com.handson.searchengine.AwsSqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.handson.searchengine.crawler.Crawler;
import com.handson.searchengine.model.CrawlerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

import java.io.IOException;
import java.util.List;

@Component
public class AwsConsumer {

    @Autowired
    private ObjectMapper om;

    @Autowired
    private Crawler crawler;

    private final SqsClient sqsClient;
    private final String queueUrl;

    // Constructor to load values from application.properties
    @Autowired
    public AwsConsumer(
            @Value("${amazon.aws.accesskey}") String accessKeyId,
            @Value("${amazon.aws.secretkey}") String secretKey,
            @Value("${aws.region}") String region,
            @Value("${aws.sqs.queueUrl}") String queueUrl) {

        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKeyId, secretKey);
        this.sqsClient = SqsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();

        this.queueUrl = queueUrl;
    }

    // Polls SQS for messages and processes them
    @Scheduled(fixedDelay = 100) // Poll every 1 seconds
    public void listen() throws IOException, InterruptedException {
        // Receive messages from SQS (up to 1 messages at a time)
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)  // You can adjust the number of messages to fetch
                .build();

        List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

        for (Message message : messages) {
            // Process each message
            CrawlerRecord rec = om.readValue(message.body().toString(), CrawlerRecord.class);
            crawler.crawlOneRecord(rec.getCrawlId(), rec);

            // After processing, delete the message from the queue
            deleteMessage(message);
        }
    }

    // Deletes a processed message from the SQS queue
    private void deleteMessage(Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();

        sqsClient.deleteMessage(deleteRequest);
    }
}


