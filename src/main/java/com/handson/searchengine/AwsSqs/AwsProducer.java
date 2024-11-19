package com.handson.searchengine.AwsSqs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@Component
public class AwsProducer {

    @Autowired
    private ObjectMapper om;

    private final SqsClient sqsClient;
    private final String queueUrl;

    // Constructor to initialize SQS Client
    public AwsProducer(@Value("${amazon.aws.accesskey}") String accessKeyId,
                       @Value("${amazon.aws.secretkey}") String secretKey,
                       @Value("${aws.region}") String region,  // Load region dynamically
                       @Value("${aws.sqs.queueUrl}") String queueUrl) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKeyId, secretKey);
        this.sqsClient = SqsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
        this.queueUrl = queueUrl;
    }

    // Send message method similar to your Kafka producer
    public void send(Object message) throws JsonProcessingException {
        // Serialize the message to JSON
        String messageBody = om.writeValueAsString(message);
        // Send message to SQS
        SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();

        sqsClient.sendMessage(sendMsgRequest);
    }
}


