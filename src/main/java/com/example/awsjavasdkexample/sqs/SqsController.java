package com.example.awsjavasdkexample.sqs;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.*;
import io.awspring.cloud.messaging.core.QueueMessagingTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SqsController {

    private final AmazonSQSAsync amazonSQSAsync;

    @PostMapping("/sqs/queue")
    public void createQueue(@RequestBody Map<String, String> body) {
        CreateQueueResult result = amazonSQSAsync.createQueue(body.get("queue"));
        log.info("{}", result);

    }

    @DeleteMapping("/sqs/queue")
    public void deleteQueue(@RequestBody Map<String, String> body) {
        DeleteQueueResult result = amazonSQSAsync.deleteQueue(body.get("queue"));
        log.info("{}", result);
    }

    @GetMapping("/sqs/messages")
    public void getMessages(@RequestBody Map<String, String> body) {
        ReceiveMessageResult result = amazonSQSAsync.receiveMessage(body.get("queue"));
        List<Message> messages = result.getMessages();
        for (Message message : messages) {
            log.info(message.getBody());
            amazonSQSAsync.deleteMessage(body.get("queue"), message.getReceiptHandle());
        }
    }

    @PostMapping("/sqs/message")
    public void sendMessage(@RequestBody Map<String, String> body) {
        SendMessageResult result = amazonSQSAsync.sendMessage(body.get("queue"), body.get("message"));
        log.info("{}", result);
    }

    @PostMapping("/sqs/messages")
    public void sendMessages(@RequestBody Map<String, String> body) {
        int size = Integer.parseInt(body.get("size"));

        SendMessageBatchRequest request = new SendMessageBatchRequest();
        request.setQueueUrl(body.get("queue"));

        List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry();
            entry.setId(String.valueOf(i));
            entry.setMessageBody(body.get("queue") + i);
            entries.add(entry);
        }

        SendMessageBatchResult queue = amazonSQSAsync.sendMessageBatch(body.get("queue"), entries);
        log.info("{}", queue);
    }

    @GetMapping("/sqs/person")
    public void getPerson(@RequestBody Map<String, String> body) {
        QueueMessagingTemplate queueMessagingTemplate = new QueueMessagingTemplate(amazonSQSAsync);
        Person person = queueMessagingTemplate.receiveAndConvert(body.get("queue"), Person.class);
        log.info("{}", person);
    }

    @PostMapping("/sqs/person")
    public void postPerson(@RequestBody Map<String, String> body) {
        QueueMessagingTemplate queueMessagingTemplate = new QueueMessagingTemplate(amazonSQSAsync);
        queueMessagingTemplate.convertAndSend(body.get("queue"), new Person(body.get("name"), 10));
    }

//    @SqsListener(value = "test", deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
//    public void listenString(@Payload Object message, @Headers Map<String, String> headers) {
//        log.info("{}", message);
//        log.info("{}", headers);
//    }

//    @SqsListener(value = "person", deletionPolicy = SqsMessageDeletionPolicy.NEVER)
//    public void listenString(@Payload Person person, @Headers Map<String, String> headers, Acknowledgment ack) {
//        log.info("{}", person);
//        log.info("{}", headers);
//        log.info("{}", ack);
//        ack.acknowledge();
//    }
}
