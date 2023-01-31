package com.example.awsjavasdkexample.sqs.fifo;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SqsFifoController {

    private final AmazonSQS amazonSQS;

    @PostMapping("/sqs/fifo/queue")
    public void createQueue(@RequestBody Map<String, String> body) {

        CreateQueueRequest request = new CreateQueueRequest();
        request.setQueueName(body.get("queue"));

        Map<String,String> maps = new HashMap<>();
        maps.put("FifoQueue", "true");
        maps.put("ContentBasedDeduplication", "true");
        maps.put("ReceiveMessageWaitTimeSeconds", "20"); // long 롤링

        request.setAttributes(maps);
        CreateQueueResult result = amazonSQS.createQueue(request);
        log.info("{}", result);
    }

    @DeleteMapping("/sqs/fifo/queue")
    public void deleteQueue(@RequestBody Map<String, String> body) {
        DeleteQueueResult result = amazonSQS.deleteQueue(body.get("queue"));
        log.info("{}", result);
    }

    @GetMapping("/sqs/fifo/messages")
    public void getMessages(@RequestBody Map<String, String> body) {
        consume(body.get("queue"));
    }

    private void consume(final String queueName){
        ReceiveMessageResult result = amazonSQS.receiveMessage(queueName);
        List<Message> messages = result.getMessages();

        if(messages.size() == 0) {
            return;
        }

        Collections.reverse(messages);
        for (Message message : messages) {
            log.info(message.getBody());
            amazonSQS.deleteMessage(queueName, message.getReceiptHandle());
        }
        consume(queueName);
    }

    @PostMapping("/sqs/fifo/message")
    public void sendMessage(@RequestBody Map<String, String> body) {

        SendMessageRequest sendMessageRequest = new SendMessageRequest();
        sendMessageRequest.setQueueUrl(body.get("queue"));
        sendMessageRequest.setMessageBody(body.get("message"));
        sendMessageRequest.setMessageGroupId("mgid");
        SendMessageResult result = amazonSQS.sendMessage(sendMessageRequest);
        log.info("{}", result);
    }

    @PostMapping("/sqs/fifo/messages")
    public void sendMessages(@RequestBody Map<String, String> body) {
        int size = Integer.parseInt(body.get("size"));

        SendMessageBatchRequest request = new SendMessageBatchRequest();
        request.setQueueUrl(body.get("queue"));

        List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry();
            entry.setId(String.valueOf(i));
            entry.setMessageBody(body.get("message") + i + " " + UUID.randomUUID());
            entry.setMessageGroupId("mgid");
            entries.add(entry);
        }

        SendMessageBatchResult queue = amazonSQS.sendMessageBatch(body.get("queue"), entries);
        log.info("{}", queue);
    }
}
