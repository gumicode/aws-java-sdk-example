package com.example.awsjavasdkexample.sqs;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageResult;
import io.awspring.cloud.messaging.core.QueueMessagingTemplate;
import io.awspring.cloud.messaging.listener.SqsMessageDeletionPolicy;
import io.awspring.cloud.messaging.listener.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

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

    @PostMapping("/sqs/message")
    public void sendMessage(@RequestBody Map<String, String> body) {
        SendMessageResult result = amazonSQSAsync.sendMessage(body.get("queue"), body.get("message"));
        log.info("{}", result);
    }

    @GetMapping("/sqs/message")
    public void getMessage(@RequestBody Map<String, String> body) {
        ReceiveMessageResult queue = amazonSQSAsync.receiveMessage(body.get("queue"));
        List<Message> messages = queue.getMessages();
        for (Message message : messages) {
            log.info(message.toString());
        }
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

    @PostMapping("/sqs/person")
    public void postPerson(@RequestBody Map<String, String> body) {
        QueueMessagingTemplate queueMessagingTemplate = new QueueMessagingTemplate(amazonSQSAsync);
        queueMessagingTemplate.convertAndSend(body.get("queue"), new Person(body.get("name"), 10));
    }

    @GetMapping("/sqs/person")
    public void getPerson(@RequestBody Map<String, String> body) {
        QueueMessagingTemplate queueMessagingTemplate = new QueueMessagingTemplate(amazonSQSAsync);
        Person person = queueMessagingTemplate.receiveAndConvert(body.get("queue"), Person.class);
        log.info("{}", person);
    }
}
