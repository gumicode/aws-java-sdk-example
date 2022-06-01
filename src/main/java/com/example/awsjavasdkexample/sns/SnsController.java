package com.example.awsjavasdkexample.sns;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.message.SnsMessage;
import com.amazonaws.services.sns.message.SnsMessageManager;
import com.amazonaws.services.sns.message.SnsNotification;
import com.amazonaws.services.sns.message.SnsSubscriptionConfirmation;
import com.amazonaws.services.sns.model.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SnsController {

    private final AmazonSNS amazonSNS;
    private final SnsMessageManager snsMessageManager;
    private final String arn = "";

    @PostMapping("/sns/topic")
    public void createTopic(@RequestBody Map<String, String> body) {
        CreateTopicResult res = amazonSNS.createTopic(body.get("topic"));
        log.info("{}", res);
    }

    @PostMapping("/sns/subscribe")
    public void subScribe(@RequestBody Map<String, String> body) {

        SubscribeRequest subscribeRequest = new SubscribeRequest();
        subscribeRequest.setProtocol("https");
        subscribeRequest.setTopicArn(arn);
        subscribeRequest.setEndpoint(body.get("endpoint"));

        SubscribeResult subscribe = amazonSNS.subscribe(subscribeRequest);
        log.info("{}", subscribe);
    }

    @PostMapping("/sns/confirm-subscribe")
    public void confirmSubScribe(@RequestBody Map<String, String> body) {

        ConfirmSubscriptionResult token = amazonSNS.confirmSubscription(arn, body.get("token"));
        log.info("{}", token);
    }

    @PostMapping("/sns/publish")
    public void publish(@RequestBody Map<String, String> body) {
        PublishResult publishResult = amazonSNS.publish(arn, body.get("message"));
        log.info("{}", publishResult);
    }

    @RequestMapping(value = "/")
    public void home(HttpServletRequest request, HttpServletResponse response) throws IOException {

        ServletInputStream inputStream = request.getInputStream();
        SnsMessage snsMessage = snsMessageManager.parseMessage(inputStream);

        if(snsMessage instanceof SnsNotification) {
            SnsNotification notification = (SnsNotification)snsMessage;
            System.out.println(notification.getMessage());
        } else if(snsMessage instanceof SnsSubscriptionConfirmation) {
            SnsSubscriptionConfirmation snsMessage1 = (SnsSubscriptionConfirmation) snsMessage;
            snsMessage1.confirmSubscription();
        }
    }
}
