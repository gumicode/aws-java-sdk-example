package com.example.awsjavasdkexample;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.junit.jupiter.api.Test;

import javax.swing.plaf.synth.Region;

class AwsJavaSdkExampleApplicationTests {

    @Test
    void createTopic() {

        final String usage = "\n" +
                "Usage: " +
                "   <topicName>\n\n" +
                "Where:\n" +
                "   topicName - The name of the topic to create (for example, mytopic).\n\n";

        if (args.length != 1) {
            System.out.println(usage);
            System.exit(1);
        }

        String topicName = args[0];
        System.out.println("Creating a topic with name: " + topicName);

        SnsCli snsClient = SnsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();

        String arnVal = createSNSTopic(snsClient, topicName) ;
        System.out.println("The topic ARN is" +arnVal);
        snsClient.close();


    }

}
