package com.example.awsjavasdkexample.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@Slf4j
@RestController
@RequiredArgsConstructor
public class S3Controller {

    @PostMapping("/s3/object")
    public void putObjectByKey() throws IOException {
        String accessKey = "";
        String accessSecret = "";
        String bucket = "";
        String key = "";
        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, accessSecret);
        AmazonS3 s3Client = AmazonS3Client.builder().withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).withRegion(Regions.AP_NORTHEAST_2).build();

        ClassPathResource classPathResource = new ClassPathResource("sample/promotion.csv");
        s3Client.putObject(bucket, key, classPathResource.getFile());
    }
}
