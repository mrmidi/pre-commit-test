package com.redhat.datarouter.connectors.storage.resource;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.net.URI;
import java.util.Date;
import java.util.UUID;

abstract public class CommonResource {


    private final static String TEMP_DIR = System.getProperty("java.io.tmpdir");
    @ConfigProperty(name = "bucket.name")
    String bucketName;

//    protected PutObjectRequest buildPutRequest(FormData formData) {
//        return PutObjectRequest.builder()
//                .bucket(bucketName)
//                .key(formData.filename)
//                .contentType(formData.mimetype)
//                .build();
//    }

    protected PutObjectRequest buildPutRequest(String bucket, String key) {
        return PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
    }

    protected S3AsyncClient getS3Client(String accessKey, String secretKey, String region, String endpoint) {
//        System.out.println("The accessKey is: " + accessKey);
//        System.out.println("The secretKey is: " + secretKey);
//        System.out.println("The region is: " + region);
//        System.out.println("The endpoint is: " + endpoint);

        StaticCredentialsProvider credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        S3AsyncClient s3 = S3AsyncClient.builder()
                .credentialsProvider(credentials)
                .region(Region.of(region))
                .endpointOverride(URI.create(endpoint))
                .build();
        return s3;
    }

    protected GetObjectRequest buildGetRequest(String objectKey) {
        return GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();
    }

    protected File tempFilePath(String base) {
        if (base.equals("")) {
            base = "s3AsyncDownloadedTemp";
        }
        return new File(TEMP_DIR, new StringBuilder()
                .append((new Date()).getTime())
                .append("-" + UUID.randomUUID().toString().split("-", 2)[0] + "-")
                .append(base).toString());
    }
}
