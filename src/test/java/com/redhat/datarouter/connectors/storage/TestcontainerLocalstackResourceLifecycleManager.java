package com.redhat.datarouter.connectors.storage;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.jboss.logging.Logger;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Testcontainers
public class TestcontainerLocalstackResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {
    private static final Logger log = Logger.getLogger(Import.class.getName());


    private static final DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:latest");
    private static final String SOURCE_BUCKET_NAME = "s3-connector-download-test";
    private static final String DEST_BUCKET_NAME = "s3-connector-upload-test";
    private static final String OBJECT_NAME = "droute_payload.tar.gz";
    //private static final Bucket bucket = new Bucket(BUCKET_NAME);
    private LocalStackContainer s3container;
    /*
        //todo set fixed version of localstack container
    DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:latest");
    LocalStackContainer s3container = new LocalStackContainer(localstackImage).withServices(Service.S3);

     */

    private S3Client s3Client;

    @Override
    public Map<String, String> start() {
        System.out.printf("Starting localstack container");
        log.info("Starting localstack S3 testcontainer");
        Map<String, String> s3TestcontainerConfig = new HashMap<>();
        s3container = new LocalStackContainer(localstackImage).withServices(Service.S3);
        s3container.start();

        s3TestcontainerConfig = getConfig();
        s3TestcontainerConfig.forEach((k, v) -> System.out.printf("%s: %s\n", k, v));

        // Put files to S3 testcontainer
        log.info("Putting files to S3 testcontainer");
        StaticCredentialsProvider credentials =
                StaticCredentialsProvider.create(AwsBasicCredentials.create(s3container.getAccessKey(),
                        s3container.getSecretKey()));
        s3Client = S3Client.builder()
                .credentialsProvider(credentials)
                .endpointOverride(URI.create("http://" + s3container.getHost() + ":" + s3container.getMappedPort(4566)))
                .region(Region.of(s3container.getRegion()))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();

        //Create source bucket
        log.info("Creating source and destination buckets " + SOURCE_BUCKET_NAME + " and " + DEST_BUCKET_NAME);
        createBucket(SOURCE_BUCKET_NAME);
        createBucket(DEST_BUCKET_NAME);

        // Check if source bucket exists
        listBuckets();

        //Upload file to source bucket
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(SOURCE_BUCKET_NAME)
                .key(OBJECT_NAME)
                .build();

        ClassLoader classLoader = getClass().getClassLoader();
        File samplePayload = new File(classLoader.getResource(OBJECT_NAME).getFile());
        System.out.println("Uploading file " + samplePayload.getAbsolutePath() + " to " + SOURCE_BUCKET_NAME);
        s3Client.putObject(putObjectRequest, RequestBody.fromFile(samplePayload));

        // Check files is uploaded to source bucket
        System.out.printf("Checking file %s is uploaded to source bucket %s\n", OBJECT_NAME, SOURCE_BUCKET_NAME);
        listObjects(SOURCE_BUCKET_NAME);

        // Test download from source bucket
        System.out.printf("Downloading file %s from source bucket %s\n", OBJECT_NAME, SOURCE_BUCKET_NAME);
        ResponseInputStream<GetObjectResponse> s3object = null;
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(SOURCE_BUCKET_NAME)
                .key(OBJECT_NAME)
                .build();
        s3object = s3Client.getObject(getObjectRequest);
        GetObjectResponse objectResponse = s3object.response();
        System.out.printf(objectResponse.toString());

        log.info("Localstack S3 testcontainer started");
        return s3TestcontainerConfig;
    }

    private void listObjects(String bucketName) {
        ListObjectsRequest listObjects = ListObjectsRequest
                .builder()
                .bucket(bucketName)
                .build();

        ListObjectsResponse res = s3Client.listObjects(listObjects);
        List<S3Object> objects = res.contents();
        for (S3Object myValue : objects) {
            System.out.println("The name of the key is " + myValue.key());
            System.out.println("The object is " + (myValue.size()) + " Bytes");
            System.out.println("The owner is " + myValue.owner());
        }
    }

    private void listBuckets() {
        ListBucketsRequest listBuckets = ListBucketsRequest.builder().build();
        ListBucketsResponse res = s3Client.listBuckets(listBuckets);
        List<Bucket> buckets = res.buckets();
        for (Bucket myValue : buckets) {
            System.out.println("he name of the bucket is " + myValue.name());
            System.out.println("The creation date of the bucket is " + myValue.creationDate());
        }
    }



    private void createBucket(String bucketName) {
        if (s3Client != null) {
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            s3Client.createBucket(bucketRequest);
        }
    }

    /*
    quarkus.s3.endpoint-override=http://localhost:8008

quarkus.s3.aws.region=us-east-1
quarkus.s3.aws.credentials.type=static
quarkus.s3.aws.credentials.static-provider.access-key-id=test-key
quarkus.s3.aws.credentials.static-provider.secret-access-key=test-secret
     */


    private Map<String, String> getConfig() {
        final Map<String, String> s3TestcontainerConfig = new HashMap<>();
        s3TestcontainerConfig.put("quarkus.s3.aws.credentials.static-provider.secret-access-key", s3container.getSecretKey());
        s3TestcontainerConfig.put("quarkus.s3.endpoint-override", "http://" + s3container.getHost() + ":" + s3container.getMappedPort(4566));
        s3TestcontainerConfig.put("quarkus.s3.aws.region", s3container.getRegion());
        s3TestcontainerConfig.put("quarkus.s3.aws.credentials.type", "static");
        s3TestcontainerConfig.put("quarkus.s3.aws.credentials.static-provider.access-key-id", s3container.getAccessKey());
        s3TestcontainerConfig.put("bucket.name", SOURCE_BUCKET_NAME);
        return s3TestcontainerConfig;
    }


    @Override
    public void stop() {
        if (s3container != null) {
            s3container.stop();
            log.info("Localstack S3 testcontainer stopped");
        }
    }
}
