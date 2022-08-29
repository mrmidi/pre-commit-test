package com.redhat.datarouter.connectors.storage;

import com.google.gson.Gson;
import com.redhat.datarouter.connectors.storage.model.StorageConnectorContainer;
import com.redhat.datarouter.connectors.storage.model.StorageUploadDoneDto;
import com.redhat.datarouter.connectors.storage.resource.S3AsyncDownloaderResource;
import com.redhat.datarouter.connectors.storage.resource.S3AsyncUploaderResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.reactive.messaging.providers.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.providers.connectors.InMemorySink;
import io.smallrye.reactive.messaging.providers.connectors.InMemorySource;


import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.enterprise.inject.Any;
import javax.inject.Inject;
import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.util.*;

import static org.awaitility.Awaitility.await;


@QuarkusTest
@TestProfile(MockTestProfile.class)
@QuarkusTestResource(KafkaTestResourceLifecycleManager.class)
@QuarkusTestResource(TestcontainerLocalstackResourceLifecycleManager.class)
public class ImportTest {
    private static final Logger log = Logger.getLogger(Import.class.getName());
    private static final String S3_URL = "s3://s3-connector-download-test/droute_payload.tar.gz";
    private static final String S3_SOURCE_KEY = "droute_payload.tar.gz";
    private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");
    private static final String S3_BUCKET = "s3-connector-upload-test";

    @ConfigProperty(name = "quarkus.s3.aws.credentials.static-provider.access-key-id")
    String s3AccessKey;
    @ConfigProperty(name = "quarkus.s3.aws.credentials.static-provider.secret-access-key")
    String s3SecretKey;
    @ConfigProperty(name = "quarkus.s3.endpoint-override")
    String s3Endpoint;
    @ConfigProperty(name = "quarkus.s3.aws.region")
    String s3Region;

    //@Inject @Any
    @Inject @Any
    InMemoryConnector connector;


    @Inject
    S3AsyncDownloaderResource s3;


    @BeforeEach
    public void setup() throws IOException, InterruptedException {

    }


    @Test
    @Order(1)
    void testConsume() {
        System.out.println("Here will be the test");


        InMemorySource<StorageConnectorContainer> storageIn = connector.source("storage-sink");
        InMemorySink<StorageUploadDoneDto> storageOut = connector.sink("storage-upload-done");

        StorageConnectorContainer storageConnectorContainer = new StorageConnectorContainer();
        storageConnectorContainer.requestID = UUID.fromString("aba86aee-6e87-45b3-a15e-7183a7f222d9");
        storageConnectorContainer.s3SourceUrl = S3_URL;
        storageConnectorContainer.targetAccesskey = s3AccessKey;
        storageConnectorContainer.targetSecretkey = s3SecretKey;
        storageConnectorContainer.targetEndpoint = s3Endpoint;
        storageConnectorContainer.targetRegion = s3Region;
        storageConnectorContainer.targetBucket = S3_BUCKET;

        Gson gson = new Gson();
        String storageContainerJson = gson.toJson(storageConnectorContainer);
        System.out.println("storageContainerJson: " + storageContainerJson);
        storageIn.send(storageConnectorContainer);


        // Wait for one message
        await().<List<? extends Message<StorageUploadDoneDto>>>until(storageOut::received, t -> t.size() == 1);

        StorageUploadDoneDto storageUploadDoneDto = storageOut.received().get(0).getPayload();

        Assertions.assertEquals("aba86aee-6e87-45b3-a15e-7183a7f222d9", storageUploadDoneDto.requestID.toString());
        Assertions.assertEquals("success", storageUploadDoneDto.status);
    }

    @Test
    @Order(2)
    void testDownload() {
        File tempFile = new File(TEMP_DIR + "/" + System.currentTimeMillis() + S3_SOURCE_KEY);
        try {
            tempFile = s3.downloadFile(S3_SOURCE_KEY);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("tempFile: " + tempFile.getAbsolutePath());
        try {
            // Payload file from resources is 2118 bytes in size
            Assertions.assertTrue(Files.size(tempFile.toPath()) == 2118);
        } catch (Exception e) {
            log.error("Downloaded temp file not found", e);
            e.printStackTrace();
        }
    }

    @Test
    @Order(3)
    void testUpload() {
        log.info("Upload test");
        ListObjectsResponse res = null;
        S3Object s3Object = null;
        ClassLoader classLoader = getClass().getClassLoader();
        File samplePayload = new File(classLoader.getResource(S3_SOURCE_KEY).getFile());
        System.out.println("Uploading file " + samplePayload.getAbsolutePath() + " to " + S3_BUCKET);
        S3AsyncUploaderResource s3Uploader = new S3AsyncUploaderResource(s3AccessKey, s3SecretKey, s3Region, s3Endpoint, samplePayload.getAbsolutePath(), S3_BUCKET);
        try {
            s3Uploader.uploadFile();
        } catch (Exception e) {
            log.error("Upload failed", e);
        }
        try {
            ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder().bucket(S3_BUCKET).build();

            S3Client s3Client = getS3Client();
            if (s3Client != null) {
                log.info("Getting objects from bucket " + S3_BUCKET);
                res = s3Client.listObjects(listObjectsRequest);
                List<S3Object> objects = res.contents();
                // Should be only one object in the bucket
                s3Object = objects.get(0);
            }
        } catch (S3Exception e) {
            log.error("Could not list bucket objects", e);
        }
        Assertions.assertNotNull(s3Object);
        Assertions.assertEquals(2118, s3Object.size());
    }

    private S3Client getS3Client() {
        S3Client s3Client = null;
        try {
            s3Client = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(s3AccessKey, s3SecretKey)))
                    .endpointOverride(URI.create(s3Endpoint))
                    .region(Region.of(s3Region))
                    .build();
        } catch (Exception e) {
            log.error("Could not create S3 client", e);
        }
        return s3Client;
    }
}
