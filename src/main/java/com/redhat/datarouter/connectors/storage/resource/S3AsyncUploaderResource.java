package com.redhat.datarouter.connectors.storage.resource;

import org.jboss.logging.Logger;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;


public class S3AsyncUploaderResource extends CommonResource {

    private static final Logger log = Logger.getLogger(S3AsyncUploaderResource.class.getName());

    private final String accessKey;
    private final String secretKey;
    private final String bucketName;
    private final String region;
    private final String endpoint;
    private final String destination;

    // TODO - metadata creation?
    // TODO - custom bucket creation?

    public S3AsyncUploaderResource(String accessKey, String secretKey, String region, String endpoint, String destination, String bucket) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.endpoint = endpoint;
        this.destination = destination;
        this.bucketName = bucket;
    }

    // TODO check file size. It seems that it's uploading empty files just fine. Also review for other possible errors.
    public boolean uploadFile() {
        boolean isSuccess = false;
        S3AsyncClient s3 = null;
        try {
            s3 = getS3Client(accessKey, secretKey, region, endpoint);
        } catch (Exception e) {
            log.error("Error creating custom s3 client", e);
        } finally {
            if (s3 != null) {
                //TODO filename generation?
                Path p = Paths.get(destination);
                String key = System.currentTimeMillis() + p.getFileName().toString();
                System.out.println("Destination is " + destination);
                //String key = System.currentTimeMillis() + destination;
                System.out.println("key is " + key);
                PutObjectRequest putRequest = buildPutRequest(bucketName, key);
                try {
                    s3.putObject(putRequest, AsyncRequestBody.fromFile(new File(destination)));
                } catch (Exception e) {
                    log.error("Error uploading file", e);
                } finally {
                    log.info("Uploaded file to s3");
                    isSuccess = true;
                }
            } else {
                log.error("Error creating custom s3 client (client is null)");
            }
        }
        return isSuccess;
    }
}
