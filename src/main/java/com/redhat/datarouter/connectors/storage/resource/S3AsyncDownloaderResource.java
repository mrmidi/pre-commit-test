package com.redhat.datarouter.connectors.storage.resource;

import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;





//@Path("/async-s3")
@ApplicationScoped
public class S3AsyncDownloaderResource extends CommonResource {

    private static final Logger log = Logger.getLogger(S3AsyncDownloaderResource.class.getName());

    @Inject
    S3AsyncClient s3;

    public File downloadFile(String objectKey) throws InterruptedException {
        File tempFile = tempFilePath(objectKey);
        var br = buildGetRequest(objectKey);
        log.debug("The get object request is " + br);
        var future = s3.getObject(br, AsyncResponseTransformer.toFile(tempFile));

        PendingDownload pd = new PendingDownload(future, tempFile, objectKey);
        pd.waitForCompletion();

        return tempFile;
    }


    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<List<FileObject>> listFiles() {
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
                .bucket(bucketName)
                .build();

        return Uni.createFrom().completionStage(() -> s3.listObjects(listRequest))
                .onItem().transform(result -> toFileItems(result));
    }

    private List<FileObject> toFileItems(ListObjectsResponse objects) {
        return objects.contents().stream()
                .map(FileObject::from)
                .sorted(Comparator.comparing(FileObject::getObjectKey))
                .collect(Collectors.toList());
    }


}