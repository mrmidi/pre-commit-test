package com.redhat.datarouter.connectors.storage;

import com.google.gson.Gson;
import com.redhat.datarouter.connectors.storage.model.StorageConnectorContainer;
import com.redhat.datarouter.connectors.storage.model.StorageUploadDoneDto;
import com.redhat.datarouter.connectors.storage.resource.S3AsyncDownloaderResource;
import com.redhat.datarouter.connectors.storage.resource.S3AsyncUploaderResource;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.io.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

@ApplicationScoped
public class Import {

    private static final Logger log = Logger.getLogger(Import.class.getName());
    private StorageUploadDoneDto storageUploadDoneDto = new StorageUploadDoneDto();
    @Inject
    S3AsyncDownloaderResource s3client;

    @ConfigProperty(name = "quarkus.s3.aws.credentials.static-provider.access-key-id")
    String accessKeyId;

    @Channel("storage-upload-done")
    Emitter<StorageUploadDoneDto> storageUploadDoneDtoEmitter;

    //TODO kafka messaging sending of status
    //@Channel("storage-upload-done")

    @Incoming("storage-sink")
    @Transactional
    public void consume(StorageConnectorContainer s3ConnectorContainer) {

        System.out.printf("*********************************"+ accessKeyId + "*******************************");

        File vf;
        String objKey;
        String pattern = "^s3://([^/]+)/(.+)$";
        Pattern r = Pattern.compile(pattern);

        log.info("The message body received is: " + s3ConnectorContainer.toString());

        String s3SourceUrl = s3ConnectorContainer.s3SourceUrl;
        UUID requestID = s3ConnectorContainer.requestID;
        String targetAccesskey = s3ConnectorContainer.targetAccesskey;
        String targetSecretkey = s3ConnectorContainer.targetSecretkey;
        String targetBucket = s3ConnectorContainer.targetBucket;
        String targetRegion = s3ConnectorContainer.targetRegion;
        String targetEndpoint = s3ConnectorContainer.targetEndpoint;

        Matcher m = r.matcher(s3SourceUrl);
        if (m.find()) {
            System.out.println("Bucket: " + m.group(1) + " s3 object: " + m.group(2));
            objKey = m.group(2);
            try {
                vf = s3client.downloadFile(objKey);
                System.out.println("The vf is: " + vf);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("can't download file");
                return;
            }
        } else {
            System.out.println("NO MATCH");
            return;
        }

        //TODO check payload structure (upload is correct CCITDART-434)
        // Make sure the file could be decompressed
        File destination = new File(vf.getParent() + "/" + FilenameUtils.getBaseName(vf.toString()));
        Archiver archiver = ArchiverFactory.createArchiver(vf);
        try {
            log.info("Extract files and list file names");
            archiver.extract(vf, destination);
            Set<Path> files = listFiles(destination.toPath(), 10);
            for (Path file : files) {
                log.info(file.toString() + " (size in bytes: " + file.toFile().length() + ")");
            }
        } catch (IOException e) {
            log.error("Failed to extract " + vf.toPath(), e);
            sendMessage(requestID, "failed", e.getMessage());
        } finally {
            try {
                FileUtils.deleteDirectory(destination);
            } catch (IOException e) {
                log.error("Failed to delete file: " + destination.toPath(), e);
            }
        }

        //TODO create new archive of the required files CCITDART-435. Wich files is EXACTLY corresponding?

        // Upload to another S3
        //TODO Research: real world credentials
        S3AsyncUploaderResource uploader = new S3AsyncUploaderResource(targetAccesskey, targetSecretkey, targetRegion, targetEndpoint, vf.getPath(), targetBucket);
        if (uploader.uploadFile()) {
            sendMessage(requestID, "success", "File uploaded successfully");
        } else {
            sendMessage(requestID, "failed", "File upload failed");
        }
    }


    public Set<Path> listFiles(Path dir, int depth) throws IOException {
        try (Stream<Path> stream = Files.walk(dir, depth)) {
            return stream
                    .filter(file -> !Files.isDirectory(file))
                    .collect(Collectors.toSet());
        }
    }

    @Transactional
    public CompletionStage<Void> sendMessage(UUID requestID, String status, String message) {

        Gson gson = new Gson();

        storageUploadDoneDto.requestID = requestID;
        storageUploadDoneDto.componentID = "storage";
        storageUploadDoneDto.status = status;
        storageUploadDoneDto.message = message;
        storageUploadDoneDto.timestamp = Timestamp.from(Instant.now());

        String storageMessage = gson.toJson(storageUploadDoneDto);
        log.info("Ready to send to topic: storage-upload-done");
        log.info("The message body is: " + storageMessage);
        log.info("Message have been sent");
        return storageUploadDoneDtoEmitter.send(storageUploadDoneDto);
    }

}
