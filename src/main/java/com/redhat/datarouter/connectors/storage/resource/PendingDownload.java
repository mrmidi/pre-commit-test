package com.redhat.datarouter.connectors.storage.resource;

import com.google.common.base.Stopwatch;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.jboss.logging.Logger;

import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * The results of a pending download from the AWS TransferManager. Call waitForCompletion() to wait for the transfer to
 * complete and get the status of whether it was successful or not.
 */
class PendingDownload {

    private static final Logger log = Logger.getLogger(PendingDownload.class.getName());

    private final CompletableFuture<GetObjectResponse> future;
    private final File Filename;
    private final Stopwatch stopwatch;
    private final String s3key;

    private boolean alreadyWaited = false; // has waitForCompletion been called

    private boolean downloadSuccessful = false;

    PendingDownload(CompletableFuture<GetObjectResponse> future, File Filename,
                    String s3key) {
        this.future = future;
        stopwatch = Stopwatch.createStarted();
        this.Filename = Filename;
        this.s3key = s3key;
    }

    /**
     * @return true if the download was successful.
     */
    boolean waitForCompletion() throws InterruptedException {
        if (alreadyWaited) {
            return downloadSuccessful;
        }
        alreadyWaited = true;
        try {
            future.get();
            if (future.isDone()) {
                log.info("Waiting download to finish");
            }
            log.info(String.format("Finished downloading %s in %s", s3key, stopwatch));
            downloadSuccessful = true;
        } catch (InterruptedException e) {
            log.warn(String.format("Failed downloading %s after %s", s3key, stopwatch), e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException ex) {
            log.warn(String.format("Failed downloading %s after: %s", s3key, stopwatch), ex);
        } catch (Exception ex) {
            log.warn(String.format("Failed downloading %s after: %s", s3key, stopwatch), ex);
        }
        return downloadSuccessful;
    }
}