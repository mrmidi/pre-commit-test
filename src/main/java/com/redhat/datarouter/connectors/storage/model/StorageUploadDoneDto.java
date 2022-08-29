package com.redhat.datarouter.connectors.storage.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.Data;
import lombok.Generated;

import java.sql.Timestamp;
import java.util.UUID;


@Data
@Generated
@JsonIgnoreProperties(ignoreUnknown = true)
@RegisterForReflection
public class StorageUploadDoneDto {

    @SerializedName("request_id")
    @JsonProperty("request_id")
    public UUID requestID;

    @SerializedName("component_id")
    @JsonProperty("component_id")
    public String componentID;

    public Timestamp timestamp;

    public String status;

    public String message;

}
