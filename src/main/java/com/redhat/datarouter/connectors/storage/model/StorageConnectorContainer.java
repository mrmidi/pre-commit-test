package com.redhat.datarouter.connectors.storage.model;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.Data;
import lombok.Generated;

import java.util.UUID;


@Data
@Generated
@JsonIgnoreProperties(ignoreUnknown = true)
@RegisterForReflection
public class StorageConnectorContainer {

    @SerializedName("request_id")
    @JsonProperty("request_id")
    public UUID requestID;

    //I need some kind of sample data for s3 storage connector
    @SerializedName("s3_source_url")
    @JsonProperty("s3_source_url")
    public String s3SourceUrl;

    @SerializedName("target_accesskey")
    @JsonProperty("target_accesskey")
    public String targetAccesskey;

    @SerializedName("target_secretkey")
    @JsonProperty("target_secretkey")
    public String targetSecretkey;

    @SerializedName("target_bucket")
    @JsonProperty("target_bucket")
    public String targetBucket;

    @SerializedName("target_region")
    @JsonProperty("target_region")
    public String targetRegion;

    @SerializedName("target_endpoint")
    @JsonProperty("target_endpoint")
    public String targetEndpoint;

}
