package com.emc.greenplum.service;

import com.yammer.dropwizard.config.Configuration;
import org.codehaus.jackson.annotate.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

public class HdfsServiceConfiguration extends Configuration {
    @NotEmpty
    @JsonProperty
    private String query_service_url;
}