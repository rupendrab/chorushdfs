package com.emc.greenplum.service;

import com.emc.greenplum.service.resources.HdfsContentResource;
import com.emc.greenplum.service.resources.HdfsEntitiesResource;
import com.emc.greenplum.service.resources.HdfsVersionResource;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Environment;
import com.emc.greenplum.service.health.TemplateHealthCheck;

public class HdfsService extends Service<HdfsServiceConfiguration> {
    public static void main(String[] args) throws Exception {

        new HdfsService().run(args);
    }

    private HdfsService() {
        super("hello-world");
    }

    @Override
    protected void initialize(HdfsServiceConfiguration configuration,
                              Environment environment) {
        final String template = configuration.getTemplate();
        environment.addResource(new HdfsVersionResource());
        environment.addResource(new HdfsEntitiesResource());
        environment.addResource(new HdfsContentResource());
        environment.addHealthCheck(new TemplateHealthCheck(template));
    }

}