package com.example.helloworld;

import com.example.helloworld.resources.HdfsVersionResource;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Environment;
import com.example.helloworld.health.TemplateHealthCheck;

public class HelloWorldService extends Service<HelloWorldConfiguration> {
    public static void main(String[] args) throws Exception {

        new HelloWorldService().run(args);
    }

    private HelloWorldService() {
        super("hello-world");
    }

    @Override
    protected void initialize(HelloWorldConfiguration configuration,
                              Environment environment) {
        final String template = configuration.getTemplate();
        environment.addResource(new HdfsVersionResource());
        environment.addHealthCheck(new TemplateHealthCheck(template));
    }

}