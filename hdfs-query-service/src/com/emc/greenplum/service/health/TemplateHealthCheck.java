package com.emc.greenplum.service.health;

import com.yammer.metrics.core.HealthCheck;

public class TemplateHealthCheck extends HealthCheck {

    public TemplateHealthCheck() {
        super("");
    }

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }
}