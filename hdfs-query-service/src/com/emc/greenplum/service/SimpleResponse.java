package com.emc.greenplum.service;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/22/12
 * Time: 4:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class SimpleResponse {
    private String response;

    public SimpleResponse(String response) {
        this.response = response;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }
}
