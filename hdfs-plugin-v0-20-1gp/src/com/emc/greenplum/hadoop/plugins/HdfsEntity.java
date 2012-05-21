package com.emc.greenplum.hadoop.plugins;


import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: pivotal
 * Date: 5/18/12
 * Time: 11:53 AM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsEntity {
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Date getModifiedAt() {
        return modifiedAt;
    }

    public void setModifiedAt(Date modifiedAt) {
        this.modifiedAt = modifiedAt;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public void setDirectory(boolean directory) {
        isDirectory = directory;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    private String path;
    private Date modifiedAt;
    private boolean isDirectory;
    private long size;
}
