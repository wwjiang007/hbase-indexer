package com.ngdata.sep.demo;

public class MyPayload {
    boolean partialUpdate = false;

    public boolean isPartialUpdate() {
        return partialUpdate;
    }

    public void setPartialUpdate(boolean partialUpdate) {
        this.partialUpdate = partialUpdate;
    }
}
