package com.example.worker.api;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class EnqueueResponse {
    private String jobId;
    private String status;

    public EnqueueResponse() {
    }

    public EnqueueResponse(String jobId, String status) {
        this.jobId = jobId;
        this.status = status;
    }

    public String getJobId() {
        return jobId;
    }

    public String getStatus() {
        return status;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
