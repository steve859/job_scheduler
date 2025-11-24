package com.example.worker.api;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobResponse {
    private String jobId;
    private String schedule;
    private Object payload;
    private Integer maxDurationSeconds;
    private Instant createdAt;
    private String status; // aggregated latest status
    private Instant lastRunStartAt;
    private Instant lastRunEndAt;
    private String lastRunStatus;

    public JobResponse() {
    }

    public JobResponse(String jobId, String schedule, Object payload, Integer maxDurationSeconds, Instant createdAt,
            String status, Instant lastRunStartAt, Instant lastRunEndAt, String lastRunStatus) {
        this.jobId = jobId;
        this.schedule = schedule;
        this.payload = payload;
        this.maxDurationSeconds = maxDurationSeconds;
        this.createdAt = createdAt;
        this.status = status;
        this.lastRunStartAt = lastRunStartAt;
        this.lastRunEndAt = lastRunEndAt;
        this.lastRunStatus = lastRunStatus;
    }

    public String getJobId() {
        return jobId;
    }

    public String getSchedule() {
        return schedule;
    }

    public Object getPayload() {
        return payload;
    }

    public Integer getMaxDurationSeconds() {
        return maxDurationSeconds;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public String getStatus() {
        return status;
    }

    public Instant getLastRunStartAt() {
        return lastRunStartAt;
    }

    public Instant getLastRunEndAt() {
        return lastRunEndAt;
    }

    public String getLastRunStatus() {
        return lastRunStatus;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public void setMaxDurationSeconds(Integer maxDurationSeconds) {
        this.maxDurationSeconds = maxDurationSeconds;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setLastRunStartAt(Instant lastRunStartAt) {
        this.lastRunStartAt = lastRunStartAt;
    }

    public void setLastRunEndAt(Instant lastRunEndAt) {
        this.lastRunEndAt = lastRunEndAt;
    }

    public void setLastRunStatus(String lastRunStatus) {
        this.lastRunStatus = lastRunStatus;
    }
}
