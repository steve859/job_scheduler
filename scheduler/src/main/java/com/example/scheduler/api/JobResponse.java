package com.example.scheduler.api;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobResponse {
    private String jobId;
    private String schedule;
    private Object payload;
    private Integer maxDurationSeconds;
    private Instant createdAt;

    private String status;
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

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public Integer getMaxDurationSeconds() {
        return maxDurationSeconds;
    }

    public void setMaxDurationSeconds(Integer maxDurationSeconds) {
        this.maxDurationSeconds = maxDurationSeconds;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Instant getLastRunStartAt() {
        return lastRunStartAt;
    }

    public void setLastRunStartAt(Instant lastRunStartAt) {
        this.lastRunStartAt = lastRunStartAt;
    }

    public Instant getLastRunEndAt() {
        return lastRunEndAt;
    }

    public void setLastRunEndAt(Instant lastRunEndAt) {
        this.lastRunEndAt = lastRunEndAt;
    }

    public String getLastRunStatus() {
        return lastRunStatus;
    }

    public void setLastRunStatus(String lastRunStatus) {
        this.lastRunStatus = lastRunStatus;
    }
}
