package com.example.scheduler.api.cron;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CronJobResponse {
    private String cronId;
    private String cron;
    private String timezone;
    private Object payload;
    private Integer maxDurationSeconds;
    private Boolean enabled;
    private Instant nextRunAt;
    private Instant createdAt;

    public CronJobResponse() {
    }

    public CronJobResponse(String cronId, String cron, String timezone, Object payload, Integer maxDurationSeconds,
                           Boolean enabled, Instant nextRunAt, Instant createdAt) {
        this.cronId = cronId;
        this.cron = cron;
        this.timezone = timezone;
        this.payload = payload;
        this.maxDurationSeconds = maxDurationSeconds;
        this.enabled = enabled;
        this.nextRunAt = nextRunAt;
        this.createdAt = createdAt;
    }

    public String getCronId() {
        return cronId;
    }

    public void setCronId(String cronId) {
        this.cronId = cronId;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
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

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public Instant getNextRunAt() {
        return nextRunAt;
    }

    public void setNextRunAt(Instant nextRunAt) {
        this.nextRunAt = nextRunAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
}
