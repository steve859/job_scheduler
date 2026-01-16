package com.example.scheduler.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobCreateRequest {
    private String schedule; // "immediate"/"now" or ISO-8601 Instant
    private String runAt; // ISO-8601 Instant (preferred for delayed jobs)
    private Long delaySeconds; // convenience: now + delaySeconds
    private Object payload; // arbitrary JSON object
    private Integer maxDurationSeconds;

    public JobCreateRequest() {
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    public String getRunAt() {
        return runAt;
    }

    public void setRunAt(String runAt) {
        this.runAt = runAt;
    }

    public Long getDelaySeconds() {
        return delaySeconds;
    }

    public void setDelaySeconds(Long delaySeconds) {
        this.delaySeconds = delaySeconds;
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
}
