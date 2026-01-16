package com.example.scheduler.api.cron;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CronJobCreateRequest {
    private String cron; // UNIX 5-field cron expression
    private String timezone; // e.g. "UTC" or "Asia/Ho_Chi_Minh" (optional)
    private Object payload;
    private Integer maxDurationSeconds;
    private Boolean enabled;

    public CronJobCreateRequest() {
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
}
