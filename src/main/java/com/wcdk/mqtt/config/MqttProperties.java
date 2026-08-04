package com.wcdk.mqtt.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
@Data
@ConfigurationProperties(prefix = "wcdk.mqtt")
public class MqttProperties {

    private boolean enabled = true;

    private String serverUri = "tcp://127.0.0.1:1883";

    private List<String> serverUris = new ArrayList<>();

    private String clientId = "";

    private String username;

    private String password;

    private boolean cleanSession = true;

    private boolean automaticReconnect = true;

    private boolean manualAck;

    private Duration connectionTimeout = Duration.ofSeconds(30);

    private Duration keepAliveInterval = Duration.ofSeconds(60);

    private Duration completionTimeout = Duration.ofSeconds(10);

    private int maxInflight = 1000;

    private boolean asyncPublish = true;

    private int defaultQos = 1;

    private boolean defaultRetained = false;

    private boolean asyncListener = true;

    private int listenerCorePoolSize = 4;

    private int listenerMaxPoolSize = 16;

    private int listenerQueueCapacity = 10000;

    private List<Subscription> subscriptions = new ArrayList<>();

    public List<String> getResolvedServerUris() {
        List<String> resolved = new ArrayList<>();
        if (serverUris != null) {
            for (String uri : serverUris) {
                if (StringUtils.hasText(uri) && !resolved.contains(uri.trim())) {
                    resolved.add(uri.trim());
                }
            }
        }
        if (resolved.isEmpty() && StringUtils.hasText(serverUri)) {
            resolved.add(serverUri.trim());
        }
        return resolved;
    }

    @Data
    public static class Subscription {

        private String topic;

        private int qos = 1;
    }
}