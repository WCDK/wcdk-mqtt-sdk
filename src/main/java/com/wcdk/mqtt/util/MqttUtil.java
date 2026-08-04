package com.wcdk.mqtt.util;

import java.nio.charset.StandardCharsets;

import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.core.MqttClientManager;
/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
public class MqttUtil {

    private final MqttClientManager mqttClientManager;

    private final MqttProperties mqttProperties;

    public MqttUtil(MqttClientManager mqttClientManager, MqttProperties mqttProperties) {
        this.mqttClientManager = mqttClientManager;
        this.mqttProperties = mqttProperties;
    }

    public void publish(String topic, String payload) {
        publish(topic, payload, mqttProperties.getDefaultQos(), mqttProperties.isDefaultRetained());
    }

    public void publish(String topic, String payload, int qos, boolean retained) {
        mqttClientManager.publish(topic, payload == null ? "" : payload, qos, retained);
    }

    public void publishBytes(String topic, byte[] payload) {
        publishBytes(topic, payload, mqttProperties.getDefaultQos(), mqttProperties.isDefaultRetained());
    }

    public void publishBytes(String topic, byte[] payload, int qos, boolean retained) {
        mqttClientManager.publish(topic, payload, qos, retained);
    }

    public void publishAndWait(String topic, String payload) {
        publishAndWait(topic, payload, mqttProperties.getDefaultQos(), mqttProperties.isDefaultRetained());
    }

    public void publishAndWait(String topic, String payload, int qos, boolean retained) {
        mqttClientManager.publishAndWait(topic, payload == null ? "" : payload, qos, retained);
    }

    public void publishBytesAndWait(String topic, byte[] payload) {
        publishBytesAndWait(topic, payload, mqttProperties.getDefaultQos(), mqttProperties.isDefaultRetained());
    }

    public void publishBytesAndWait(String topic, byte[] payload, int qos, boolean retained) {
        mqttClientManager.publishAndWait(topic, payload, qos, retained);
    }

    public void publishUtf8(String topic, byte[] payload) {
        publish(topic, payload == null ? "" : new String(payload, StandardCharsets.UTF_8));
    }

    public void subscribe(String topic) {
        subscribe(topic, mqttProperties.getDefaultQos());
    }

    public void subscribe(String topic, int qos) {
        mqttClientManager.subscribe(topic, qos);
    }

    public void unsubscribe(String topic) {
        mqttClientManager.unsubscribe(topic);
    }

    public boolean isConnected() {
        return mqttClientManager.isConnected();
    }
}
