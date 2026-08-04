package com.wcdk.mqtt.listener;

import java.nio.charset.StandardCharsets;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
public class LoggingMqttMessageListener implements MqttMessageListener {

    private static final Logger log = LoggerFactory.getLogger(LoggingMqttMessageListener.class);

    @Override
    public void onMessage(String topic, MqttMessage message) {
        String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
        log.info("Received MQTT message, topic={}, qos={}, retained={}, payload={}",
                topic, message.getQos(), message.isRetained(), payload);
    }
}
