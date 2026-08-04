package com.wcdk.mqtt.listener;

import com.wcdk.mqtt.config.MqttProperties;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Collections;
import java.util.List;

/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
public interface MqttMessageListener {

    default List<MqttProperties.Subscription> subscriptions() {
        return Collections.emptyList();
    }

    default void onMessage(String topic, MqttMessage message) throws Exception {
    }

    default void onMessage(String topic, MqttMessage message, MqttAcknowledgement acknowledgement) throws Exception {
    }
}