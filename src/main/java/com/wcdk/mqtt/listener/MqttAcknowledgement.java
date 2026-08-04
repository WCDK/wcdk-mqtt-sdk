package com.wcdk.mqtt.listener;

/**
 * @auther WCDK
 * @date 2026/7/9
 * @version 1.0
 **/
public interface MqttAcknowledgement {

    void commit();

    boolean isAcknowledged();
}
