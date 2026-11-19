package com.wcdk.mqtt.listener;

import java.nio.charset.StandardCharsets;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
* 默认日志打印监听器：订阅日志主题即可把消息以日志形式输出，用于快速验证连通性。
* @author wcdk
**/
public class LoggingMqttMessageListener implements MqttMessageListener {

    private static final Logger log = LoggerFactory.getLogger(LoggingMqttMessageListener.class);

    @Override
    public void onMessage(String topic, MqttMessage message) {
        String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
        log.info("收到MQTT消息，主题：{}，QoS：{}，是否保留：{}，内容：{}",
                topic, message.getQos(), message.isRetained(), payload);
    }
}
