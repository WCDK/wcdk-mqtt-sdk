package com.wcdk.mqtt.util;

import java.nio.charset.StandardCharsets;

import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.core.MqttClientManager;
import com.wcdk.mqtt.listener.MqttConnectionEvent;
import com.wcdk.mqtt.listener.MqttConnectionListener;
import com.wcdk.mqtt.listener.MqttIncomingMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/***
* 面向业务的响应式 MQTT 门面：默认 QoS 与 retained 取自 wcdk.mqtt 配置，
* 发布、订阅、取消订阅返回 Mono，入站消息与连接状态以 Flux 暴露；
* 所有 Mono 为冷流，必须订阅才会真正执行。
* @author wcdk
***/
public class MqttUtil {

    private final MqttClientManager mqttClientManager;

    private final MqttProperties mqttProperties;

    public MqttUtil(MqttClientManager mqttClientManager, MqttProperties mqttProperties) {
        this.mqttClientManager = mqttClientManager;
        this.mqttProperties = mqttProperties;
    }

    /***
    * 按默认 QoS 与 retained 发布 UTF-8 文本。
    * @author wcdk
    * @param topic 主题
    * @param payload 正文
    * @return 完成型 Mono
    ***/
    public Mono<Void> publish(String topic, String payload) {
        return publish(topic, payload, mqttProperties.getDefaultQos(), mqttProperties.isDefaultRetained());
    }

    /***
    * 发布 UTF-8 文本。
    * @author wcdk
    * @param topic 主题
    * @param payload 正文
    * @param qos QoS，取值 0、1、2
    * @param retained 是否为保留消息
    * @return 完成型 Mono
    ***/
    public Mono<Void> publish(String topic, String payload, int qos, boolean retained) {
        return mqttClientManager.publish(topic, payload == null ? "" : payload, qos, retained);
    }

    /***
    * 按默认 QoS 与 retained 发布消息体。
    * @author wcdk
    * @param topic 主题
    * @param payload 消息体
    * @return 完成型 Mono
    ***/
    public Mono<Void> publishBytes(String topic, byte[] payload) {
        return publishBytes(topic, payload, mqttProperties.getDefaultQos(), mqttProperties.isDefaultRetained());
    }

    /***
    * 发布消息体。
    * @author wcdk
    * @param topic 主题
    * @param payload 消息体
    * @param qos QoS，取值 0、1、2
    * @param retained 是否为保留消息
    * @return 完成型 Mono
    ***/
    public Mono<Void> publishBytes(String topic, byte[] payload, int qos, boolean retained) {
        return mqttClientManager.publish(topic, payload, qos, retained);
    }

    /***
    * 按默认 QoS 与 retained 发布按 UTF-8 解码的字节内容。
    * @author wcdk
    * @param topic 主题
    * @param payload 字节正文
    * @return 完成型 Mono
    ***/
    public Mono<Void> publishUtf8(String topic, byte[] payload) {
        return publish(topic, payload == null ? "" : new String(payload, StandardCharsets.UTF_8));
    }

    /***
    * 按默认 QoS 订阅主题，重连后自动重新订阅。
    * @author wcdk
    * @param topic 主题过滤器
    * @return 完成型 Mono
    ***/
    public Mono<Void> subscribe(String topic) {
        return subscribe(topic, mqttProperties.getDefaultQos());
    }

    /***
    * 订阅主题，重连后自动重新订阅。
    * @author wcdk
    * @param topic 主题过滤器
    * @param qos QoS，取值 0、1、2
    * @return 完成型 Mono
    ***/
    public Mono<Void> subscribe(String topic, int qos) {
        return mqttClientManager.subscribe(topic, qos);
    }

    /***
    * 取消订阅。
    * @author wcdk
    * @param topic 主题过滤器
    * @return 完成型 Mono
    ***/
    public Mono<Void> unsubscribe(String topic) {
        return mqttClientManager.unsubscribe(topic);
    }

    /***
    * 入站消息热流：接收全部消息，仅推送订阅之后到达的消息。
    * @author wcdk
    * @return 消息流
    ***/
    public Flux<MqttIncomingMessage> receive() {
        return mqttClientManager.receive();
    }

    /***
    * 入站消息热流：仅接收命中主题过滤器的消息，支持 + 与 # 通配符。
    * @author wcdk
    * @param topicFilter 主题过滤器，为空表示接收全部消息
    * @return 消息流
    ***/
    public Flux<MqttIncomingMessage> receive(String topicFilter) {
        return mqttClientManager.receive(topicFilter);
    }

    /***
    * 连接状态事件流：连接建立（含自动重连）与连接断开事件。
    * @author wcdk
    * @return 事件流
    ***/
    public Flux<MqttConnectionEvent> connectionEvents() {
        return mqttClientManager.connectionEvents();
    }

    /***
    * 当前是否已连接到 Broker。
    * @author wcdk
    * @return 连接状态
    ***/
    public boolean isConnected() {
        return mqttClientManager.isConnected();
    }

    /***
    * 客户端 ID。
    * @author wcdk
    * @return clientId
    ***/
    public String getClientId() {
        return mqttClientManager.getClientId();
    }

    /***
    * 当前连接的 Broker 地址。
    * @author wcdk
    * @return serverUri
    ***/
    public String getServerUri() {
        return mqttClientManager.getServerUri();
    }

    /***
    * 注册连接状态监听器。
    * @author wcdk
    * @param listener 监听器
    ***/
    public void addConnectionListener(MqttConnectionListener listener) {
        mqttClientManager.addConnectionListener(listener);
    }

    /***
    * 移除连接状态监听器。
    * @author wcdk
    * @param listener 监听器
    ***/
    public void removeConnectionListener(MqttConnectionListener listener) {
        mqttClientManager.removeConnectionListener(listener);
    }

    /***
    * 重新扫描监听器 Bean 声明的订阅，动态注册监听器后调用。
    * @author wcdk
    ***/
    public void refreshSubscriptions() {
        mqttClientManager.refreshSubscriptions();
    }
}
