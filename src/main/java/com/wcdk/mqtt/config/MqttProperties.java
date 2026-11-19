package com.wcdk.mqtt.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/***
* wcdk.mqtt 前缀客户端配置映射，覆盖连接、认证、遗嘱、重连、离线缓冲与监听线程池等能力。
* @author wcdk
***/
@Data
@ConfigurationProperties(prefix = "wcdk.mqtt")
public class MqttProperties {

    /** 是否创建 MQTT 客户端相关 Bean */
    private boolean enabled = true;

    /** 单个 Broker 地址，如 tcp://127.0.0.1:1883 */
    private String serverUri = "tcp://127.0.0.1:1883";

    /** 多地址故障转移列表，非空时优先于 serverUri */
    private List<String> serverUris = new ArrayList<>();

    /** 客户端 ID，为空时自动生成 wcdk-mqtt-server-{UUID} */
    private String clientId = "";

    /** Broker 认证用户名 */
    private String username;

    /** Broker 认证密码 */
    private String password;

    /** 是否使用清洁会话 */
    private boolean cleanSession = true;

    /** 断线后是否由 Paho 自动重连 */
    private boolean automaticReconnect = true;

    /** 自动重连最大间隔，Paho 指数退避上限 */
    private Duration maxReconnectDelay = Duration.ofSeconds(128);

    /** 首次连接失败后的重试间隔，0 或负数表示不重试 */
    private Duration connectRetryInterval = Duration.ofSeconds(10);

    /** MQTT 协议版本：0=自动协商，3=MQTT 3.1，4=MQTT 3.1.1 */
    private int mqttVersion = 0;

    /** 是否手动确认收到的消息 */
    private boolean manualAck;

    /** 建立连接超时 */
    private Duration connectionTimeout = Duration.ofSeconds(30);

    /** 心跳间隔 */
    private Duration keepAliveInterval = Duration.ofSeconds(60);

    /** 同步发布、订阅等操作的等待超时，同时作为响应式 Mono 的超时上限 */
    private Duration completionTimeout = Duration.ofSeconds(10);

    /** 同时处于发送中的最大消息数，最小为 1 */
    private int maxInflight = 1000;

    /** MqttUtil 简化方法的默认 QoS，取值 0、1、2 */
    private int defaultQos = 1;

    /** MqttUtil 简化方法的默认 retained 标志 */
    private boolean defaultRetained = false;

    /** 消息监听器是否在线程池执行 */
    private boolean asyncListener = true;

    /** 消息监听线程池核心线程数 */
    private int listenerCorePoolSize = 4;

    /** 消息监听线程池最大线程数 */
    private int listenerMaxPoolSize = 16;

    /** 消息监听线程池队列容量 */
    private int listenerQueueCapacity = 10000;

    /** Flux 订阅流（receive）的缓冲容量，慢消费者超过该容量后新消息对订阅流跳过并告警，最小为 1 */
    private int streamBufferSize = 10000;

    /** 遗嘱主题，配置后客户端异常掉线时由 Broker 发布遗嘱消息 */
    private String willTopic;

    /** 遗嘱消息正文 */
    private String willPayload;

    /** 遗嘱 QoS，取值 0、1、2 */
    private int willQos = 1;

    /** 遗嘱是否为保留消息 */
    private boolean willRetained = false;

    /** 未连接时是否缓存待发送消息 */
    private boolean disconnectedBufferEnabled = false;

    /** 未连接缓存队列容量，最小为 1 */
    private int disconnectedBufferSize = 10000;

    /** 缓存队列满时是否删除最早的消息 */
    private boolean disconnectedBufferDeleteOldest = false;

    /** 连接成功及重连后自动订阅的主题列表 */
    private List<Subscription> subscriptions = new ArrayList<>();

    /***
    * 解析生效的 Broker 地址列表，serverUris 优先且去重，均为空时回退 serverUri。
    * @author wcdk
    * @return 地址列表
    ***/
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

    /***
    * 订阅主题与 QoS 的键值对，供配置、注解与编程式订阅统一使用。
    * @author wcdk
    ***/
    @Data
    public static class Subscription {

        /** 主题过滤器，支持 + 与 # 通配符 */
        private String topic;

        /** 订阅 QoS，取值 0、1、2 */
        private int qos = 1;

        public Subscription() {
        }

        public Subscription(String topic, int qos) {
            this.topic = topic;
            this.qos = qos;
        }
    }
}
