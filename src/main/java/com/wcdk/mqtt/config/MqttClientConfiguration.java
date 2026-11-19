package com.wcdk.mqtt.config;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.wcdk.mqtt.core.MqttClientManager;
import com.wcdk.mqtt.core.MqttListenerRegistrar;
import com.wcdk.mqtt.core.MqttTopics;
import com.wcdk.mqtt.listener.MqttConnectionListener;
import com.wcdk.mqtt.listener.MqttMessageListener;
import com.wcdk.mqtt.util.MqttUtil;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;

/***
* wcdk-mqtt-sdk 自动装配入口：随依赖引入即创建连接参数、异步客户端、监听线程池、
* 监听器注册器、客户端管理器与 MqttUtil；每个 Bean 均可用 @ConditionalOnMissingBean 覆盖。
* 旧版本的 @Import(MqttClientConfiguration.class) 显式启用方式继续有效。
* @author wcdk
***/
@AutoConfiguration
@ConditionalOnClass(MqttAsyncClient.class)
@EnableConfigurationProperties(MqttProperties.class)
@ConditionalOnProperty(prefix = "wcdk.mqtt", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MqttClientConfiguration {

    /***
    * 构建连接参数：认证、协议版本、超时、重连退避、多地址故障转移与遗嘱消息。
    * @author wcdk
    * @param properties 配置
    * @return 连接参数
    ***/
    @Bean
    @ConditionalOnMissingBean(MqttConnectOptions.class)
    public MqttConnectOptions mqttConnectOptions(MqttProperties properties) {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(properties.isCleanSession());
        options.setAutomaticReconnect(properties.isAutomaticReconnect());
        options.setMaxReconnectDelay(Math.max(1, toSeconds(properties.getMaxReconnectDelay(), 128)));
        options.setMqttVersion(properties.getMqttVersion());
        options.setConnectionTimeout(toSeconds(properties.getConnectionTimeout(), 30));
        options.setKeepAliveInterval(toSeconds(properties.getKeepAliveInterval(), 60));
        options.setMaxInflight(Math.max(1, properties.getMaxInflight()));

        List<String> serverUris = properties.getResolvedServerUris();
        if (!serverUris.isEmpty()) {
            options.setServerURIs(serverUris.toArray(new String[0]));
        }

        if (StringUtils.hasText(properties.getUsername())) {
            options.setUserName(properties.getUsername());
        }
        if (StringUtils.hasText(properties.getPassword())) {
            options.setPassword(properties.getPassword().toCharArray());
        }

        if (StringUtils.hasText(properties.getWillTopic())) {
            byte[] willPayload = properties.getWillPayload() == null
                    ? new byte[0]
                    : properties.getWillPayload().getBytes(StandardCharsets.UTF_8);
            options.setWill(properties.getWillTopic(), willPayload,
                    MqttTopics.normalizeQos(properties.getWillQos()), properties.isWillRetained());
        }
        return options;
    }

    /***
    * 构建 Paho 异步客户端，client-id 为空时自动生成。
    * @author wcdk
    * @param properties 配置
    * @return 异步客户端
    * @throws MqttException 客户端创建异常
    ***/
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(MqttAsyncClient.class)
    public MqttAsyncClient mqttAsyncClient(MqttProperties properties) throws MqttException {
        String clientId = StringUtils.hasText(properties.getClientId())
                ? properties.getClientId()
                : "wcdk-mqtt-server-" + UUID.randomUUID();
        List<String> serverUris = properties.getResolvedServerUris();
        String serverUri = serverUris.isEmpty() ? properties.getServerUri() : serverUris.get(0);
        return new MqttAsyncClient(serverUri, clientId, new MemoryPersistence());
    }

    /***
    * 消息监听线程池，daemon 线程命名 wcdk-mqtt-listener-{n}。
    * @author wcdk
    * @param properties 配置
    * @return 线程池
    ***/
    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "mqttMessageListenerExecutor")
    public ExecutorService mqttMessageListenerExecutor(MqttProperties properties) {
        int corePoolSize = Math.max(1, properties.getListenerCorePoolSize());
        int maxPoolSize = Math.max(corePoolSize, properties.getListenerMaxPoolSize());
        int queueCapacity = Math.max(1, properties.getListenerQueueCapacity());
        AtomicInteger threadIndex = new AtomicInteger();
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                runnable -> {
                    Thread thread = new Thread(runnable, "wcdk-mqtt-listener-" + threadIndex.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /***
    * @MqttListener 注解方法注册器，同时作为按注解主题过滤的消息监听器。
    * @author wcdk
    * @return 注册器
    ***/
    @Bean
    @ConditionalOnMissingBean(MqttListenerRegistrar.class)
    public static MqttListenerRegistrar mqttListenerRegistrar() {
        return new MqttListenerRegistrar();
    }

    /***
    * 客户端生命周期管理器。
    * @author wcdk
    * @param mqttAsyncClient 异步客户端
    * @param mqttConnectOptions 连接参数
    * @param properties 配置
    * @param messageListeners 消息监听器
    * @param connectionListeners 连接状态监听器
    * @param messageListenerExecutor 监听线程池
    * @return 管理器
    ***/
    @Bean
    @ConditionalOnMissingBean(MqttClientManager.class)
    public MqttClientManager mqttClientManager(MqttAsyncClient mqttAsyncClient,
                                               MqttConnectOptions mqttConnectOptions,
                                               MqttProperties properties,
                                               ObjectProvider<MqttMessageListener> messageListeners,
                                               ObjectProvider<MqttConnectionListener> connectionListeners,
                                               ExecutorService mqttMessageListenerExecutor) {
        return new MqttClientManager(mqttAsyncClient, mqttConnectOptions, properties,
                messageListeners.orderedStream().toList(),
                mqttMessageListenerExecutor,
                connectionListeners.orderedStream().toList());
    }

    /***
    * 常用发布、订阅与连接状态门面。
    * @author wcdk
    * @param mqttClientManager 管理器
    * @param properties 配置
    * @return 门面
    ***/
    @Bean
    @ConditionalOnMissingBean(MqttUtil.class)
    public MqttUtil mqttUtil(MqttClientManager mqttClientManager, MqttProperties properties) {
        return new MqttUtil(mqttClientManager, properties);
    }

    private static int toSeconds(java.time.Duration duration, int fallback) {
        if (duration == null) {
            return fallback;
        }
        long seconds = Math.max(1, duration.toSeconds());
        return seconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) seconds;
    }
}
