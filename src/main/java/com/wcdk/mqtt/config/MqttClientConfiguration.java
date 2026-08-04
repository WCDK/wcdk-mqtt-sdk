package com.wcdk.mqtt.config;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.wcdk.mqtt.core.MqttClientManager;
import com.wcdk.mqtt.core.MqttListenerRegistrar;
import com.wcdk.mqtt.listener.MqttMessageListener;
import com.wcdk.mqtt.util.MqttUtil;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(MqttAsyncClient.class)
@EnableConfigurationProperties(MqttProperties.class)
@ConditionalOnProperty(prefix = "wcdk.mqtt", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MqttClientConfiguration {

    @Bean
    public MqttConnectOptions mqttConnectOptions(MqttProperties properties) {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(properties.isCleanSession());
        options.setAutomaticReconnect(properties.isAutomaticReconnect());
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
        return options;
    }

    @Bean(destroyMethod = "close")
    public MqttAsyncClient mqttAsyncClient(MqttProperties properties) throws MqttException {
        String clientId = StringUtils.hasText(properties.getClientId())
                ? properties.getClientId()
                : "wcdk-mqtt-server-" + UUID.randomUUID();
        List<String> serverUris = properties.getResolvedServerUris();
        String serverUri = serverUris.isEmpty() ? properties.getServerUri() : serverUris.get(0);
        return new MqttAsyncClient(serverUri, clientId, new MemoryPersistence());
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService mqttMessageListenerExecutor(MqttProperties mqttProperties) {
        int corePoolSize = Math.max(1, mqttProperties.getListenerCorePoolSize());
        int maxPoolSize = Math.max(corePoolSize, mqttProperties.getListenerMaxPoolSize());
        int queueCapacity = Math.max(1, mqttProperties.getListenerQueueCapacity());
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

    @Bean
    public static MqttListenerRegistrar mqttListenerRegistrar() {
        return new MqttListenerRegistrar();
    }

    @Bean
    public MqttClientManager mqttClientManager(MqttAsyncClient mqttAsyncClient,
                                               MqttConnectOptions mqttConnectOptions,
                                               MqttProperties mqttProperties,
                                               List<MqttMessageListener> messageListeners,
                                               ExecutorService mqttMessageListenerExecutor) {
        return new MqttClientManager(mqttAsyncClient, mqttConnectOptions, mqttProperties,
                messageListeners, mqttMessageListenerExecutor);
    }

    @Bean
    public MqttUtil mqttUtil(MqttClientManager mqttClientManager, MqttProperties mqttProperties) {
        return new MqttUtil(mqttClientManager, mqttProperties);
    }

    private static int toSeconds(java.time.Duration duration, int fallback) {
        if (duration == null) {
            return fallback;
        }
        long seconds = Math.max(1, duration.toSeconds());
        return seconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) seconds;
    }
}
