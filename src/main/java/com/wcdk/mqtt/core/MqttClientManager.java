package com.wcdk.mqtt.core;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.listener.MqttAcknowledgement;
import com.wcdk.mqtt.listener.MqttMessageListener;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.StringUtils;
/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
public class MqttClientManager implements SmartLifecycle, MqttCallbackExtended {

    private static final Logger log = LoggerFactory.getLogger(MqttClientManager.class);

    private final MqttAsyncClient client;

    private final MqttConnectOptions connectOptions;

    private final MqttProperties properties;

    private final List<MqttMessageListener> messageListeners;

    private final Executor messageListenerExecutor;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private final AtomicBoolean connected = new AtomicBoolean(false);

    public MqttClientManager(MqttAsyncClient client,
                             MqttConnectOptions connectOptions,
                             MqttProperties properties,
                             List<MqttMessageListener> messageListeners) {
        this(client, connectOptions, properties, messageListeners, Runnable::run);
    }

    public MqttClientManager(MqttAsyncClient client,
                             MqttConnectOptions connectOptions,
                             MqttProperties properties,
                             List<MqttMessageListener> messageListeners,
                             Executor messageListenerExecutor) {
        this.client = client;
        this.connectOptions = connectOptions;
        this.properties = properties;
        this.messageListeners = messageListeners == null ? Collections.emptyList() : messageListeners;
        this.messageListenerExecutor = messageListenerExecutor == null ? Runnable::run : messageListenerExecutor;
        this.client.setCallback(this);
        configureManualAcks();
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        CompletableFuture.runAsync(() -> {
            try {
                connectIfNecessary();
            } catch (IllegalStateException ex) {
                connected.set(false);
                started.set(false);
                log.warn("MQTT client startup connection failed, system will continue, serverUris={}", properties.getResolvedServerUris(), ex);
            }
        });
    }

    @Override
    public void stop() {
        try {
            if (client.isConnected()) {
                client.disconnect().waitForCompletion(timeoutMillis());
            }
        } catch (MqttException ex) {
            log.warn("Disconnect MQTT client failed", ex);
        } finally {
            connected.set(false);
            started.set(false);
        }
    }

    @Override
    public boolean isRunning() {
        return started.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    @Override
    public void connectComplete(boolean reconnect, String serverUri) {
        connected.set(true);
        log.info("MQTT client connected to {}, reconnect={}", serverUri, reconnect);
        CompletableFuture.runAsync(() -> {
            try {
                subscribeConfiguredTopics();
            } catch (RuntimeException ex) {
                log.warn("Subscribe configured MQTT topics failed after connect, serverUri={}", serverUri, ex);
            }
        });
    }

    @Override
    public void connectionLost(Throwable cause) {
        connected.set(false);
        log.warn("MQTT connection lost", cause);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        for (MqttMessageListener listener : messageListeners) {
            Runnable task = () -> dispatchMessage(listener, topic, message);
            if (!properties.isAsyncListener()) {
                task.run();
                continue;
            }
            try {
                messageListenerExecutor.execute(task);
            } catch (RejectedExecutionException ex) {
                log.warn("MQTT listener executor rejected message, fallback to caller thread, topic={}", topic, ex);
                task.run();
            }
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        log.debug("MQTT message delivery complete, messageId={}", token.getMessageId());
    }

    public void publish(String topic, String payload, int qos, boolean retained) {
        publish(topic, payload == null ? null : payload.getBytes(StandardCharsets.UTF_8), qos, retained);
    }

    public void publish(String topic, byte[] payload, int qos, boolean retained) {
        if (properties.isAsyncPublish()) {
            publishAsync(topic, payload, qos, retained);
        } else {
            publishAndWait(topic, payload, qos, retained);
        }
    }

    public void publishAndWait(String topic, String payload, int qos, boolean retained) {
        publishAndWait(topic, payload == null ? null : payload.getBytes(StandardCharsets.UTF_8), qos, retained);
    }

    public void publishAndWait(String topic, byte[] payload, int qos, boolean retained) {
        MqttMessage message = createMessage(topic, payload, qos, retained);
        try {
            client.publish(topic, message).waitForCompletion(timeoutMillis());
        } catch (MqttException ex) {
            throw new IllegalStateException("Publish MQTT message failed, topic=" + topic, ex);
        }
    }

    public void publishAsync(String topic, String payload, int qos, boolean retained) {
        publishAsync(topic, payload == null ? null : payload.getBytes(StandardCharsets.UTF_8), qos, retained);
    }

    public void publishAsync(String topic, byte[] payload, int qos, boolean retained) {
        MqttMessage message = createMessage(topic, payload, qos, retained);
        try {
            IMqttDeliveryToken token = client.publish(topic, message);
            token.setActionCallback(new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    log.debug("Publish MQTT message success, topic={}, messageId={}", topic, asyncActionToken.getMessageId());
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    log.warn("Publish MQTT message failed, topic={}, messageId={}", topic,
                            asyncActionToken == null ? null : asyncActionToken.getMessageId(), exception);
                }
            });
        } catch (MqttException ex) {
            throw new IllegalStateException("Publish MQTT message failed, topic=" + topic, ex);
        }
    }

    public void subscribe(String topic, int qos) {
        assertTopic(topic);
        connectIfNecessary();
        try {
            client.subscribe(topic, normalizeQos(qos)).waitForCompletion(timeoutMillis());
            log.info("Subscribe MQTT topic success, topic={}, qos={}", topic, normalizeQos(qos));
        } catch (MqttException ex) {
            throw new IllegalStateException("Subscribe MQTT topic failed, topic=" + topic, ex);
        }
    }

    public void unsubscribe(String topic) {
        assertTopic(topic);
        if (!client.isConnected()) {
            return;
        }
        try {
            client.unsubscribe(topic).waitForCompletion(timeoutMillis());
            log.info("Unsubscribe MQTT topic success, topic={}", topic);
        } catch (MqttException ex) {
            throw new IllegalStateException("Unsubscribe MQTT topic failed, topic=" + topic, ex);
        }
    }

    public boolean isConnected() {
        return started.get() && connected.get() && client.isConnected();
    }

    private synchronized void connectIfNecessary() {
        if (client.isConnected()) {
            return;
        }
        try {
            client.connect(connectOptions).waitForCompletion(timeoutMillis());
        } catch (MqttException ex) {
            Throwable cause = ex.getCause();
            String causeMessage = cause == null
                    ? "none"
                    : cause.getClass().getSimpleName() + ": " + cause.getMessage();
            throw new IllegalStateException(
                    "Connect MQTT broker failed, serverUris=" + properties.getResolvedServerUris()
                            + ", reasonCode=" + ex.getReasonCode()
                            + ", reason=" + ex.getMessage()
                            + ", cause=" + causeMessage,
                    ex);
        }
    }

    private void dispatchMessage(MqttMessageListener listener, String topic, MqttMessage message) {
        try {
            if (properties.isManualAck()) {
                listener.onMessage(topic, message, new ClientMqttAcknowledgement(message.getId(), message.getQos()));
            } else {
                listener.onMessage(topic, message);
            }
        } catch (Exception ex) {
            log.warn("Handle MQTT message failed, topic={}", topic, ex);
        }
    }

    private MqttMessage createMessage(String topic, byte[] payload, int qos, boolean retained) {
        assertTopic(topic);
        connectIfNecessary();
        byte[] messagePayload = payload == null ? new byte[0] : payload.clone();
        MqttMessage message = new MqttMessage(messagePayload);
        message.setQos(normalizeQos(qos));
        message.setRetained(retained);
        return message;
    }

    private void subscribeConfiguredTopics() {
        Set<String> subscribedTopics = new LinkedHashSet<>();
        subscribeTopics(properties.getSubscriptions(), subscribedTopics);
        for (MqttMessageListener listener : messageListeners) {
            subscribeTopics(listener.subscriptions(), subscribedTopics);
        }
    }

    private void subscribeTopics(List<MqttProperties.Subscription> subscriptions, Set<String> subscribedTopics) {
        if (subscriptions == null) {
            return;
        }
        for (MqttProperties.Subscription subscription : subscriptions) {
            if (subscription != null && StringUtils.hasText(subscription.getTopic()) && subscribedTopics.add(subscription.getTopic())) {
                subscribe(subscription.getTopic(), subscription.getQos());
            }
        }
    }

    private long timeoutMillis() {
        Duration timeout = properties.getCompletionTimeout();
        return timeout == null ? 10_000L : Math.max(1_000L, timeout.toMillis());
    }

    private static int normalizeQos(int qos) {
        if (qos < 0 || qos > 2) {
            throw new IllegalArgumentException("MQTT QoS must be 0, 1 or 2");
        }
        return qos;
    }

    private static void assertTopic(String topic) {
        if (!StringUtils.hasText(topic)) {
            throw new IllegalArgumentException("MQTT topic must not be blank");
        }
    }

    private void configureManualAcks() {
        try {
            client.getClass().getMethod("setManualAcks", boolean.class).invoke(client, properties.isManualAck());
        } catch (ReflectiveOperationException ex) {
            if (properties.isManualAck()) {
                throw new IllegalStateException("Current MQTT client does not support manual ACK", ex);
            }
        }
    }

    private void completeMessage(int messageId, int qos) {
        try {
            client.getClass().getMethod("messageArrivedComplete", int.class, int.class).invoke(client, messageId, qos);
        } catch (ReflectiveOperationException ex) {
            throw new IllegalStateException("Manual ACK MQTT message failed, messageId=" + messageId, ex);
        }
    }

    private final class ClientMqttAcknowledgement implements MqttAcknowledgement {

        private final int messageId;

        private final int qos;

        private final AtomicBoolean acknowledged = new AtomicBoolean(false);

        private ClientMqttAcknowledgement(int messageId, int qos) {
            this.messageId = messageId;
            this.qos = qos;
        }

        @Override
        public void commit() {
            if (acknowledged.compareAndSet(false, true)) {
                completeMessage(messageId, qos);
            }
        }

        @Override
        public boolean isAcknowledged() {
            return acknowledged.get();
        }
    }
}