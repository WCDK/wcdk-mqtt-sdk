package com.wcdk.mqtt.core;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.listener.MqttAcknowledgement;
import com.wcdk.mqtt.listener.MqttConnectionEvent;
import com.wcdk.mqtt.listener.MqttConnectionListener;
import com.wcdk.mqtt.listener.MqttIncomingMessage;
import com.wcdk.mqtt.listener.MqttMessageListener;
import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions;
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
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

/***
* MQTT 客户端生命周期与响应式收发能力入口：连接管理、断线重连、首次连接失败重试、
* 连接后恢复订阅（配置订阅、监听器订阅与动态订阅）、Mono 发布/订阅、Flux 入站消息流与连接事件流、
* 手动 ACK 与连接状态回调。Paho 回调统一适配为 Mono/Flux，所有收发操作不在订阅线程上阻塞等待。
* @author wcdk
***/
public class MqttClientManager implements SmartLifecycle, MqttCallbackExtended {

    private static final Logger log = LoggerFactory.getLogger(MqttClientManager.class);

    private final MqttAsyncClient client;

    private final MqttConnectOptions connectOptions;

    private final MqttProperties properties;

    private final List<MqttMessageListener> messageListeners;

    private final List<MqttConnectionListener> connectionListeners = new CopyOnWriteArrayList<>();

    private final Map<String, Integer> dynamicSubscriptions = new ConcurrentHashMap<>();

    private final Executor messageListenerExecutor;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private final AtomicBoolean connected = new AtomicBoolean(false);

    private volatile String connectedServerUri;

    private volatile Map<MqttMessageListener, List<MqttProperties.Subscription>> subscriptionIndex;

    /** 入站消息热流，监听器与 Flux 消费方共享同一批消息 */
    private final Sinks.Many<MqttIncomingMessage> messageSink;

    /** 连接状态事件热流 */
    private final Sinks.Many<MqttConnectionEvent> connectionEventSink;

    /** 进行中的连接尝试（单飞），多个操作共享同一次连接避免并发重复连接 */
    private final AtomicReference<Mono<Void>> pendingConnection = new AtomicReference<>();

    /** 连接守护订阅，stop() 时取消 */
    private volatile Disposable connectionDisposable;

    public MqttClientManager(MqttAsyncClient client,
                             MqttConnectOptions connectOptions,
                             MqttProperties properties,
                             List<MqttMessageListener> messageListeners) {
        this(client, connectOptions, properties, messageListeners, Runnable::run, Collections.emptyList());
    }

    public MqttClientManager(MqttAsyncClient client,
                             MqttConnectOptions connectOptions,
                             MqttProperties properties,
                             List<MqttMessageListener> messageListeners,
                             Executor messageListenerExecutor) {
        this(client, connectOptions, properties, messageListeners, messageListenerExecutor,
                Collections.emptyList());
    }

    /***
    * 构造客户端管理器。
    * @author wcdk
    * @param client Paho 异步客户端
    * @param connectOptions 连接参数
    * @param properties SDK 配置
    * @param messageListeners 消息监听器集合
    * @param messageListenerExecutor 消息监听执行器
    * @param connectionListeners 连接状态监听器集合
    ***/
    public MqttClientManager(MqttAsyncClient client,
                             MqttConnectOptions connectOptions,
                             MqttProperties properties,
                             List<MqttMessageListener> messageListeners,
                             Executor messageListenerExecutor,
                             List<MqttConnectionListener> connectionListeners) {
        this.client = client;
        this.connectOptions = connectOptions;
        this.properties = properties;
        this.messageListeners = messageListeners == null ? Collections.emptyList() : messageListeners;
        this.messageListenerExecutor = messageListenerExecutor == null ? Runnable::run : messageListenerExecutor;
        if (connectionListeners != null) {
            this.connectionListeners.addAll(connectionListeners);
        }
        // 缓冲容量受 stream-buffer-size 约束，慢消费者不会阻塞 Paho 收包线程
        this.messageSink = Sinks.many().multicast()
                .onBackpressureBuffer(Math.max(1, properties.getStreamBufferSize()), false);
        this.connectionEventSink = Sinks.many().multicast().onBackpressureBuffer(16, false);
        this.client.setCallback(this);
        configureManualAcks();
        configureDisconnectedBuffer();
    }

    /***
    * 启动客户端：以连接守护在后台建立连接，首次失败时按 connect-retry-interval 反复重试，不阻塞应用启动。
    * @author wcdk
    ***/
    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        superviseConnection("启动连接");
    }

    /***
    * 停止客户端：取消连接守护与后续重试并正常断开连接。
    * @author wcdk
    ***/
    @Override
    public void stop() {
        started.set(false);
        Disposable supervisor = this.connectionDisposable;
        if (supervisor != null && !supervisor.isDisposed()) {
            // 停止连接守护，避免停止后继续发起连接
            supervisor.dispose();
        }
        if (!client.isConnected()) {
            connected.set(false);
            return;
        }
        // 生命周期订阅边界：主动订阅断开流程，失败仅记录日志
        Mono.<Void>create(sink -> {
            try {
                client.disconnect(null, new IMqttActionListener() {
                    @Override
                    public void onSuccess(IMqttToken asyncActionToken) {
                        connected.set(false);
                        sink.success();
                    }

                    @Override
                    public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                        connected.set(false);
                        sink.error(exception);
                    }
                });
            } catch (MqttException ex) {
                connected.set(false);
                sink.error(ex);
            }
        })
                .timeout(timeoutDuration())
                .doOnSuccess(value -> {
                    log.info("MQTT客户端已断开连接");
                    emitConnectionEvent(MqttConnectionEvent.disconnected(null));
                })
                .subscribe(value -> { },
                        ex -> log.warn("断开MQTT连接失败，异常信息：{}", ex.getMessage(), ex));
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
        this.connectedServerUri = serverUri;
        log.info("MQTT客户端连接成功，地址：{}，是否重连：{}", serverUri, reconnect);
        emitConnectionEvent(MqttConnectionEvent.connected(reconnect, serverUri));
        // 回调线程即订阅边界：恢复订阅完成后再通知连接监听器
        subscribeConfiguredTopics()
                .onErrorResume(ex -> {
                    log.warn("连接后恢复订阅失败，地址：{}，异常信息：{}", serverUri, ex.getMessage(), ex);
                    return Mono.empty();
                })
                .doFinally(signalType -> notifyAsync(() -> notifyConnected(reconnect)))
                .subscribe(value -> { },
                        ex -> log.warn("连接后恢复订阅处理失败，地址：{}，异常信息：{}", serverUri, ex.getMessage(), ex));
    }

    @Override
    public void connectionLost(Throwable cause) {
        connected.set(false);
        log.warn("MQTT连接断开，异常信息：{}", cause == null ? "无" : cause.getMessage(), cause);
        emitConnectionEvent(MqttConnectionEvent.disconnected(cause));
        notifyAsync(() -> notifyDisconnected(cause));
        // Paho 自动重连关闭时，由 SDK 的连接守护恢复连接
        if (started.get() && !properties.isAutomaticReconnect()) {
            superviseConnection("断线重连");
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        List<MqttMessageListener> targets = new ArrayList<>();
        for (Map.Entry<MqttMessageListener, List<MqttProperties.Subscription>> entry
                : subscriptionIndex().entrySet()) {
            if (matchesSubscriptions(entry.getValue(), topic)) {
                targets.add(entry.getKey());
            }
        }
        // 同一条消息只生成一个确认对象，监听器与 Flux 订阅流共享，任一 commit 即完成确认
        ClientMqttAcknowledgement acknowledgement = properties.isManualAck()
                ? new ClientMqttAcknowledgement(message.getId(), message.getQos())
                : null;
        // 先推送到 Flux 订阅流（仅在存在订阅者时）
        boolean deliveredToStream = emitToMessageStream(topic, message, acknowledgement);
        if (targets.isEmpty()) {
            if (acknowledgement != null && !deliveredToStream) {
                // 手动 ACK 模式下没有任何消费方的消息必须立即确认，否则会占满在途窗口
                acknowledge(message);
            }
            return;
        }
        for (MqttMessageListener listener : targets) {
            Runnable task = () -> dispatchMessage(listener, topic, message, acknowledgement);
            if (!properties.isAsyncListener()) {
                task.run();
                continue;
            }
            try {
                messageListenerExecutor.execute(task);
            } catch (RejectedExecutionException ex) {
                log.warn("MQTT监听线程池拒绝消息，回退到当前线程执行，主题：{}，异常信息：{}",
                        topic, ex.getMessage(), ex);
                task.run();
            }
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        log.debug("MQTT消息投递完成，消息ID：{}", token.getMessageId());
    }

    /***
    * 发布 UTF-8 文本，Mono 在消息发送流程完成时结束；冷流需订阅才会真正发送。
    * @author wcdk
    * @param topic 主题
    * @param payload UTF-8 文本正文，null 按空消息发送
    * @param qos QoS，取值 0、1、2
    * @param retained 是否为保留消息
    * @return 完成型 Mono
    ***/
    public Mono<Void> publish(String topic, String payload, int qos, boolean retained) {
        return publish(topic, payload == null ? null : payload.getBytes(StandardCharsets.UTF_8), qos, retained);
    }

    /***
    * 发布消息体，Mono 在消息发送流程完成时结束；冷流需订阅才会真正发送。
    * @author wcdk
    * @param topic 主题
    * @param payload 消息体，null 按空消息发送
    * @param qos QoS，取值 0、1、2
    * @param retained 是否为保留消息
    * @return 完成型 Mono
    ***/
    public Mono<Void> publish(String topic, byte[] payload, int qos, boolean retained) {
        return Mono.defer(() -> {
            MqttMessage message = createMessage(topic, payload, qos, retained);
            // 启用离线缓冲时由 Paho 排队，未启用则先尝试把连接补上
            Mono<Void> ensure = properties.isDisconnectedBufferEnabled() ? Mono.empty() : ensureConnected();
            return ensure.then(sendMessage(topic, message));
        });
    }

    /***
    * 订阅主题并登记为动态订阅，断线重连后自动重新订阅。
    * @author wcdk
    * @param topic 主题过滤器，支持 + 与 # 通配符
    * @param qos QoS，取值 0、1、2
    * @return 完成型 Mono
    ***/
    public Mono<Void> subscribe(String topic, int qos) {
        return Mono.defer(() -> {
            MqttTopics.assertTopic(topic);
            int normalizedQos = MqttTopics.normalizeQos(qos);
            // 登记为动态订阅：即使本次订阅失败，连接恢复后也会自动补订阅
            dynamicSubscriptions.put(topic, normalizedQos);
            return ensureConnected()
                    .then(subscribeOne(topic, normalizedQos))
                    .doOnSuccess(value -> log.info("订阅MQTT主题成功，主题：{}，QoS：{}", topic, normalizedQos));
        });
    }

    /***
    * 取消订阅并移除动态订阅登记。
    * @author wcdk
    * @param topic 主题过滤器
    * @return 完成型 Mono，未连接时直接完成
    ***/
    public Mono<Void> unsubscribe(String topic) {
        return Mono.defer(() -> {
            MqttTopics.assertTopic(topic);
            // 先移除动态订阅登记，避免重连后重新订阅
            dynamicSubscriptions.remove(topic);
            if (!client.isConnected()) {
                return Mono.empty();
            }
            return Mono.<Void>create(sink -> {
                try {
                    client.unsubscribe(topic, null,
                            completionListener(sink, "取消订阅MQTT主题", "主题：" + topic));
                } catch (MqttException ex) {
                    sink.error(new IllegalStateException(
                            "取消订阅MQTT主题失败，主题：" + topic + "，原因码：" + ex.getReasonCode(), ex));
                }
            })
                    .timeout(timeoutDuration())
                    .doOnSuccess(value -> log.info("取消订阅MQTT主题成功，主题：{}", topic))
                    .doOnError(ex -> log.warn("取消订阅MQTT主题失败，主题：{}，异常信息：{}",
                            topic, ex.getMessage(), ex));
        });
    }

    /***
    * 入站消息热流：接收客户端收到的全部消息，仅推送订阅之后到达的消息。
    * @author wcdk
    * @return 消息流
    ***/
    public Flux<MqttIncomingMessage> receive() {
        return messageSink.asFlux();
    }

    /***
    * 入站消息热流：仅接收命中主题过滤器的消息，支持 + 与 # 通配符。
    * @author wcdk
    * @param topicFilter 主题过滤器，为空表示接收全部消息
    * @return 消息流
    ***/
    public Flux<MqttIncomingMessage> receive(String topicFilter) {
        if (!StringUtils.hasText(topicFilter)) {
            return receive();
        }
        // 延迟校验，非法主题以 onError 形式反馈
        return Flux.defer(() -> {
            MqttTopics.assertTopic(topicFilter);
            return messageSink.asFlux()
                    .filter(incoming -> MqttTopics.matches(topicFilter, incoming.topic()));
        });
    }

    /***
    * 连接状态事件流：连接建立（含自动重连）与连接断开事件。
    * @author wcdk
    * @return 事件流
    ***/
    public Flux<MqttConnectionEvent> connectionEvents() {
        return connectionEventSink.asFlux();
    }

    /***
    * 当前是否已连接到 Broker。
    * @author wcdk
    * @return 连接状态
    ***/
    public boolean isConnected() {
        return client.isConnected();
    }

    /***
    * 客户端 ID。
    * @author wcdk
    * @return clientId
    ***/
    public String getClientId() {
        return client.getClientId();
    }

    /***
    * 当前连接的 Broker 地址，故障转移后返回实际连接成功的地址。
    * @author wcdk
    * @return serverUri
    ***/
    public String getServerUri() {
        String uri = this.connectedServerUri;
        return uri != null ? uri : client.getServerURI();
    }

    /***
    * 注册连接状态监听器。
    * @author wcdk
    * @param listener 监听器
    ***/
    public void addConnectionListener(MqttConnectionListener listener) {
        if (listener != null) {
            connectionListeners.add(listener);
        }
    }

    /***
    * 移除连接状态监听器。
    * @author wcdk
    * @param listener 监听器
    ***/
    public void removeConnectionListener(MqttConnectionListener listener) {
        if (listener != null) {
            connectionListeners.remove(listener);
        }
    }

    /***
    * 重新扫描各 MqttMessageListener 的 subscriptions()，动态注册监听器 Bean 后调用。
    * @author wcdk
    ***/
    public void refreshSubscriptions() {
        this.subscriptionIndex = null;
    }

    /***
    * 连接守护：单飞发起连接，首次连接失败或断线后按 connect-retry-interval 持续重试。
    * @author wcdk
    * @param reason 触发原因，用于日志
    ***/
    private void superviseConnection(String reason) {
        Duration retryInterval = properties.getConnectRetryInterval();
        Mono<Void> connection = ensureConnected();
        if (retryInterval != null && !retryInterval.isZero() && !retryInterval.isNegative()) {
            long delayMillis = Math.max(1_000L, retryInterval.toMillis());
            // 连接守护需要持续重试（等同 Paho 自动重连语义），故不设置重试上限
            connection = connection.retryWhen(
                    Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(delayMillis))
                            .doBeforeRetry(signal -> log.warn("{}失败，{}秒后进行第{}次重试，地址列表：{}",
                                    reason, delayMillis / 1000, signal.totalRetries() + 1,
                                    properties.getResolvedServerUris())));
        }
        Disposable previous = this.connectionDisposable;
        if (previous != null && !previous.isDisposed()) {
            previous.dispose();
        }
        // 生命周期订阅边界：主动订阅连接守护，stop() 时取消
        this.connectionDisposable = connection.subscribe(
                value -> { },
                ex -> log.error("{}失败，已按配置停止重试，地址列表：{}",
                        reason, properties.getResolvedServerUris(), ex));
    }

    /***
    * 确保已连接：已连接直接完成，否则共享或发起一次连接尝试（回调式，不阻塞订阅线程）。
    * @author wcdk
    * @return 完成型 Mono
    ***/
    private Mono<Void> ensureConnected() {
        return Mono.defer(() -> {
            if (client.isConnected()) {
                return Mono.empty();
            }
            Mono<Void> pending = pendingConnection.get();
            if (pending != null) {
                // 共享进行中的连接尝试，避免并发重复连接
                return pending;
            }
            Mono<Void> attempt = createConnectionAttempt().cache();
            if (pendingConnection.compareAndSet(null, attempt)) {
                return attempt;
            }
            return pendingConnection.get();
        });
    }

    /***
    * 创建单次连接尝试；终止后清理占位以便后续重连，外层超时仅作为连接回调丢失时的兜底。
    * @author wcdk
    * @return 完成型 Mono
    ***/
    private Mono<Void> createConnectionAttempt() {
        return Mono.<Void>create(sink -> {
            try {
                client.connect(connectOptions, null, new IMqttActionListener() {
                    @Override
                    public void onSuccess(IMqttToken asyncActionToken) {
                        connected.set(true);
                        started.set(true);
                        sink.success();
                    }

                    @Override
                    public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                        connected.set(false);
                        sink.error(connectionFailure(exception));
                    }
                });
            } catch (MqttException ex) {
                connected.set(false);
                sink.error(connectionFailure(ex));
            }
        })
                .timeout(connectTimeoutDuration())
                // 放在 cache 之前保证占位清理只执行一次（含超时取消场景）
                .doFinally(signalType -> pendingConnection.set(null))
                .doOnError(ex -> log.warn("MQTT客户端连接失败，地址列表：{}，异常信息：{}",
                        properties.getResolvedServerUris(), ex.getMessage()))
                .doOnSuccess(value -> log.debug("MQTT客户端连接尝试成功"));
    }

    /***
    * 发送消息：Paho 回调适配为 Mono，完成时表示发送流程结束。
    * @author wcdk
    * @param topic 主题
    * @param message 消息
    * @return 完成型 Mono
    ***/
    private Mono<Void> sendMessage(String topic, MqttMessage message) {
        return Mono.<Void>create(sink -> {
            try {
                client.publish(topic, message, null,
                        completionListener(sink, "发布MQTT消息", "主题：" + topic));
            } catch (MqttException ex) {
                sink.error(new IllegalStateException(
                        "发布MQTT消息失败，主题：" + topic + "，原因码：" + ex.getReasonCode(), ex));
            }
        })
                .timeout(timeoutDuration())
                .doOnSuccess(value -> log.debug("发布MQTT消息成功，主题：{}", topic))
                .doOnError(ex -> log.warn("发布MQTT消息失败，主题：{}，异常信息：{}",
                        topic, ex.getMessage(), ex));
    }

    /***
    * 订阅单个主题：Paho 回调适配为 Mono。
    * @author wcdk
    * @param topic 主题过滤器
    * @param qos QoS
    * @return 完成型 Mono
    ***/
    private Mono<Void> subscribeOne(String topic, int qos) {
        return Mono.<Void>create(sink -> {
            try {
                client.subscribe(topic, qos, null,
                        completionListener(sink, "订阅MQTT主题", "主题：" + topic));
            } catch (MqttException ex) {
                sink.error(new IllegalStateException(
                        "订阅MQTT主题失败，主题：" + topic + "，原因码：" + ex.getReasonCode(), ex));
            }
        })
                .timeout(timeoutDuration())
                .doOnError(ex -> log.warn("订阅MQTT主题失败，主题：{}，QoS：{}，异常信息：{}",
                        topic, qos, ex.getMessage(), ex));
    }

    /***
    * 把 Paho 操作回调适配为 Mono 完成信号，失败统一转换为中文说明的 IllegalStateException。
    * @author wcdk
    * @param sink Mono 信号
    * @param action 操作名（中文）
    * @param detail 失败上下文，如主题
    * @return Paho 操作回调
    ***/
    private IMqttActionListener completionListener(MonoSink<Void> sink, String action, String detail) {
        return new IMqttActionListener() {
            @Override
            public void onSuccess(IMqttToken asyncActionToken) {
                sink.success();
            }

            @Override
            public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                sink.error(new IllegalStateException(action + "失败，" + detail
                        + "，异常信息：" + exception.getMessage(), exception));
            }
        };
    }

    /***
    * 包装连接失败异常，附带地址列表与原因码便于排查。
    * @author wcdk
    * @param ex 原始异常
    * @return 中文说明的连接异常
    ***/
    private IllegalStateException connectionFailure(Throwable ex) {
        String base = "连接MQTT Broker失败，地址列表：" + properties.getResolvedServerUris();
        if (ex instanceof MqttException mqttException) {
            Throwable cause = mqttException.getCause();
            String causeMessage = cause == null
                    ? "无"
                    : cause.getClass().getSimpleName() + "：" + cause.getMessage();
            return new IllegalStateException(base + "，原因码：" + mqttException.getReasonCode()
                    + "，原因：" + mqttException.getMessage() + "，根因：" + causeMessage, mqttException);
        }
        return new IllegalStateException(base + "，异常信息：" + ex.getMessage(), ex);
    }

    private void dispatchMessage(MqttMessageListener listener, String topic, MqttMessage message,
                                 ClientMqttAcknowledgement acknowledgement) {
        try {
            if (acknowledgement != null) {
                listener.onMessage(topic, message, acknowledgement);
            } else {
                listener.onMessage(topic, message);
            }
        } catch (Exception ex) {
            log.warn("处理MQTT消息失败，主题：{}，异常信息：{}", topic, ex.getMessage(), ex);
        }
    }

    private MqttMessage createMessage(String topic, byte[] payload, int qos, boolean retained) {
        MqttTopics.assertTopic(topic);
        byte[] messagePayload = payload == null ? new byte[0] : payload.clone();
        MqttMessage message = new MqttMessage(messagePayload);
        message.setQos(MqttTopics.normalizeQos(qos));
        message.setRetained(retained);
        return message;
    }

    private boolean emitToMessageStream(String topic, MqttMessage message,
                                        ClientMqttAcknowledgement acknowledgement) {
        if (messageSink.currentSubscriberCount() == 0) {
            return false;
        }
        Sinks.EmitResult result = messageSink.tryEmitNext(
                new MqttIncomingMessage(topic, message, acknowledgement));
        if (result.isSuccess()) {
            return true;
        }
        // 溢出等失败时不向订阅流投递，若无监听器消费则由调用方兜底确认
        log.warn("推送MQTT消息到订阅流失败，主题：{}，结果：{}", topic, result);
        return false;
    }

    private void emitConnectionEvent(MqttConnectionEvent event) {
        if (connectionEventSink.currentSubscriberCount() == 0) {
            return;
        }
        Sinks.EmitResult result = connectionEventSink.tryEmitNext(event);
        if (result.isFailure()) {
            log.warn("推送MQTT连接事件到订阅流失败，事件：{}，结果：{}",
                    event.connected() ? "连接建立" : "连接断开", result);
        }
    }

    /***
    * 收集连接后需要恢复的全部主题：配置订阅、监听器订阅与动态订阅。
    * @author wcdk
    * @return 主题到 QoS 的映射
    ***/
    private Map<String, Integer> collectConfiguredTopics() {
        Set<String> collectedTopics = new LinkedHashSet<>();
        Map<String, Integer> topics = new LinkedHashMap<>();
        collectTopics(properties.getSubscriptions(), collectedTopics, topics);
        for (MqttMessageListener listener : messageListeners) {
            collectTopics(listener.subscriptions(), collectedTopics, topics);
        }
        topics.putAll(dynamicSubscriptions);
        return topics;
    }

    /***
    * 连接后恢复订阅：优先批量订阅，失败时逐个订阅并容忍单个失败。
    * @author wcdk
    * @return 完成型 Mono
    ***/
    private Mono<Void> subscribeConfiguredTopics() {
        Map<String, Integer> topics = collectConfiguredTopics();
        if (topics.isEmpty()) {
            return Mono.empty();
        }
        String[] topicArray = topics.keySet().toArray(new String[0]);
        int[] qosArray = new int[topicArray.length];
        int index = 0;
        for (String topic : topicArray) {
            qosArray[index++] = topics.get(topic);
        }
        List<String> topicList = List.of(topicArray);
        return Mono.<Void>create(sink -> {
            try {
                client.subscribe(topicArray, qosArray, null,
                        completionListener(sink, "批量订阅MQTT主题", "主题：" + topicList));
            } catch (MqttException ex) {
                sink.error(new IllegalStateException(
                        "批量订阅MQTT主题失败，主题：" + topicList + "，原因码：" + ex.getReasonCode(), ex));
            }
        })
                .timeout(timeoutDuration())
                .doOnSuccess(value -> log.info("连接后恢复订阅成功，主题：{}", topicList))
                .doOnError(ex -> log.warn("批量订阅MQTT主题失败，将逐个订阅，主题：{}，异常信息：{}",
                        topicList, ex.getMessage(), ex))
                .onErrorResume(batchEx -> Flux.fromArray(topicArray)
                        .concatMap(topic -> subscribeOne(topic, topics.get(topic))
                                // 单个主题失败已记录日志，不阻断其余主题
                                .onErrorResume(ex -> Mono.empty()))
                        .then());
    }

    private void collectTopics(List<MqttProperties.Subscription> subscriptions,
                               Set<String> collected,
                               Map<String, Integer> topics) {
        if (subscriptions == null) {
            return;
        }
        for (MqttProperties.Subscription subscription : subscriptions) {
            if (subscription != null && StringUtils.hasText(subscription.getTopic())
                    && collected.add(subscription.getTopic())) {
                topics.put(subscription.getTopic(), MqttTopics.normalizeQos(subscription.getQos()));
            }
        }
    }

    private boolean matchesSubscriptions(List<MqttProperties.Subscription> subscriptions, String topic) {
        if (subscriptions.isEmpty()) {
            return true;
        }
        for (MqttProperties.Subscription subscription : subscriptions) {
            if (subscription != null && MqttTopics.matches(subscription.getTopic(), topic)) {
                return true;
            }
        }
        return false;
    }

    /***
    * 在监听执行器上运行连接通知，避免阻塞 Paho 回调线程。
    * @author wcdk
    * @param task 通知任务
    ***/
    private void notifyAsync(Runnable task) {
        if (!properties.isAsyncListener()) {
            task.run();
            return;
        }
        try {
            messageListenerExecutor.execute(task);
        } catch (RejectedExecutionException ex) {
            // 执行器已关闭等场景回退到当前线程，保证连接事件不丢失
            task.run();
        }
    }

    private void notifyConnected(boolean reconnect) {
        for (MqttConnectionListener listener : connectionListeners) {
            try {
                listener.onConnected(reconnect);
            } catch (Exception ex) {
                log.warn("MqttConnectionListener连接回调执行失败，异常信息：{}", ex.getMessage(), ex);
            }
        }
    }

    private void notifyDisconnected(Throwable cause) {
        for (MqttConnectionListener listener : connectionListeners) {
            try {
                listener.onDisconnected(cause);
            } catch (Exception ex) {
                log.warn("MqttConnectionListener断开回调执行失败，异常信息：{}", ex.getMessage(), ex);
            }
        }
    }

    private Map<MqttMessageListener, List<MqttProperties.Subscription>> subscriptionIndex() {
        Map<MqttMessageListener, List<MqttProperties.Subscription>> index = this.subscriptionIndex;
        if (index != null) {
            return index;
        }
        synchronized (this) {
            if (this.subscriptionIndex == null) {
                Map<MqttMessageListener, List<MqttProperties.Subscription>> built = new LinkedHashMap<>();
                for (MqttMessageListener listener : messageListeners) {
                    List<MqttProperties.Subscription> subscriptions = listener.subscriptions();
                    built.put(listener, subscriptions == null
                            ? Collections.emptyList()
                            : List.copyOf(subscriptions));
                }
                this.subscriptionIndex = Collections.unmodifiableMap(built);
            }
            return this.subscriptionIndex;
        }
    }

    /***
    * 操作超时：completion-timeout 下限 1 秒。
    * @author wcdk
    * @return 超时时长
    ***/
    private Duration timeoutDuration() {
        Duration timeout = properties.getCompletionTimeout();
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            return Duration.ofSeconds(10);
        }
        return Duration.ofMillis(Math.max(1_000L, timeout.toMillis()));
    }

    /***
    * 连接兜底超时：Paho connectionTimeout 为主，外加余量用于回调丢失时兜底。
    * @author wcdk
    * @return 超时时长
    ***/
    private Duration connectTimeoutDuration() {
        Duration timeout = properties.getConnectionTimeout();
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            timeout = Duration.ofSeconds(30);
        }
        return timeout.plusSeconds(10);
    }

    private void configureManualAcks() {
        client.setManualAcks(properties.isManualAck());
    }

    private void configureDisconnectedBuffer() {
        DisconnectedBufferOptions bufferOptions = new DisconnectedBufferOptions();
        bufferOptions.setBufferEnabled(properties.isDisconnectedBufferEnabled());
        bufferOptions.setBufferSize(Math.max(1, properties.getDisconnectedBufferSize()));
        bufferOptions.setDeleteOldestMessages(properties.isDisconnectedBufferDeleteOldest());
        bufferOptions.setPersistBuffer(false);
        client.setBufferOpts(bufferOptions);
    }

    private void acknowledge(MqttMessage message) {
        try {
            client.messageArrivedComplete(message.getId(), message.getQos());
        } catch (MqttException ex) {
            log.warn("自动确认MQTT消息失败，消息ID：{}，异常信息：{}", message.getId(), ex.getMessage(), ex);
        }
    }

    /***
    * 手动 ACK 确认对象：同一消息幂等确认，监听器与 Flux 消费方共享。
    * @author wcdk
    ***/
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
                try {
                    client.messageArrivedComplete(messageId, qos);
                } catch (MqttException ex) {
                    throw new IllegalStateException("手动确认MQTT消息失败，消息ID：" + messageId, ex);
                }
            }
        }

        @Override
        public boolean isAcknowledged() {
            return acknowledged.get();
        }
    }
}
