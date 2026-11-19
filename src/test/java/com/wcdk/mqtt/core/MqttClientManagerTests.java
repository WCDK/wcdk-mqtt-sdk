package com.wcdk.mqtt.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.listener.MqttConnectionListener;
import com.wcdk.mqtt.listener.MqttMessageListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Test;
import org.awaitility.Awaitility;

/***
* 客户端管理器单测：启动容错、连接恢复、消息按监听器订阅过滤、手动 ACK 兜底与动态订阅恢复。
* @author wcdk
***/
class MqttClientManagerTests {

    @Test
    void startShouldNotFailWhenBrokerIsUnavailable() throws MqttException {
        MqttAsyncClient client = mock(MqttAsyncClient.class);
        IMqttToken token = mock(IMqttToken.class);
        MqttProperties properties = new MqttProperties();
        properties.setServerUri("tcp://127.0.0.1:1883");

        when(client.isConnected()).thenReturn(false);
        when(client.connect(any(MqttConnectOptions.class))).thenReturn(token);
        doThrow(new MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT))
                .when(token)
                .waitForCompletion(anyLong());

        MqttClientManager manager = new MqttClientManager(client, new MqttConnectOptions(), properties, List.of());

        assertThatNoException().isThrownBy(manager::start);
        // 启动连接失败不等于生命周期结束，后台仍按 connect-retry-interval 重试
        assertThat(manager.isRunning()).isTrue();
        assertThat(manager.isConnected()).isFalse();
    }

    @Test
    void connectSuccessShouldMarkManagerConnected() throws MqttException {
        MqttAsyncClient client = mock(MqttAsyncClient.class);
        IMqttToken token = mock(IMqttToken.class);
        MqttProperties properties = new MqttProperties();
        properties.setConnectRetryInterval(java.time.Duration.ZERO);

        java.util.concurrent.atomic.AtomicBoolean link = new java.util.concurrent.atomic.AtomicBoolean(false);
        when(client.isConnected()).thenAnswer(invocation -> link.get());
        when(client.connect(any(MqttConnectOptions.class))).thenReturn(token);
        org.mockito.Mockito.doAnswer(invocation -> {
            link.set(true);
            return null;
        }).when(token).waitForCompletion(anyLong());

        MqttClientManager manager = new MqttClientManager(client, new MqttConnectOptions(), properties, List.of());
        manager.start();

        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(manager::isConnected);
        assertThat(manager.isRunning()).isTrue();
    }

    @Test
    void messageArrivedShouldOnlyDispatchToMatchingListeners() throws MqttException {
        MqttAsyncClient client = mock(MqttAsyncClient.class);
        MqttProperties properties = new MqttProperties();
        properties.setAsyncListener(false);

        RecordingListener deviceListener = new RecordingListener("device/+/status");
        RecordingListener noticeListener = new RecordingListener("system/#");

        MqttClientManager manager = new MqttClientManager(client, new MqttConnectOptions(), properties,
                List.of(deviceListener, noticeListener));

        manager.messageArrived("device/a/status", new MqttMessage("online".getBytes(StandardCharsets.UTF_8)));

        assertThat(deviceListener.topics).containsExactly("device/a/status");
        assertThat(noticeListener.topics).isEmpty();
    }

    @Test
    void manualAckShouldAutoAckMessageWithoutAnyListener() throws MqttException {
        MqttAsyncClient client = mock(MqttAsyncClient.class);
        MqttProperties properties = new MqttProperties();
        properties.setManualAck(true);
        properties.setAsyncListener(false);

        RecordingListener listener = new RecordingListener("only/this/topic");
        MqttClientManager manager = new MqttClientManager(client, new MqttConnectOptions(), properties,
                List.of(listener));

        MqttMessage message = new MqttMessage("ignored".getBytes(StandardCharsets.UTF_8));
        message.setQos(1);
        manager.messageArrived("other/topic", message);

        assertThat(listener.topics).isEmpty();
        verify(client).messageArrivedComplete(message.getId(), 1);
    }

    @Test
    void manualAckShouldAutoCommitListenerThatIgnoresAcknowledgement() throws MqttException {
        MqttAsyncClient client = mock(MqttAsyncClient.class);
        MqttProperties properties = new MqttProperties();
        properties.setManualAck(true);
        properties.setAsyncListener(false);

        AllMessagesListener listener = new AllMessagesListener();
        MqttClientManager manager = new MqttClientManager(client, new MqttConnectOptions(), properties,
                List.of(listener));

        MqttMessage message = new MqttMessage("payload".getBytes(StandardCharsets.UTF_8));
        message.setQos(1);
        manager.messageArrived("some/topic", message);

        // 未覆盖三参方法的监听器走接口默认实现：委托两参方法后由 SDK 兜底确认
        assertThat(listener.lastTopic).isEqualTo("some/topic");
        verify(client).messageArrivedComplete(message.getId(), 1);
    }

    @Test
    void dynamicSubscriptionShouldBeRestoredAfterReconnect() throws MqttException {
        MqttAsyncClient client = mock(MqttAsyncClient.class);
        IMqttToken token = mock(IMqttToken.class);
        MqttProperties properties = new MqttProperties();
        properties.setConnectRetryInterval(java.time.Duration.ZERO);

        when(client.isConnected()).thenReturn(true);
        when(client.connect(any(MqttConnectOptions.class))).thenReturn(token);
        when(client.subscribe(anyString(), anyInt())).thenReturn(token);
        when(client.subscribe(any(String[].class), any(int[].class))).thenReturn(token);

        MqttClientManager manager = new MqttClientManager(client, new MqttConnectOptions(), properties, List.of());
        manager.subscribe("dynamic/+/topic", 2);

        verify(client).subscribe("dynamic/+/topic", 2);

        manager.connectComplete(true, "tcp://127.0.0.1:1883");

        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                verify(client).subscribe(any(String[].class), any(int[].class)));
        verify(client, never()).unsubscribe("dynamic/+/topic");
    }

    @Test
    void connectionListenersShouldBeNotifiedOnConnectAndLost() throws MqttException {
        MqttAsyncClient client = mock(MqttAsyncClient.class);
        MqttProperties properties = new MqttProperties();
        properties.setConnectRetryInterval(java.time.Duration.ZERO);

        ConnectionRecorder recorder = new ConnectionRecorder();
        MqttClientManager manager = new MqttClientManager(client, new MqttConnectOptions(), properties,
                List.of(), Runnable::run, List.of(recorder));

        manager.connectComplete(true, "tcp://127.0.0.1:1883");
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> recorder.connected >= 1);
        assertThat(recorder.lastReconnect).isTrue();

        manager.connectionLost(new IllegalStateException("network down"));
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> recorder.disconnected >= 1);
        assertThat(recorder.lastCause).isInstanceOf(IllegalStateException.class);

        manager.stop();
    }

    private static final class RecordingListener implements MqttMessageListener {

        private final List<MqttProperties.Subscription> declared;

        private final java.util.List<String> topics = new java.util.concurrent.CopyOnWriteArrayList<>();

        private RecordingListener(String topicFilter) {
            this.declared = List.of(new MqttProperties.Subscription(topicFilter, 1));
        }

        @Override
        public List<MqttProperties.Subscription> subscriptions() {
            return declared;
        }

        @Override
        public void onMessage(String topic, MqttMessage message) {
            topics.add(topic);
        }
    }

    private static final class AllMessagesListener implements MqttMessageListener {

        private volatile String lastTopic;

        @Override
        public void onMessage(String topic, MqttMessage message) {
            this.lastTopic = topic;
        }
    }

    private static final class ConnectionRecorder implements MqttConnectionListener {

        private volatile int connected;

        private volatile int disconnected;

        private volatile boolean lastReconnect;

        private volatile Throwable lastCause;

        @Override
        public void onConnected(boolean reconnect) {
            connected++;
            lastReconnect = reconnect;
        }

        @Override
        public void onDisconnected(Throwable cause) {
            disconnected++;
            lastCause = cause;
        }
    }
}
