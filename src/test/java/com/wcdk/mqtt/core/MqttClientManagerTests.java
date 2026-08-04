package com.wcdk.mqtt.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import com.wcdk.mqtt.config.MqttProperties;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.Test;

class MqttClientManagerTests {

    @Test
    void startShouldNotFailWhenBrokerIsUnavailable() throws MqttException {
        MqttAsyncClient client = mock(MqttAsyncClient.class);
        IMqttToken token = mock(IMqttToken.class);
        MqttProperties properties = new MqttProperties();
        properties.setServerUri("tcp://127.0.0.1:1883");

        when(client.isConnected()).thenReturn(false);
        when(client.connect(org.mockito.ArgumentMatchers.any(MqttConnectOptions.class))).thenReturn(token);
        doThrow(new MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT))
                .when(token)
                .waitForCompletion(org.mockito.ArgumentMatchers.anyLong());

        MqttClientManager manager = new MqttClientManager(client, new MqttConnectOptions(), properties, List.of());

        assertThatNoException().isThrownBy(manager::start);
        assertThat(manager.isRunning()).isFalse();
    }
}
