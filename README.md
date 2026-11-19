# wcdk-mqtt-sdk 使用说明

`wcdk-mqtt-sdk` 是基于 Spring Boot 3 和 Eclipse Paho 的 MQTT 客户端封装，提供连接管理、自动重连、消息发布、主题订阅、注解监听、接口监听和手动 ACK。

## 1. 环境要求

- Java 17 或更高版本
- Spring Boot 3.x
- MQTT 3.1/3.1.1 Broker

当前模块 `pom.xml` 的 Maven 坐标为：

```xml
<dependency>
    <groupId>com.wcdk.mqtt</groupId>
    <artifactId>wcdk-mqtt-sdk</artifactId>
    <version>1.0.0</version>
</dependency>
```

## 2. 启用 SDK

SDK 已注册 Spring Boot 自动配置（`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`），第三方应用**只需引入依赖**，无需 `@Import`、无需扫描 SDK 包：

```java
package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
```

自动装配会创建 `MqttConnectOptions`、`MqttAsyncClient`、监听线程池、`MqttListenerRegistrar`、`MqttClientManager` 与 `MqttUtil`，每个 Bean 均可用自定义 Bean 覆盖（`@ConditionalOnMissingBean`）。旧版本的 `@Import(MqttClientConfiguration.class)` 显式启用方式继续有效。设置 `wcdk.mqtt.enabled=false` 可关闭全部 Bean。

## 3. 配置连接

最小配置：

```yaml
wcdk:
  mqtt:
    server-uri: tcp://127.0.0.1:1883
    client-id: order-service-01
    username: mqtt-user
    password: mqtt-password
```

完整配置示例：

```yaml
wcdk:
  mqtt:
    enabled: true

    # 单地址配置；server-uris 非空时优先使用 server-uris
    server-uri: tcp://127.0.0.1:1883

    # 多地址故障转移，按顺序尝试
    server-uris:
      - tcp://mqtt-1.example.com:1883
      - tcp://mqtt-2.example.com:1883

    # 为空时自动生成 wcdk-mqtt-server-{UUID}
    client-id: order-service-01
    username: mqtt-user
    password: mqtt-password

    clean-session: true
    automatic-reconnect: true
    # Paho 自动重连指数退避上限
    max-reconnect-delay: 128s
    # 首次连接失败后的重试间隔，0 表示不重试
    connect-retry-interval: 10s
    # MQTT 协议版本：0=自动协商，3=MQTT 3.1，4=MQTT 3.1.1
    mqtt-version: 0
    manual-ack: false
    connection-timeout: 30s
    keep-alive-interval: 60s
    completion-timeout: 10s
    max-inflight: 1000

    # 遗嘱消息：客户端异常掉线（无 DISCONNECT）时由 Broker 发布
    will-topic: device/1/lwt
    will-payload: offline
    will-qos: 1
    will-retained: false

    # 未连接时缓存待发送消息
    disconnected-buffer-enabled: false
    disconnected-buffer-size: 10000
    disconnected-buffer-delete-oldest: false

    # publish() 是否异步发送
    async-publish: true
    default-qos: 1
    default-retained: false

    # 消息监听器是否在线程池执行
    async-listener: true
    listener-core-pool-size: 4
    listener-max-pool-size: 16
    listener-queue-capacity: 10000

    # 应用启动或重连成功后自动订阅
    subscriptions:
      - topic: device/+/status
        qos: 1
      - topic: system/notice
        qos: 0
```

### 配置项

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `enabled` | `true` | 是否创建 MQTT 客户端相关 Bean |
| `server-uri` | `tcp://127.0.0.1:1883` | 单个 Broker 地址 |
| `server-uris` | 空 | Broker 地址列表；非空时优先于 `server-uri` |
| `client-id` | 自动生成 | MQTT 客户端 ID，生产环境建议显式设置且保证实例间唯一 |
| `username` / `password` | 空 | Broker 认证信息 |
| `clean-session` | `true` | 是否使用清洁会话 |
| `automatic-reconnect` | `true` | 断线后是否由 Paho 自动重连 |
| `max-reconnect-delay` | `128s` | Paho 自动重连指数退避的最大间隔 |
| `connect-retry-interval` | `10s` | 首次连接失败后的后台重试间隔；`0` 或负数表示不重试 |
| `mqtt-version` | `0` | MQTT 协议版本：`0`=自动协商，`3`=3.1，`4`=3.1.1 |
| `manual-ack` | `false` | 是否手动确认收到的消息 |
| `connection-timeout` | `30s` | 连接超时 |
| `keep-alive-interval` | `60s` | 心跳间隔 |
| `completion-timeout` | `10s` | 同步发布、订阅、取消订阅等操作的等待超时 |
| `max-inflight` | `1000` | 同时处于发送中的最大消息数，最小为 1 |
| `will-topic` | 空 | 遗嘱主题，配置后客户端异常掉线时由 Broker 发布遗嘱消息 |
| `will-payload` | 空 | 遗嘱消息正文（UTF-8） |
| `will-qos` | `1` | 遗嘱 QoS |
| `will-retained` | `false` | 遗嘱是否为保留消息 |
| `disconnected-buffer-enabled` | `false` | 未连接时是否缓存待发送消息 |
| `disconnected-buffer-size` | `10000` | 缓存队列容量，最小为 1 |
| `disconnected-buffer-delete-oldest` | `false` | 缓存队列满时是否删除最早的消息 |
| `async-publish` | `true` | `publish` 默认异步还是同步等待 |
| `default-qos` | `1` | `MqttUtil` 简化方法使用的默认 QoS |
| `default-retained` | `false` | `MqttUtil` 简化方法使用的默认 retained 标志 |
| `async-listener` | `true` | 是否异步执行消息监听器 |
| `listener-core-pool-size` | `4` | 监听线程池核心线程数 |
| `listener-max-pool-size` | `16` | 监听线程池最大线程数 |
| `listener-queue-capacity` | `10000` | 监听线程池队列容量 |
| `subscriptions` | 空 | 连接和重连后自动订阅的主题列表 |

QoS 只能设置为 `0`、`1` 或 `2`。

## 4. 发布消息

推荐注入 `MqttUtil`：

```java
package com.example.demo.service;

import com.wcdk.mqtt.util.MqttUtil;
import org.springframework.stereotype.Service;

@Service
public class DeviceCommandService {

    private final MqttUtil mqttUtil;

    public DeviceCommandService(MqttUtil mqttUtil) {
        this.mqttUtil = mqttUtil;
    }

    public void sendCommand(String deviceId, String json) {
        // 使用 default-qos 和 default-retained
        mqttUtil.publish("device/" + deviceId + "/command", json);

        // 指定 QoS 和 retained
        mqttUtil.publish("device/" + deviceId + "/command", json, 1, false);

        // 发布二进制数据
        mqttUtil.publishBytes("device/" + deviceId + "/firmware", new byte[]{1, 2, 3}, 2, false);
    }
}
```

发送方式：

- `publish(...)` / `publishBytes(...)`：由 `async-publish` 决定异步发送或同步等待。
- `publishAndWait(...)` / `publishBytesAndWait(...)`：始终等待发送完成，超时由 `completion-timeout` 控制；失败时抛出 `IllegalStateException`。
- 发布时若客户端尚未连接，SDK 会先尝试建立连接。
- `null` 文本或字节数组会以空消息体发送。

## 5. 使用注解接收消息

在 Spring Bean 的方法上使用 `@MqttListener`。SDK 会自动订阅注解声明的主题，并在重连后重新订阅。

```java
package com.example.demo.listener;

import com.wcdk.mqtt.core.MqttListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.stereotype.Component;

@Component
public class DeviceMessageListener {

    // 只有一个 String 参数时，该参数是 UTF-8 消息正文
    @MqttListener("device/+/status")
    public void onStatus(String payload) {
        System.out.println("payload=" + payload);
    }

    // 有两个 String 参数时，第一个是实际主题，第二个是 UTF-8 正文
    @MqttListener(topics = {"device/+/event", "gateway/+/event"}, qos = 1)
    public void onEvent(String topic, String payload) {
        System.out.println("topic=" + topic + ", payload=" + payload);
    }

    // 可直接取得原始 MQTT 消息
    @MqttListener(value = "binary/#", qos = 2)
    public void onBinary(byte[] payload, MqttMessage message) {
        System.out.println("bytes=" + payload.length + ", retained=" + message.isRetained());
    }
}
```

注解支持 MQTT 通配符 `+` 和 `#`。监听方法必须返回 `void`，参数只能使用以下类型：

- `String`：一个 `String` 参数表示 UTF-8 正文；两个及以上时，第一个表示主题，其余表示正文。
- `byte[]`：原始消息体。
- `MqttMessage`：Paho 原始消息，可读取 QoS、retained、消息 ID 等。
- `MqttAcknowledgement`：手动 ACK 对象，仅在 `manual-ack: true` 时有效。

## 6. 手动 ACK

开启手动 ACK：

```yaml
wcdk:
  mqtt:
    manual-ack: true
```

处理成功后调用 `commit()`：

```java
import com.wcdk.mqtt.core.MqttListener;
import com.wcdk.mqtt.listener.MqttAcknowledgement;
import org.springframework.stereotype.Component;

@Component
public class PaymentListener {

    @MqttListener(value = "payment/result", qos = 1)
    public void onPayment(String payload, MqttAcknowledgement acknowledgement) {
        // 先完成业务处理
        savePaymentResult(payload);

        // commit() 可重复调用，但只会实际确认一次
        acknowledgement.commit();
    }

    private void savePaymentResult(String payload) {
        // 业务逻辑
    }
}
```

### 手动 ACK 确认规则（重要）

同一条消息只生成**一个** `MqttAcknowledgement`，多个监听器共享，任一 `commit()` 即完成确认（`commit()` 幂等，重复调用只确认一次）。确认时机按以下规则兜底，避免在途窗口被未确认消息占满：

| 场景 | 行为 |
| --- | --- |
| 注解方法声明了 `MqttAcknowledgement` 参数 | 由该方法自行决定确认时机，SDK 不兜底 |
| 注解方法均未声明该参数 | 方法执行结束（含异常）后由 SDK 自动 `commit()` |
| 接口监听器覆盖了三参 `onMessage` | 由实现自行决定确认时机 |
| 接口监听器只实现两参 `onMessage` | 默认实现委托两参方法，并在 `finally` 中自动 `commit()` |
| 消息未被任何监听器匹配 | 分发前立即自动确认 |

不开启手动 ACK 时，注解参数中的 `MqttAcknowledgement` 为 `null`，因此不要在自动 ACK 模式下调用它。

## 7. 实现接口接收消息

除注解外，也可以注册 `MqttMessageListener` Bean：

```java
package com.example.demo.listener;

import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.listener.MqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
public class RawMessageListener implements MqttMessageListener {

    @Override
    public List<MqttProperties.Subscription> subscriptions() {
        MqttProperties.Subscription subscription = new MqttProperties.Subscription();
        subscription.setTopic("raw/#");
        subscription.setQos(1);
        return List.of(subscription);
    }

    @Override
    public void onMessage(String topic, MqttMessage message) {
        String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
        System.out.println(topic + " -> " + payload);
    }
}
```

若启用 `manual-ack`，接口实现应改为覆盖三参数方法：

```java
@Override
public void onMessage(String topic,
                      MqttMessage message,
                      MqttAcknowledgement acknowledgement) {
    // 处理消息
    acknowledgement.commit();
}
```

> `subscriptions()` 非空时，SDK 会按声明的主题过滤器（支持 `+` / `#`）**只把命中的消息分发给该监听器**；返回空列表表示不产生自动订阅，同时接收客户端收到的全部消息。多个接口监听器互不干扰，无需自行判断 `topic`。

## 8. 动态订阅与连接状态

```java
import com.wcdk.mqtt.util.MqttUtil;
import org.springframework.stereotype.Service;

@Service
public class DynamicSubscriptionService {

    private final MqttUtil mqttUtil;

    public DynamicSubscriptionService(MqttUtil mqttUtil) {
        this.mqttUtil = mqttUtil;
    }

    public void subscribe() {
        mqttUtil.subscribe("temporary/+/event", 1);
    }

    public void unsubscribe() {
        mqttUtil.unsubscribe("temporary/+/event");
    }

    public boolean connected() {
        return mqttUtil.isConnected();
    }
}
```

注意：

- `subscribe` 会在未连接时先尝试连接。
- `unsubscribe` 在未连接时直接返回。
- 通过配置、注解或监听器声明的主题会在重连后自动订阅。
- 通过 `mqttUtil.subscribe(...)` 创建的**动态订阅会被 SDK 登记持久化**，断线重连后自动重新订阅；`unsubscribe` 会取消登记。

### 连接状态回调

注册 `MqttConnectionListener` Bean 即可被 SDK 自动收集，也可通过 `MqttUtil.addConnectionListener(...)` 编程式注册：

```java
import com.wcdk.mqtt.listener.MqttConnectionListener;
import org.springframework.stereotype.Component;

@Component
public class ConnectionStateLogger implements MqttConnectionListener {

    @Override
    public void onConnected(boolean reconnect) {
        // reconnect=true 表示断线后的自动重连
    }

    @Override
    public void onDisconnected(Throwable cause) {
        // 主动正常断开时 cause 可能为 null
    }
}
```

回调在 SDK 内部线程执行，不要长时间阻塞。连接成功后可通过 `mqttUtil.getServerUri()` 获取实际连接成功的 Broker 地址（多地址故障转移后为命中的地址）。

## 9. 常见问题

### 注入不到 `MqttUtil`

确认应用已添加 `wcdk-mqtt-sdk` 依赖且 `wcdk.mqtt.enabled` 没有设为 `false`。SDK 走 Spring Boot 自动配置，无需 `@Import`；若曾显式导入 `MqttClientConfiguration` 也可以继续保留。

### 应用启动时 Broker 不可用

SDK 的启动连接在后台执行。首次连接失败会记录警告日志，不会阻止 Spring Boot 应用启动，之后按 `connect-retry-interval` 反复重试；连接成功后自动补齐配置订阅、监听器订阅与动态订阅。`automatic-reconnect: true` 时断线由 Paho 自动重连。

### 收不到注解消息

检查以下内容：

1. 注解所在类是否为 Spring Bean，例如标注了 `@Component`。
2. 监听方法是否返回 `void`，参数类型是否受支持。
3. 主题过滤器和 QoS 是否正确。
4. `MqttUtil.isConnected()` 是否为 `true`。
5. Broker ACL 是否允许当前用户订阅该主题。

### 客户端频繁互相挤下线

多个应用实例使用了相同 `client-id`。每个同时在线的客户端应使用唯一 ID，可结合应用名、主机名或实例编号生成。

## 10. 主要 API

| 类 | 用途 |
| --- | --- |
| `MqttUtil` | 常用发布、订阅、取消订阅、连接状态与连接回调 |
| `MqttClientManager` | 底层连接生命周期及完整发布/订阅能力 |
| `@MqttListener` | 声明式主题监听 |
| `MqttMessageListener` | 编程式消息监听接口（按 `subscriptions()` 过滤） |
| `MqttConnectionListener` | 连接建立/断开状态回调 |
| `MqttAcknowledgement` | 手动确认消息 |
| `MqttProperties` | `wcdk.mqtt` 配置映射 |
| `MqttClientConfiguration` | SDK 自动装配入口（无需显式导入） |


## 贡献

欢迎提交 Issue 和 Pull Request！

## 联系方式

- 作者：WCDK
- 邮箱：wcdk1024@gmail.com