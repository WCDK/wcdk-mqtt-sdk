package com.wcdk.mqtt.core;

import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.listener.MqttAcknowledgement;
import com.wcdk.mqtt.listener.MqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.lang.NonNull;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/***
* @MqttListener 注解方法注册器：扫描 Spring Bean 中的监听方法并登记，
* 同时作为按注解主题过滤的 MqttMessageListener 参与消息分发与手动 ACK 兜底确认。
* @author wcdk
**/
public class MqttListenerRegistrar implements BeanPostProcessor, MqttMessageListener {

    private static final Logger log = LoggerFactory.getLogger(MqttListenerRegistrar.class);

    private final List<MethodHandler> handlers = new CopyOnWriteArrayList<>();

    @Override
    public Object postProcessAfterInitialization(Object bean, @NonNull String beanName) throws BeansException {
        if (bean == this) {
            return bean;
        }
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        for (Method method : getCandidateMethods(targetClass)) {
            MqttListener listener = AnnotationUtils.findAnnotation(method, MqttListener.class);
            if (listener != null) {
                registerListener(bean, beanName, method, listener);
            }
        }
        return bean;
    }

    @Override
    public List<MqttProperties.Subscription> subscriptions() {
        if (handlers.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, MqttProperties.Subscription> subscriptions = new LinkedHashMap<>();
        for (MethodHandler handler : handlers) {
            for (String topic : handler.topics) {
                MqttProperties.Subscription subscription = subscriptions.get(topic);
                if (subscription == null) {
                    // 新增主题按监听方法声明的 QoS 订阅，避免被 Subscription 默认值 1 抬高
                    subscription = new MqttProperties.Subscription(topic, handler.qos);
                    subscriptions.put(topic, subscription);
                } else {
                    subscription.setQos(Math.max(subscription.getQos(), handler.qos));
                }
            }
        }
        return new ArrayList<>(subscriptions.values());
    }

    @Override
    public void onMessage(String topic, MqttMessage message) throws Exception {
        dispatch(topic, message, null);
    }

    @Override
    public void onMessage(String topic, MqttMessage message, MqttAcknowledgement acknowledgement) throws Exception {
        dispatch(topic, message, acknowledgement);
    }

    private List<Method> getCandidateMethods(Class<?> targetClass) {
        List<Method> methods = new ArrayList<>();
        Class<?> currentClass = targetClass;
        while (currentClass != null && !Object.class.equals(currentClass)) {
            for (Method method : currentClass.getDeclaredMethods()) {
                if (!method.isBridge() && !method.isSynthetic()) {
                    methods.add(method);
                }
            }
            currentClass = currentClass.getSuperclass();
        }
        return methods;
    }

    private void registerListener(Object bean, String beanName, Method method, MqttListener listener) {
        List<String> topics = resolveTopics(listener);
        if (topics.isEmpty()) {
            log.error("MQTT监听方法主题不能为空，方法：{}", method.getName());
            return;
        }
        if (!validateMethod(method)) {
            return;
        }
        ReflectionUtils.makeAccessible(method);
        MethodHandler handler = new MethodHandler(bean, method, topics, MqttTopics.normalizeQos(listener.qos()),
                hasAcknowledgementParameter(method));
        handlers.add(handler);
        log.info("MQTT监听方法注册成功，Bean名称：{}，方法：{}，主题：{}，QoS：{}",
                beanName, method.getName(), topics, handler.qos);
    }

    private static boolean hasAcknowledgementParameter(Method method) {
        for (Parameter parameter : method.getParameters()) {
            if (MqttAcknowledgement.class.isAssignableFrom(parameter.getType())) {
                return true;
            }
        }
        return false;
    }

    private boolean validateMethod(Method method) {
        if (!void.class.equals(method.getReturnType())) {
            log.error("MQTT监听方法返回值必须为void，方法：{}", method.getName());
            return false;
        }
        for (Parameter parameter : method.getParameters()) {
            Class<?> parameterType = parameter.getType();
            if (String.class == parameterType
                    || byte[].class == parameterType
                    || MqttMessage.class.isAssignableFrom(parameterType)
                    || MqttAcknowledgement.class.isAssignableFrom(parameterType)) {
                continue;
            }
            log.error("MQTT监听方法存在不支持的参数类型，方法：{}，参数类型：{}",
                    method.getName(), parameterType.getName());
            return false;
        }
        return true;
    }

    private void dispatch(String topic, MqttMessage message, MqttAcknowledgement acknowledgement) throws Exception {
        boolean ackRequested = false;
        List<MethodHandler> matched = new ArrayList<>();
        for (MethodHandler handler : handlers) {
            if (handler.matches(topic)) {
                matched.add(handler);
                ackRequested = ackRequested || handler.acceptsAcknowledgement;
            }
        }
        try {
            for (MethodHandler handler : matched) {
                invokeListenerMethod(handler, topic, message, acknowledgement);
            }
        } finally {
            // 没有注解方法接收 ACK 入参时由 SDK 兜底确认，避免在途窗口被未确认消息占满
            if (acknowledgement != null && !ackRequested) {
                acknowledgement.commit();
            }
        }
    }

    private void invokeListenerMethod(MethodHandler handler,
                                      String topic,
                                      MqttMessage message,
                                      MqttAcknowledgement acknowledgement) throws Exception {
        try {
            log.debug("MQTT监听方法收到消息，主题：{}，QoS：{}，是否保留：{}",
                    topic, message == null ? null : message.getQos(), message != null && message.isRetained());
            handler.method.invoke(handler.bean, resolveArguments(handler.method, topic, message, acknowledgement));
        } catch (InvocationTargetException ex) {
            Throwable targetException = ex.getTargetException();
            if (targetException instanceof Exception exception) {
                throw exception;
            }
            throw new IllegalStateException("调用MQTT监听方法失败，方法：" + handler.method.getName(), targetException);
        }
    }

    private Object[] resolveArguments(Method method,
                                      String topic,
                                      MqttMessage message,
                                      MqttAcknowledgement acknowledgement) {
        Parameter[] parameters = method.getParameters();
        Object[] args = new Object[parameters.length];
        byte[] payloadBytes = message == null || message.getPayload() == null ? new byte[0] : message.getPayload();
        String payload = new String(payloadBytes, StandardCharsets.UTF_8);
        int stringIndex = 0;
        boolean multipleStringParameters = hasMultipleStringParameters(parameters);
        for (int i = 0; i < parameters.length; i++) {
            Class<?> parameterType = parameters[i].getType();
            if (MqttMessage.class.isAssignableFrom(parameterType)) {
                args[i] = message;
            } else if (MqttAcknowledgement.class.isAssignableFrom(parameterType)) {
                args[i] = acknowledgement;
            } else if (byte[].class == parameterType) {
                args[i] = payloadBytes;
            } else if (String.class == parameterType) {
                args[i] = stringIndex++ == 0 && multipleStringParameters ? topic : payload;
            }
        }
        return args;
    }

    private boolean hasMultipleStringParameters(Parameter[] parameters) {
        int count = 0;
        for (Parameter parameter : parameters) {
            if (String.class == parameter.getType()) {
                count++;
            }
        }
        return count > 1;
    }

    private List<String> resolveTopics(MqttListener listener) {
        List<String> topics = new ArrayList<>();
        addTopics(topics, listener.value());
        addTopics(topics, listener.topics());
        return topics;
    }

    private void addTopics(List<String> topics, String[] values) {
        if (values == null) {
            return;
        }
        Arrays.stream(values)
                .filter(StringUtils::hasText)
                .map(String::trim)
                .filter(topic -> !topics.contains(topic))
                .forEach(topics::add);
    }

    private int normalizeQos(int qos) {
        return MqttTopics.normalizeQos(qos);
    }

    private static final class MethodHandler {

        private final Object bean;

        private final Method method;

        private final List<String> topics;

        private final int qos;

        private final boolean acceptsAcknowledgement;

        private MethodHandler(Object bean, Method method, List<String> topics, int qos,
                              boolean acceptsAcknowledgement) {
            this.bean = bean;
            this.method = method;
            this.topics = topics;
            this.qos = qos;
            this.acceptsAcknowledgement = acceptsAcknowledgement;
        }

        private boolean matches(String topic) {
            for (String topicFilter : topics) {
                if (MqttTopics.matches(topicFilter, topic)) {
                    return true;
                }
            }
            return false;
        }
    }
}
