/*
 * Copyright 2022 Apollo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.ctrip.framework.apollo.internals;

import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.dto.ApolloConfigNotification;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;
import com.ctrip.framework.apollo.core.dto.ServiceDTO;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.framework.apollo.core.schedule.SchedulePolicy;
import com.ctrip.framework.apollo.core.signature.Signature;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.ctrip.framework.apollo.exceptions.ApolloConfigException;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.ctrip.framework.apollo.util.http.HttpRequest;
import com.ctrip.framework.apollo.util.http.HttpResponse;
import com.ctrip.framework.apollo.util.http.HttpClient;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class RemoteConfigLongPollService {
  private static final Logger logger = LoggerFactory.getLogger(RemoteConfigLongPollService.class);
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
  private static final Joiner.MapJoiner MAP_JOINER = Joiner.on("&").withKeyValueSeparator("=");
  private static final Escaper queryParamEscaper = UrlEscapers.urlFormParameterEscaper();
  private static final long INIT_NOTIFICATION_ID = ConfigConsts.NOTIFICATION_ID_PLACEHOLDER;
  //90 seconds, should be longer than server side's long polling timeout, which is now 60 seconds
  private static final int LONG_POLLING_READ_TIMEOUT = 90 * 1000;
  private final ExecutorService m_longPollingService; // 长轮询 ExecutorService
  private final AtomicBoolean m_longPollingStopped; // 是否停止长轮询的标识
  private SchedulePolicy m_longPollFailSchedulePolicyInSecond; // 失败定时重试策略，使用 {@link ExponentialSchedulePolicy}
  private RateLimiter m_longPollRateLimiter; // 长轮询的 RateLimiter
  private final AtomicBoolean m_longPollStarted; // 是否长轮询已经开始的标识
  private final Multimap<String, RemoteConfigRepository> m_longPollNamespaces; // 长轮询的 Namespace Multimap 缓存。通过 {@link #submit(String, RemoteConfigRepository)} 添加 RemoteConfigRepository 。KEY：Namespace 的名字，VALUE：RemoteConfigRepository 集合
  private final ConcurrentMap<String, Long> m_notifications; // 通知编号 Map 缓存。KEY：Namespace 的名字。VALUE：最新的通知编号
  private final Map<String, ApolloNotificationMessages> m_remoteNotificationMessages;//namespaceName -> watchedKey -> notificationId 通知消息 Map 缓存。KEY：Namespace 的名字。VALUE：ApolloNotificationMessages 对象
  private Type m_responseType;
  private static final Gson GSON = new Gson();
  private ConfigUtil m_configUtil;
  private HttpClient m_httpClient;
  private ConfigServiceLocator m_serviceLocator;

  /**
   * Constructor.
   */
  public RemoteConfigLongPollService() {
    m_longPollFailSchedulePolicyInSecond = new ExponentialSchedulePolicy(1, 120); //in second
    m_longPollingStopped = new AtomicBoolean(false);
    m_longPollingService = Executors.newSingleThreadExecutor(
        ApolloThreadFactory.create("RemoteConfigLongPollService", true));
    m_longPollStarted = new AtomicBoolean(false);
    m_longPollNamespaces =
        Multimaps.synchronizedSetMultimap(HashMultimap.<String, RemoteConfigRepository>create());
    m_notifications = Maps.newConcurrentMap();
    m_remoteNotificationMessages = Maps.newConcurrentMap();
    m_responseType = new TypeToken<List<ApolloConfigNotification>>() {
    }.getType();
    m_configUtil = ApolloInjector.getInstance(ConfigUtil.class);
    m_httpClient = ApolloInjector.getInstance(HttpClient.class);
    m_serviceLocator = ApolloInjector.getInstance(ConfigServiceLocator.class);
    m_longPollRateLimiter = RateLimiter.create(m_configUtil.getLongPollQPS());
  }
  // 提交 RemoteConfigRepository 到长轮询任务
  public boolean submit(String namespace, RemoteConfigRepository remoteConfigRepository) {
    boolean added = m_longPollNamespaces.put(namespace, remoteConfigRepository); // 添加到 m_longPollNamespaces 中
    m_notifications.putIfAbsent(namespace, INIT_NOTIFICATION_ID); // 添加到 m_notifications 中
    if (!m_longPollStarted.get()) { // 若未启动长轮询定时任务，进行启动
      startLongPolling();
    }
    return added;
  }
  // 启动长轮询任务
  private void startLongPolling() {
    if (!m_longPollStarted.compareAndSet(false, true)) { // CAS 设置长轮询任务已经启动。若已经启动，不重复启动
      //already started
      return;
    }
    try {
      final String appId = m_configUtil.getAppId(); // 获得 appId cluster dataCenter 配置信息
      final String cluster = m_configUtil.getCluster();
      final String dataCenter = m_configUtil.getDataCenter();
      final String secret = m_configUtil.getAccessKeySecret();
      final long longPollingInitialDelayInMills = m_configUtil.getLongPollingInitialDelayInMills(); // 获得长轮询任务的初始化延迟时间，单位毫秒
      m_longPollingService.submit(new Runnable() { // 提交长轮询任务。该任务会持续且循环执行
        @Override
        public void run() {
          if (longPollingInitialDelayInMills > 0) { // 初始等待
            try {
              logger.debug("Long polling will start in {} ms.", longPollingInitialDelayInMills);
              TimeUnit.MILLISECONDS.sleep(longPollingInitialDelayInMills);
            } catch (InterruptedException e) {
              //ignore
            }
          }
          doLongPollingRefresh(appId, cluster, dataCenter, secret); // 执行长轮询
        }
      });
    } catch (Throwable ex) {
      m_longPollStarted.set(false); // 设置 m_longPollStarted 为 false
      ApolloConfigException exception =
          new ApolloConfigException("Schedule long polling refresh failed", ex);
      Tracer.logError(exception);
      logger.warn(ExceptionUtil.getDetailMessage(exception));
    }
  }

  void stopLongPollingRefresh() {
    this.m_longPollingStopped.compareAndSet(false, true);
  }
  // 持续执行长轮询
  private void doLongPollingRefresh(String appId, String cluster, String dataCenter, String secret) {
    final Random random = new Random();
    ServiceDTO lastServiceDto = null;
    while (!m_longPollingStopped.get() && !Thread.currentThread().isInterrupted()) { // 循环执行，直到停止或线程中断
      if (!m_longPollRateLimiter.tryAcquire(5, TimeUnit.SECONDS)) { // 限流
        //wait at most 5 seconds
        try {
          TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
        }
      }
      Transaction transaction = Tracer.newTransaction("Apollo.ConfigService", "pollNotification");
      String url = null;
      try {
        if (lastServiceDto == null) { // 获得 Config Service 的地址
          List<ServiceDTO> configServices = getConfigServices(); // 获得所有的 Config Service 的地址
          lastServiceDto = configServices.get(random.nextInt(configServices.size()));
        }
        // 组装长轮询通知变更的地址
        url =
            assembleLongPollRefreshUrl(lastServiceDto.getHomepageUrl(), appId, cluster, dataCenter,
                m_notifications);

        logger.debug("Long polling from {}", url);
        // 创建 HttpRequest 对象，并设置超时时间
        HttpRequest request = new HttpRequest(url);
        request.setReadTimeout(LONG_POLLING_READ_TIMEOUT);
        if (!StringUtils.isBlank(secret)) {
          Map<String, String> headers = Signature.buildHttpHeaders(url, appId, secret);
          request.setHeaders(headers);
        }

        transaction.addData("Url", url);
        // 发起请求，返回 HttpResponse 对象
        final HttpResponse<List<ApolloConfigNotification>> response =
            m_httpClient.doGet(request, m_responseType);

        logger.debug("Long polling response: {}, url: {}", response.getStatusCode(), url);
        if (response.getStatusCode() == 200 && response.getBody() != null) { // 有新的通知，刷新本地的缓存
          updateNotifications(response.getBody()); // 更新 m_notifications
          updateRemoteNotifications(response.getBody()); // 更新 m_remoteNotificationMessages
          transaction.addData("Result", response.getBody().toString());
          notify(lastServiceDto, response.getBody()); // 通知对应的 RemoteConfigRepository 们
        }

        //try to load balance
        if (response.getStatusCode() == 304 && random.nextBoolean()) { // 无新的通知，重置连接的 Config Service 的地址，下次请求不同的 Config Service ，实现负载均衡
          lastServiceDto = null;
        }

        m_longPollFailSchedulePolicyInSecond.success(); // 标记成功
        transaction.addData("StatusCode", response.getStatusCode());
        transaction.setStatus(Transaction.SUCCESS);
      } catch (Throwable ex) {
        lastServiceDto = null; // 重置连接的 Config Service 的地址，下次请求不同的 Config Service
        Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(ex));
        transaction.setStatus(ex);
        long sleepTimeInSecond = m_longPollFailSchedulePolicyInSecond.fail(); // 标记失败，计算下一次延迟执行时间
        logger.warn(
            "Long polling failed, will retry in {} seconds. appId: {}, cluster: {}, namespaces: {}, long polling url: {}, reason: {}",
            sleepTimeInSecond, appId, cluster, assembleNamespaces(), url, ExceptionUtil.getDetailMessage(ex));
        try {
          TimeUnit.SECONDS.sleep(sleepTimeInSecond); // 等待一定时间，下次失败重试
        } catch (InterruptedException ie) {
          //ignore
        }
      } finally {
        transaction.complete();
      }
    }
  }
  // 通知对应的 RemoteConfigRepository 们
  private void notify(ServiceDTO lastServiceDto, List<ApolloConfigNotification> notifications) {
    if (notifications == null || notifications.isEmpty()) {
      return;
    }
    for (ApolloConfigNotification notification : notifications) {  // 循环 ApolloConfigNotification
      String namespaceName = notification.getNamespaceName();  // Namespace 的名字
      //create a new list to avoid ConcurrentModificationException // 创建 RemoteConfigRepository 数组，避免并发问题
      List<RemoteConfigRepository> toBeNotified =
          Lists.newArrayList(m_longPollNamespaces.get(namespaceName));
      ApolloNotificationMessages originalMessages = m_remoteNotificationMessages.get(namespaceName);
      ApolloNotificationMessages remoteMessages = originalMessages == null ? null : originalMessages.clone(); // 获得远程的 ApolloNotificationMessages 对象，并克隆
      //since .properties are filtered out by default, so we need to check if there is any listener for it
      // 因为 .properties 在默认情况下被过滤掉，所以我们需要检查是否有监听器。若有，添加到 RemoteConfigRepository 数组
      toBeNotified.addAll(m_longPollNamespaces
          .get(String.format("%s.%s", namespaceName, ConfigFileFormat.Properties.getValue())));
      for (RemoteConfigRepository remoteConfigRepository : toBeNotified) { // 循环 RemoteConfigRepository ，进行通知
        try {
          remoteConfigRepository.onLongPollNotified(lastServiceDto, remoteMessages); // 进行通知
        } catch (Throwable ex) {
          Tracer.logError(ex);
        }
      }
    }
  }
  // 更新 m_notifications
  private void updateNotifications(List<ApolloConfigNotification> deltaNotifications) {
    for (ApolloConfigNotification notification : deltaNotifications) { // 循环 ApolloConfigNotification
      if (Strings.isNullOrEmpty(notification.getNamespaceName())) {
        continue;
      }
      String namespaceName = notification.getNamespaceName(); // 更新 m_notifications
      if (m_notifications.containsKey(namespaceName)) {
        m_notifications.put(namespaceName, notification.getNotificationId());
      }
      //since .properties are filtered out by default, so we need to check if there is notification with .properties suffix
      // 因为 .properties 在默认情况下被过滤掉，所以我们需要检查是否有 .properties 后缀的通知。如有，更新 m_notifications
      String namespaceNameWithPropertiesSuffix =
          String.format("%s.%s", namespaceName, ConfigFileFormat.Properties.getValue());
      if (m_notifications.containsKey(namespaceNameWithPropertiesSuffix)) {
        m_notifications.put(namespaceNameWithPropertiesSuffix, notification.getNotificationId());
      }
    }
  }
  // 更新 m_remoteNotificationMessages
  private void updateRemoteNotifications(List<ApolloConfigNotification> deltaNotifications) {
    for (ApolloConfigNotification notification : deltaNotifications) { // 循环 ApolloConfigNotification
      if (Strings.isNullOrEmpty(notification.getNamespaceName())) {
        continue;
      }

      if (notification.getMessages() == null || notification.getMessages().isEmpty()) {
        continue;
      }

      ApolloNotificationMessages localRemoteMessages =
          m_remoteNotificationMessages.get(notification.getNamespaceName());
      if (localRemoteMessages == null) { // 若不存在 Namespace 对应的 ApolloNotificationMessages ，进行创建
        localRemoteMessages = new ApolloNotificationMessages();
        m_remoteNotificationMessages.put(notification.getNamespaceName(), localRemoteMessages);
      }
      // 合并通知消息到 ApolloNotificationMessages 中
      localRemoteMessages.mergeFrom(notification.getMessages());
    }
  }

  private String assembleNamespaces() {
    return STRING_JOINER.join(m_longPollNamespaces.keySet());
  }

  String assembleLongPollRefreshUrl(String uri, String appId, String cluster, String dataCenter,
                                    Map<String, Long> notificationsMap) {
    Map<String, String> queryParams = Maps.newHashMap();
    queryParams.put("appId", queryParamEscaper.escape(appId));
    queryParams.put("cluster", queryParamEscaper.escape(cluster));
    queryParams // notifications
        .put("notifications", queryParamEscaper.escape(assembleNotifications(notificationsMap)));

    if (!Strings.isNullOrEmpty(dataCenter)) { // dataCenter
      queryParams.put("dataCenter", queryParamEscaper.escape(dataCenter));
    }
    String localIp = m_configUtil.getLocalIp();
    if (!Strings.isNullOrEmpty(localIp)) { // ip
      queryParams.put("ip", queryParamEscaper.escape(localIp));
    }

    String params = MAP_JOINER.join(queryParams); // 创建 Query String
    if (!uri.endsWith("/")) {
      uri += "/";
    }

    return uri + "notifications/v2?" + params;  // 拼接 URL
  }

  String assembleNotifications(Map<String, Long> notificationsMap) {
    List<ApolloConfigNotification> notifications = Lists.newArrayList(); // 创建 ApolloConfigNotification 数组
    for (Map.Entry<String, Long> entry : notificationsMap.entrySet()) { // 循环，添加 ApolloConfigNotification 对象
      ApolloConfigNotification notification = new ApolloConfigNotification(entry.getKey(), entry.getValue());
      notifications.add(notification);
    }
    return GSON.toJson(notifications); // JSON 化成字符串
  }

  private List<ServiceDTO> getConfigServices() {
    List<ServiceDTO> services = m_serviceLocator.getConfigServices();
    if (services.size() == 0) {
      throw new ApolloConfigException("No available config service");
    }

    return services;
  }
}
