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

import com.ctrip.framework.apollo.Apollo;
import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.dto.ApolloConfig;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;
import com.ctrip.framework.apollo.core.dto.ServiceDTO;
import com.ctrip.framework.apollo.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.framework.apollo.core.schedule.SchedulePolicy;
import com.ctrip.framework.apollo.core.signature.Signature;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.core.utils.DeferredLoggerFactory;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.ctrip.framework.apollo.enums.ConfigSourceType;
import com.ctrip.framework.apollo.exceptions.ApolloConfigException;
import com.ctrip.framework.apollo.exceptions.ApolloConfigStatusCodeException;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.ctrip.framework.apollo.util.http.HttpRequest;
import com.ctrip.framework.apollo.util.http.HttpResponse;
import com.ctrip.framework.apollo.util.http.HttpClient;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class RemoteConfigRepository extends AbstractConfigRepository {
  private static final Logger logger = DeferredLoggerFactory.getLogger(RemoteConfigRepository.class);
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
  private static final Joiner.MapJoiner MAP_JOINER = Joiner.on("&").withKeyValueSeparator("=");
  private static final Escaper pathEscaper = UrlEscapers.urlPathSegmentEscaper();
  private static final Escaper queryParamEscaper = UrlEscapers.urlFormParameterEscaper();

  private final ConfigServiceLocator m_serviceLocator;
  private final HttpClient m_httpClient;
  private final ConfigUtil m_configUtil;
  private final RemoteConfigLongPollService remoteConfigLongPollService; // 远程配置长轮询服务
  private volatile AtomicReference<ApolloConfig> m_configCache; // 指向 ApolloConfig 的 AtomicReference ，缓存配置
  private final String m_namespace; // Namespace 名字
  private final static ScheduledExecutorService m_executorService; // ScheduledExecutorService 对象
  private final AtomicReference<ServiceDTO> m_longPollServiceDto; //  指向 ServiceDTO( Config Service 信息) 的 AtomicReference
  private final AtomicReference<ApolloNotificationMessages> m_remoteMessages; // ApolloNotificationMessages 的 AtomicReference
  private final RateLimiter m_loadConfigRateLimiter; // 加载配置的 RateLimiter
  private final AtomicBoolean m_configNeedForceRefresh; // 是否强制拉取缓存的标记。若为 true ，则多一轮从 Config Service 拉取配置。为 true 的原因，RemoteConfigRepository 知道 Config Service 有配置刷新
  private final SchedulePolicy m_loadConfigFailSchedulePolicy; // 失败定时重试策略，使用 {@link ExponentialSchedulePolicy}
  private static final Gson GSON = new Gson();

  static {
    m_executorService = Executors.newScheduledThreadPool(1, // 单线程池
        ApolloThreadFactory.create("RemoteConfigRepository", true));
  }

  /**
   * Constructor.
   *
   * @param namespace the namespace
   */
  public RemoteConfigRepository(String namespace) {
    m_namespace = namespace;
    m_configCache = new AtomicReference<>();
    m_configUtil = ApolloInjector.getInstance(ConfigUtil.class);
    m_httpClient = ApolloInjector.getInstance(HttpClient.class);
    m_serviceLocator = ApolloInjector.getInstance(ConfigServiceLocator.class);
    remoteConfigLongPollService = ApolloInjector.getInstance(RemoteConfigLongPollService.class);
    m_longPollServiceDto = new AtomicReference<>();
    m_remoteMessages = new AtomicReference<>();
    m_loadConfigRateLimiter = RateLimiter.create(m_configUtil.getLoadConfigQPS());
    m_configNeedForceRefresh = new AtomicBoolean(true);
    m_loadConfigFailSchedulePolicy = new ExponentialSchedulePolicy(m_configUtil.getOnErrorRetryInterval(),
        m_configUtil.getOnErrorRetryInterval() * 8);
    this.trySync(); // 尝试同步配置
    this.schedulePeriodicRefresh();  // 初始化定时刷新配置的任务
    this.scheduleLongPollingRefresh(); // 注册自己到 RemoteConfigLongPollService 中，实现配置更新的实时通知
  }

  @Override
  public Properties getConfig() {
    if (m_configCache.get() == null) { // 如果缓存为空，强制从 Config Service 拉取配置
      this.sync();
    }
    return transformApolloConfigToProperties(m_configCache.get()); // 转换成 Properties 对象，并返回
  }

  @Override
  public void setUpstreamRepository(ConfigRepository upstreamConfigRepository) {
    //remote config doesn't need upstream
  }

  @Override
  public ConfigSourceType getSourceType() {
    return ConfigSourceType.REMOTE;
  }
  // 初始化定时刷新配置的任务
  private void schedulePeriodicRefresh() {
    logger.debug("Schedule periodic refresh with interval: {} {}",
        m_configUtil.getRefreshInterval(), m_configUtil.getRefreshIntervalTimeUnit());
    m_executorService.scheduleAtFixedRate( // 创建定时任务，定时刷新配置
        new Runnable() {
          @Override
          public void run() {
            Tracer.logEvent("Apollo.ConfigService", String.format("periodicRefresh: %s", m_namespace));
            logger.debug("refresh config for namespace: {}", m_namespace);
            trySync(); // 尝试同步配置
            Tracer.logEvent("Apollo.Client.Version", Apollo.VERSION);
          }
        }, m_configUtil.getRefreshInterval(), m_configUtil.getRefreshInterval(),
        m_configUtil.getRefreshIntervalTimeUnit());
  }

  @Override
  protected synchronized void sync() {
    Transaction transaction = Tracer.newTransaction("Apollo.ConfigService", "syncRemoteConfig");

    try {
      ApolloConfig previous = m_configCache.get();  // 获得缓存的 ApolloConfig 对象
      ApolloConfig current = loadApolloConfig();  // 从 Config Service 加载 ApolloConfig 对象

      //reference equals means HTTP 304 // 若不相等，说明更新了，设置到缓存中
      if (previous != current) {
        logger.debug("Remote Config refreshed!");
        m_configCache.set(current); // 设置到缓存
        this.fireRepositoryChange(m_namespace, this.getConfig()); // 发布 Repository 的配置发生变化，触发对应的监听器们
      }

      if (current != null) {
        Tracer.logEvent(String.format("Apollo.Client.Configs.%s", current.getNamespaceName()),
            current.getReleaseKey());
      }

      transaction.setStatus(Transaction.SUCCESS);
    } catch (Throwable ex) {
      transaction.setStatus(ex);
      throw ex;
    } finally {
      transaction.complete();
    }
  }
  // 转换成 Properties 对象
  private Properties transformApolloConfigToProperties(ApolloConfig apolloConfig) {
    Properties result = propertiesFactory.getPropertiesInstance();
    result.putAll(apolloConfig.getConfigurations());
    return result;
  }
  // 从 Config Service 加载 ApolloConfig 对象
  private ApolloConfig loadApolloConfig() {
    if (!m_loadConfigRateLimiter.tryAcquire(5, TimeUnit.SECONDS)) { // 限流
      //wait at most 5 seconds
      try {
        TimeUnit.SECONDS.sleep(5);
      } catch (InterruptedException e) {
      }
    }
    String appId = m_configUtil.getAppId(); // 获得 appId cluster dataCenter 配置信息
    String cluster = m_configUtil.getCluster();
    String dataCenter = m_configUtil.getDataCenter();
    String secret = m_configUtil.getAccessKeySecret();
    Tracer.logEvent("Apollo.Client.ConfigMeta", STRING_JOINER.join(appId, cluster, m_namespace));
    int maxRetries = m_configNeedForceRefresh.get() ? 2 : 1; // 计算重试次数
    long onErrorSleepTime = 0; // 0 means no sleep
    Throwable exception = null;

    List<ServiceDTO> configServices = getConfigServices(); // 获得所有的 Config Service 的地址
    String url = null;
    retryLoopLabel:
    for (int i = 0; i < maxRetries; i++) { // 循环读取配置重试次数直到成功。每一次，都会循环所有的 ServiceDTO 数组
      List<ServiceDTO> randomConfigServices = Lists.newLinkedList(configServices); // 随机所有的 Config Service 的地址
      Collections.shuffle(randomConfigServices);
      //Access the server which notifies the client first
      if (m_longPollServiceDto.get() != null) {  // 优先访问通知配置变更的 Config Service 的地址。并且，获取到时，需要置空，避免重复优先访问
        randomConfigServices.add(0, m_longPollServiceDto.getAndSet(null));
      }

      for (ServiceDTO configService : randomConfigServices) { // 循环所有的 Config Service 的地址
        if (onErrorSleepTime > 0) {  // sleep 等待，下次从 Config Service 拉取配置
          logger.warn(
              "Load config failed, will retry in {} {}. appId: {}, cluster: {}, namespaces: {}",
              onErrorSleepTime, m_configUtil.getOnErrorRetryIntervalTimeUnit(), appId, cluster, m_namespace);

          try {
            m_configUtil.getOnErrorRetryIntervalTimeUnit().sleep(onErrorSleepTime);
          } catch (InterruptedException e) {
            //ignore
          }
        }
        // 组装查询配置的地址
        url = assembleQueryConfigUrl(configService.getHomepageUrl(), appId, cluster, m_namespace,
                dataCenter, m_remoteMessages.get(), m_configCache.get());

        logger.debug("Loading config from {}", url);
        // 创建 HttpRequest 对象
        HttpRequest request = new HttpRequest(url);
        if (!StringUtils.isBlank(secret)) {
          Map<String, String> headers = Signature.buildHttpHeaders(url, appId, secret);
          request.setHeaders(headers);
        }

        Transaction transaction = Tracer.newTransaction("Apollo.ConfigService", "queryConfig");
        transaction.addData("Url", url);
        try {
          // 发起请求，返回 HttpResponse 对象
          HttpResponse<ApolloConfig> response = m_httpClient.doGet(request, ApolloConfig.class);
          m_configNeedForceRefresh.set(false); // 设置 m_configNeedForceRefresh = false
          m_loadConfigFailSchedulePolicy.success(); // 标记成功

          transaction.addData("StatusCode", response.getStatusCode());
          transaction.setStatus(Transaction.SUCCESS);

          if (response.getStatusCode() == 304) { // 无新的配置，直接返回缓存的 ApolloConfig 对象
            logger.debug("Config server responds with 304 HTTP status code.");
            return m_configCache.get();
          }
          // 有新的配置，进行返回新的 ApolloConfig 对象
          ApolloConfig result = response.getBody();

          logger.debug("Loaded config for {}: {}", m_namespace, result);

          return result;
        } catch (ApolloConfigStatusCodeException ex) {
          ApolloConfigStatusCodeException statusCodeException = ex;
          //config not found
          if (ex.getStatusCode() == 404) { // 若返回的状态码是 404 ，说明查询配置的 Config Service 不存在该 Namespace
            String message = String.format(
                "Could not find config for namespace - appId: %s, cluster: %s, namespace: %s, " +
                    "please check whether the configs are released in Apollo!",
                appId, cluster, m_namespace);
            statusCodeException = new ApolloConfigStatusCodeException(ex.getStatusCode(),
                message);
          }
          Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(statusCodeException));
          transaction.setStatus(statusCodeException);
          exception = statusCodeException; // 设置最终的异常
          if(ex.getStatusCode() == 404) {
            break retryLoopLabel;
          }
        } catch (Throwable ex) {
          Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(ex));
          transaction.setStatus(ex);
          exception = ex; // 设置最终的异常
        } finally {
          transaction.complete();
        }
        // 计算延迟时间
        // if force refresh, do normal sleep, if normal config load, do exponential sleep
        onErrorSleepTime = m_configNeedForceRefresh.get() ? m_configUtil.getOnErrorRetryInterval() :
            m_loadConfigFailSchedulePolicy.fail();
      }

    }
    String message = String.format(
        "Load Apollo Config failed - appId: %s, cluster: %s, namespace: %s, url: %s",
        appId, cluster, m_namespace, url);
    throw new ApolloConfigException(message, exception); // 若查询配置失败，抛出 ApolloConfigException 异常
  }
  // 组装轮询 Config Service 的配置读取 "/configs/{appId}/{clusterName}/{namespace:.+}" 接口的 URL
  String assembleQueryConfigUrl(String uri, String appId, String cluster, String namespace,
                                String dataCenter, ApolloNotificationMessages remoteMessages, ApolloConfig previousConfig) {

    String path = "configs/%s/%s/%s";
    List<String> pathParams =
        Lists.newArrayList(pathEscaper.escape(appId), pathEscaper.escape(cluster),
            pathEscaper.escape(namespace));
    Map<String, String> queryParams = Maps.newHashMap();
    // releaseKey
    if (previousConfig != null) {
      queryParams.put("releaseKey", queryParamEscaper.escape(previousConfig.getReleaseKey()));
    }
    // dataCenter
    if (!Strings.isNullOrEmpty(dataCenter)) {
      queryParams.put("dataCenter", queryParamEscaper.escape(dataCenter));
    }
    // ip
    String localIp = m_configUtil.getLocalIp();
    if (!Strings.isNullOrEmpty(localIp)) {
      queryParams.put("ip", queryParamEscaper.escape(localIp));
    }
    // label
    String label = m_configUtil.getApolloLabel();
    if (!Strings.isNullOrEmpty(label)) {
      queryParams.put("label", queryParamEscaper.escape(label));
    }
    // messages
    if (remoteMessages != null) {
      queryParams.put("messages", queryParamEscaper.escape(GSON.toJson(remoteMessages)));
    }
    // 格式化 URL
    String pathExpanded = String.format(path, pathParams.toArray());
    // 拼接 Query String
    if (!queryParams.isEmpty()) {
      pathExpanded += "?" + MAP_JOINER.join(queryParams);
    }
    if (!uri.endsWith("/")) {
      uri += "/";
    }
    return uri + pathExpanded; // 拼接最终的请求 URL
  }
  // 将"自己"注册到 RemoteConfigLongPollService 中，实现配置更新的实时通知
  private void scheduleLongPollingRefresh() {
    remoteConfigLongPollService.submit(m_namespace, this);
  }
  // 当长轮询到配置更新时，发起同步配置的任务
  public void onLongPollNotified(ServiceDTO longPollNotifiedServiceDto, ApolloNotificationMessages remoteMessages) {
    m_longPollServiceDto.set(longPollNotifiedServiceDto); // 设置长轮询到配置更新的 Config Service 。下次同步配置时，优先读取该服务
    m_remoteMessages.set(remoteMessages); // 设置 m_remoteMessages
    m_executorService.submit(new Runnable() { // 提交同步任务
      @Override
      public void run() {
        m_configNeedForceRefresh.set(true); // 设置 m_configNeedForceRefresh 为 true
        trySync();  // 尝试同步配置
      }
    });
  }
  // 获得 Config Service 集群的地址们
  private List<ServiceDTO> getConfigServices() {
    List<ServiceDTO> services = m_serviceLocator.getConfigServices();
    if (services.size() == 0) {
      throw new ApolloConfigException("No available config service");
    }

    return services;
  }
}
