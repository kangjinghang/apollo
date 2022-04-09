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

import com.ctrip.framework.apollo.core.ApolloClientSystemConsts;
import com.ctrip.framework.apollo.core.ServiceNameConsts;
import com.ctrip.framework.apollo.core.utils.DeferredLoggerFactory;
import com.ctrip.framework.apollo.core.utils.DeprecatedPropertyNotifyUtil;
import com.ctrip.framework.foundation.Foundation;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.dto.ServiceDTO;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import com.google.gson.reflect.TypeToken;
// Config Service 定位器
public class ConfigServiceLocator {
  private static final Logger logger = DeferredLoggerFactory.getLogger(ConfigServiceLocator.class);
  private HttpClient m_httpClient;
  private ConfigUtil m_configUtil;
  private AtomicReference<List<ServiceDTO>> m_configServices; // ServiceDTO 数组的缓存
  private Type m_responseType;
  private ScheduledExecutorService m_executorService; // 定时任务 ExecutorService
  private static final Joiner.MapJoiner MAP_JOINER = Joiner.on("&").withKeyValueSeparator("=");
  private static final Escaper queryParamEscaper = UrlEscapers.urlFormParameterEscaper();

  /**
   * Create a config service locator.
   */
  public ConfigServiceLocator() {
    List<ServiceDTO> initial = Lists.newArrayList();
    m_configServices = new AtomicReference<>(initial);
    m_responseType = new TypeToken<List<ServiceDTO>>() {
    }.getType();
    m_httpClient = ApolloInjector.getInstance(HttpClient.class);
    m_configUtil = ApolloInjector.getInstance(ConfigUtil.class);
    this.m_executorService = Executors.newScheduledThreadPool(1,
        ApolloThreadFactory.create("ConfigServiceLocator", true));
    initConfigServices();
  }

  private void initConfigServices() {
    // get from run time configurations
    List<ServiceDTO> customizedConfigServices = getCustomizedConfigService();

    if (customizedConfigServices != null) {
      setConfigServices(customizedConfigServices);
      return;
    }

    // update from meta service
    this.tryUpdateConfigServices(); // 初始拉取 Config Service 地址
    this.schedulePeriodicRefresh(); // 创建定时任务，定时拉取 Config Service 地址
  }

  private List<ServiceDTO> getCustomizedConfigService() {
    // 1. Get from System Property
    String configServices = System.getProperty(ApolloClientSystemConsts.APOLLO_CONFIG_SERVICE);
    if (Strings.isNullOrEmpty(configServices)) {
      // 2. Get from OS environment variable
      configServices = System.getenv(ApolloClientSystemConsts.APOLLO_CONFIG_SERVICE_ENVIRONMENT_VARIABLES);
    }
    if (Strings.isNullOrEmpty(configServices)) {
      // 3. Get from server.properties
      configServices = Foundation.server().getProperty(ApolloClientSystemConsts.APOLLO_CONFIG_SERVICE, null);
    }
    if (Strings.isNullOrEmpty(configServices)) {
      // 4. Get from deprecated config
      configServices = getDeprecatedCustomizedConfigService();
    }

    if (Strings.isNullOrEmpty(configServices)) {
      return null;
    }

    logger.info("Located config services from apollo.config-service configuration: {}, will not refresh config services from remote meta service!", configServices);

    // mock service dto list
    String[] configServiceUrls = configServices.split(",");
    List<ServiceDTO> serviceDTOS = Lists.newArrayList();

    for (String configServiceUrl : configServiceUrls) {
      configServiceUrl = configServiceUrl.trim();
      ServiceDTO serviceDTO = new ServiceDTO();
      serviceDTO.setHomepageUrl(configServiceUrl);
      serviceDTO.setAppName(ServiceNameConsts.APOLLO_CONFIGSERVICE);
      serviceDTO.setInstanceId(configServiceUrl);
      serviceDTOS.add(serviceDTO);
    }

    return serviceDTOS;
  }

  @SuppressWarnings("deprecation")
  private String getDeprecatedCustomizedConfigService() {
    // 1. Get from System Property
    String configServices = System.getProperty(ApolloClientSystemConsts.DEPRECATED_APOLLO_CONFIG_SERVICE);
    if (!Strings.isNullOrEmpty(configServices)) {
      DeprecatedPropertyNotifyUtil.warn(ApolloClientSystemConsts.DEPRECATED_APOLLO_CONFIG_SERVICE,
          ApolloClientSystemConsts.APOLLO_CONFIG_SERVICE);
    }
    if (Strings.isNullOrEmpty(configServices)) {
      // 2. Get from OS environment variable
      configServices = System.getenv(ApolloClientSystemConsts.DEPRECATED_APOLLO_CONFIG_SERVICE_ENVIRONMENT_VARIABLES);
      if (!Strings.isNullOrEmpty(configServices)) {
        DeprecatedPropertyNotifyUtil.warn(ApolloClientSystemConsts.DEPRECATED_APOLLO_CONFIG_SERVICE_ENVIRONMENT_VARIABLES,
            ApolloClientSystemConsts.APOLLO_CONFIG_SERVICE_ENVIRONMENT_VARIABLES);
      }
    }
    if (Strings.isNullOrEmpty(configServices)) {
      // 3. Get from server.properties
      configServices = Foundation.server().getProperty(ApolloClientSystemConsts.DEPRECATED_APOLLO_CONFIG_SERVICE, null);
      if (!Strings.isNullOrEmpty(configServices)) {
        DeprecatedPropertyNotifyUtil.warn(ApolloClientSystemConsts.DEPRECATED_APOLLO_CONFIG_SERVICE,
            ApolloClientSystemConsts.APOLLO_CONFIG_SERVICE);
      }
    }
    return configServices;
  }

  /**
   * Get the config service info from remote meta server.
   *
   * @return the services dto
   */
  public List<ServiceDTO> getConfigServices() {
    if (m_configServices.get().isEmpty()) { // 缓存为空，强制拉取
      updateConfigServices();
    }

    return m_configServices.get(); // 返回 ServiceDTO 数组
  }

  private boolean tryUpdateConfigServices() {
    try {
      updateConfigServices();
      return true;
    } catch (Throwable ex) {
      //ignore
    }
    return false;
  }

  private void schedulePeriodicRefresh() {
    this.m_executorService.scheduleAtFixedRate(
        new Runnable() {
          @Override
          public void run() {
            logger.debug("refresh config services");
            Tracer.logEvent("Apollo.MetaService", "periodicRefresh");
            tryUpdateConfigServices();  // 拉取 Config Service 地址
          }
        }, m_configUtil.getRefreshInterval(), m_configUtil.getRefreshInterval(),
        m_configUtil.getRefreshIntervalTimeUnit());
  }

  private synchronized void updateConfigServices() {
    String url = assembleMetaServiceUrl(); // 拼接请求 Meta Service URL

    HttpRequest request = new HttpRequest(url);
    int maxRetries = 2; // 重试两次
    Throwable exception = null;
    // 循环请求 Meta Service ，获取 Config Service 地址
    for (int i = 0; i < maxRetries; i++) {
      Transaction transaction = Tracer.newTransaction("Apollo.MetaService", "getConfigService");
      transaction.addData("Url", url);
      try {
        HttpResponse<List<ServiceDTO>> response = m_httpClient.doGet(request, m_responseType); // 请求
        transaction.setStatus(Transaction.SUCCESS);
        List<ServiceDTO> services = response.getBody(); // 获得结果 ServiceDTO 数组
        if (services == null || services.isEmpty()) { // 获得结果为空，重新请求
          logConfigService("Empty response!");
          continue;
        }
        setConfigServices(services); // 更新缓存
        return;
      } catch (Throwable ex) {
        Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(ex));
        transaction.setStatus(ex);
        exception = ex; // 暂存一下异常
      } finally {
        transaction.complete();
      }
      // 请求失败，sleep 等待下次重试
      try {
        m_configUtil.getOnErrorRetryIntervalTimeUnit().sleep(m_configUtil.getOnErrorRetryInterval());
      } catch (InterruptedException ex) {
        //ignore
      }
    }
    // 请求全部失败，抛出 ApolloConfigException 异常
    throw new ApolloConfigException(
        String.format("Get config services failed from %s", url), exception);
  }

  private void setConfigServices(List<ServiceDTO> services) {
    m_configServices.set(services);
    logConfigServices(services); // 打印结果 ServiceDTO 数组
  }

  private String assembleMetaServiceUrl() {
    String domainName = m_configUtil.getMetaServerDomainName();
    String appId = m_configUtil.getAppId();
    String localIp = m_configUtil.getLocalIp();
    // 参数集合
    Map<String, String> queryParams = Maps.newHashMap();
    queryParams.put("appId", queryParamEscaper.escape(appId));
    if (!Strings.isNullOrEmpty(localIp)) {
      queryParams.put("ip", queryParamEscaper.escape(localIp));
    }

    return domainName + "/services/config?" + MAP_JOINER.join(queryParams);
  }

  private void logConfigServices(List<ServiceDTO> serviceDtos) {
    for (ServiceDTO serviceDto : serviceDtos) {
      logConfigService(serviceDto.getHomepageUrl());
    }
  }

  private void logConfigService(String serviceUrl) {
    Tracer.logEvent("Apollo.Config.Services", serviceUrl);
  }
}
