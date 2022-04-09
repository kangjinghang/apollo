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
package com.ctrip.framework.apollo.portal.component;

import com.ctrip.framework.apollo.portal.environment.PortalMetaDomainService;
import com.ctrip.framework.apollo.core.dto.ServiceDTO;
import com.ctrip.framework.apollo.portal.environment.Env;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.http.HttpMessageConverters;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
// Admin Service 定位器
@Component
public class AdminServiceAddressLocator {

  private static final long NORMAL_REFRESH_INTERVAL = 5 * 60 * 1000;
  private static final long OFFLINE_REFRESH_INTERVAL = 10 * 1000;
  private static final int RETRY_TIMES = 3;
  private static final String ADMIN_SERVICE_URL_PATH = "/services/admin";
  private static final Logger logger = LoggerFactory.getLogger(AdminServiceAddressLocator.class);

  private ScheduledExecutorService refreshServiceAddressService; // 定时任务 ExecutorService
  private RestTemplate restTemplate;
  private List<Env> allEnvs; // Env 数组
  private Map<Env, List<ServiceDTO>> cache = new ConcurrentHashMap<>(); // List<ServiceDTO 缓存 Map。KEY：ENV

  private final PortalSettings portalSettings;
  private final RestTemplateFactory restTemplateFactory;
  private final PortalMetaDomainService portalMetaDomainService;

  public AdminServiceAddressLocator(
      final HttpMessageConverters httpMessageConverters,
      final PortalSettings portalSettings,
      final RestTemplateFactory restTemplateFactory,
      final PortalMetaDomainService portalMetaDomainService
  ) {
    this.portalSettings = portalSettings;
    this.restTemplateFactory = restTemplateFactory;
    this.portalMetaDomainService = portalMetaDomainService;
  }

  @PostConstruct
  public void init() {
    allEnvs = portalSettings.getAllEnvs(); // 获得 Env 数组

    //init restTemplate
    restTemplate = restTemplateFactory.getObject();

    refreshServiceAddressService = // 创建 ScheduledExecutorService
        Executors.newScheduledThreadPool(1, ApolloThreadFactory.create("ServiceLocator", true));
    // 创建延迟任务，1 秒后拉取 Admin Service 地址
    refreshServiceAddressService.schedule(new RefreshAdminServerAddressTask(), 1, TimeUnit.MILLISECONDS);
  }

  public List<ServiceDTO> getServiceList(Env env) {
    List<ServiceDTO> services = cache.get(env); // 从缓存中获得 ServiceDTO 数组
    if (CollectionUtils.isEmpty(services)) { // 若不存在，直接返回空数组。这点和 ConfigServiceLocator 不同
      return Collections.emptyList();
    }
    List<ServiceDTO> randomConfigServices = Lists.newArrayList(services);
    Collections.shuffle(randomConfigServices); // 打乱 ServiceDTO 数组，返回。实现 Client 级的负载均衡
    return randomConfigServices;
  }

  //maintain admin server address
  private class RefreshAdminServerAddressTask implements Runnable {

    @Override
    public void run() {
      boolean refreshSuccess = true;
      //refresh fail if get any env address fail
      for (Env env : allEnvs) {  // 循环多个 Env ，请求对应的 Meta Service ，获得 Admin Service 集群地址
        boolean currentEnvRefreshResult = refreshServerAddressCache(env);
        refreshSuccess = refreshSuccess && currentEnvRefreshResult;
      }

      if (refreshSuccess) { // 若刷新成功，则创建定时任务，5 分钟后执行
        refreshServiceAddressService
            .schedule(new RefreshAdminServerAddressTask(), NORMAL_REFRESH_INTERVAL, TimeUnit.MILLISECONDS);
      } else { // 若刷新失败，则创建定时任务，10 秒后执行
        refreshServiceAddressService
            .schedule(new RefreshAdminServerAddressTask(), OFFLINE_REFRESH_INTERVAL, TimeUnit.MILLISECONDS);
      }
    }
  }

  private boolean refreshServerAddressCache(Env env) {

    for (int i = 0; i < RETRY_TIMES; i++) {

      try {
        ServiceDTO[] services = getAdminServerAddress(env); // 请求 Meta Service ，获得 Admin Service 集群地址
        if (services == null || services.length == 0) { // 获得结果为空，continue ，继续执行下一次请求
          continue;
        }
        cache.put(env, Arrays.asList(services)); // 更新缓存
        return true; // 返回获取成功
      } catch (Throwable e) {
        logger.error(String.format("Get admin server address from meta server failed. env: %s, meta server address:%s",
                                   env, portalMetaDomainService.getDomain(env)), e);
        Tracer
            .logError(String.format("Get admin server address from meta server failed. env: %s, meta server address:%s",
                                    env, portalMetaDomainService.getDomain(env)), e);
      }
    }
    return false; // 返回获取失败
  }

  private ServiceDTO[] getAdminServerAddress(Env env) {
    String domainName = portalMetaDomainService.getDomain(env);
    String url = domainName + ADMIN_SERVICE_URL_PATH;
    return restTemplate.getForObject(url, ServiceDTO[].class);
  }


}
