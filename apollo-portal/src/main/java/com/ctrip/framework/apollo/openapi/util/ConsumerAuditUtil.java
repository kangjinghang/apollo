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
package com.ctrip.framework.apollo.openapi.util;

import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.openapi.entity.ConsumerAudit;
import com.ctrip.framework.apollo.openapi.service.ConsumerService;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class ConsumerAuditUtil implements InitializingBean {
  private static final int CONSUMER_AUDIT_MAX_SIZE = 10000;
  private final BlockingQueue<ConsumerAudit> audits = Queues.newLinkedBlockingQueue(CONSUMER_AUDIT_MAX_SIZE); // 队列
  private final ExecutorService auditExecutorService; // ExecutorService 对象
  private final AtomicBoolean auditStopped; // 是否停止
  private static final int BATCH_SIZE = 100; // 批任务 ConsumerAudit 数量

  // ConsumerAuditUtilTest used reflection to set BATCH_TIMEOUT and BATCH_TIMEUNIT, so without `final` now
  private static long BATCH_TIMEOUT = 5;  // 批任务 ConsumerAudit 等待超时时间
  private static TimeUnit BATCH_TIMEUNIT = TimeUnit.SECONDS; // {@link #BATCH_TIMEOUT} 单位

  private final ConsumerService consumerService;

  public ConsumerAuditUtil(final ConsumerService consumerService) {
    this.consumerService = consumerService;
    auditExecutorService = Executors.newSingleThreadExecutor(
        ApolloThreadFactory.create("ConsumerAuditUtil", true));
    auditStopped = new AtomicBoolean(false);
  }

  public boolean audit(HttpServletRequest request, long consumerId) {
    //ignore GET request
    if ("GET".equalsIgnoreCase(request.getMethod())) { // 忽略 GET 请求
      return true;
    }
    String uri = request.getRequestURI(); // 组装 URI
    if (!Strings.isNullOrEmpty(request.getQueryString())) {
      uri += "?" + request.getQueryString();
    }
    // 创建 ConsumerAudit 对象
    ConsumerAudit consumerAudit = new ConsumerAudit();
    Date now = new Date();
    consumerAudit.setConsumerId(consumerId);
    consumerAudit.setUri(uri);
    consumerAudit.setMethod(request.getMethod());
    consumerAudit.setDataChangeCreatedTime(now);
    consumerAudit.setDataChangeLastModifiedTime(now);

    //throw away audits if exceeds the max size // 添加到队列
    return this.audits.offer(consumerAudit);
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    auditExecutorService.submit(() -> {
      while (!auditStopped.get() && !Thread.currentThread().isInterrupted()) { // 循环【批任务】，直到停止
        List<ConsumerAudit> toAudit = Lists.newArrayList();
        try {
          Queues.drain(audits, toAudit, BATCH_SIZE, BATCH_TIMEOUT, BATCH_TIMEUNIT);  // 获得 ConsumerAudit 批任务，直到到达上限，或者超时
          if (!toAudit.isEmpty()) {
            consumerService.createConsumerAudits(toAudit);
          }
        } catch (Throwable ex) {
          Tracer.logError(ex);
        }
      }
    });
  }

  public void stopAudit() {
    auditStopped.set(true);
  }
}
