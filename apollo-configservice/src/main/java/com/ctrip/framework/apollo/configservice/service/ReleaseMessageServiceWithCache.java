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
package com.ctrip.framework.apollo.configservice.service;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.message.ReleaseMessageListener;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class ReleaseMessageServiceWithCache implements ReleaseMessageListener, InitializingBean {
  private static final Logger logger = LoggerFactory.getLogger(ReleaseMessageServiceWithCache
      .class);
  private final ReleaseMessageRepository releaseMessageRepository;
  private final BizConfig bizConfig;

  private int scanInterval; // 扫描周期
  private TimeUnit scanIntervalTimeUnit; // 扫描周期单位

  private volatile long maxIdScanned; // 最后扫描到的 ReleaseMessage 的编号
  // ReleaseMessage 缓存。KEY：ReleaseMessage.message。VALUE：对应的最新的 ReleaseMessage 记录
  private ConcurrentMap<String, ReleaseMessage> releaseMessageCache;

  private AtomicBoolean doScan; // 是否执行扫描任务
  private ExecutorService executorService; // ExecutorService 对象

  public ReleaseMessageServiceWithCache(
      final ReleaseMessageRepository releaseMessageRepository,
      final BizConfig bizConfig) {
    this.releaseMessageRepository = releaseMessageRepository;
    this.bizConfig = bizConfig;
    initialize();
  }

  private void initialize() {
    releaseMessageCache = Maps.newConcurrentMap(); // 创建缓存对象
    doScan = new AtomicBoolean(true); // 设置 doScan 为 true
    executorService = Executors.newSingleThreadExecutor(ApolloThreadFactory
        .create("ReleaseMessageServiceWithCache", true)); // 创建 ScheduledExecutorService 对象，大小为 1
  }

  public ReleaseMessage findLatestReleaseMessageForMessages(Set<String> messages) {
    if (CollectionUtils.isEmpty(messages)) {
      return null;
    }

    long maxReleaseMessageId = 0;
    ReleaseMessage result = null;
    for (String message : messages) {
      ReleaseMessage releaseMessage = releaseMessageCache.get(message);
      if (releaseMessage != null && releaseMessage.getId() > maxReleaseMessageId) {
        maxReleaseMessageId = releaseMessage.getId();
        result = releaseMessage;
      }
    }

    return result;
  }
  // 获得每条消息内容对应的最新的 ReleaseMessage 对象
  public List<ReleaseMessage> findLatestReleaseMessagesGroupByMessages(Set<String> messages) {
    if (CollectionUtils.isEmpty(messages)) {
      return Collections.emptyList();
    }
    List<ReleaseMessage> releaseMessages = Lists.newArrayList();

    for (String message : messages) { // 获得每条消息内容对应的最新的 ReleaseMessage 对象
      ReleaseMessage releaseMessage = releaseMessageCache.get(message);
      if (releaseMessage != null) {
        releaseMessages.add(releaseMessage);
      }
    }

    return releaseMessages;
  }

  @Override
  public void handleMessage(ReleaseMessage message, String channel) {
    //Could stop once the ReleaseMessageScanner starts to work
    doScan.set(false); // 关闭增量拉取定时任务的执行
    logger.info("message received - channel: {}, message: {}", channel, message);

    String content = message.getMessage();
    Tracer.logEvent("Apollo.ReleaseMessageService.UpdateCache", String.valueOf(message.getId()));
    if (!Topics.APOLLO_RELEASE_TOPIC.equals(channel) || Strings.isNullOrEmpty(content)) { // 仅处理 APOLLO_RELEASE_TOPIC
      return;
    }
    // 计算 gap
    long gap = message.getId() - maxIdScanned;
    if (gap == 1) { // 若无空缺 gap ，直接合并
      mergeReleaseMessage(message);
    } else if (gap > 1) { // 如有空缺 gap ，增量拉取
      //gap found!
      loadReleaseMessages(maxIdScanned);
    }
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    populateDataBaseInterval(); // 从 ServerConfig 中，读取任务的周期配置
    //block the startup process until load finished
    //this should happen before ReleaseMessageScanner due to autowire
    loadReleaseMessages(0); // 初始拉取 ReleaseMessage 到缓存
    // 创建定时任务，增量拉取 ReleaseMessage 到缓存，用以处理初始化期间，产生的 ReleaseMessage 遗漏的问题
    executorService.submit(() -> {
      while (doScan.get() && !Thread.currentThread().isInterrupted()) {
        Transaction transaction = Tracer.newTransaction("Apollo.ReleaseMessageServiceWithCache",
            "scanNewReleaseMessages");
        try {
          loadReleaseMessages(maxIdScanned); // 增量拉取 ReleaseMessage 到缓存
          transaction.setStatus(Transaction.SUCCESS);
        } catch (Throwable ex) {
          transaction.setStatus(ex);
          logger.error("Scan new release messages failed", ex);
        } finally {
          transaction.complete();
        }
        try {
          scanIntervalTimeUnit.sleep(scanInterval);
        } catch (InterruptedException e) {
          //ignore
        }
      }
    });
  }

  private synchronized void mergeReleaseMessage(ReleaseMessage releaseMessage) {
    ReleaseMessage old = releaseMessageCache.get(releaseMessage.getMessage()); // 获得对应的 ReleaseMessage 对象
    if (old == null || releaseMessage.getId() > old.getId()) { // 若编号更大，进行更新缓存
      releaseMessageCache.put(releaseMessage.getMessage(), releaseMessage);
      maxIdScanned = releaseMessage.getId();
    }
  }

  private void loadReleaseMessages(long startId) {
    boolean hasMore = true;
    while (hasMore && !Thread.currentThread().isInterrupted()) {
      //current batch is 500 // 获得大于 maxIdScanned 的 500 条 ReleaseMessage 记录，按照 id 升序
      List<ReleaseMessage> releaseMessages = releaseMessageRepository
          .findFirst500ByIdGreaterThanOrderByIdAsc(startId);
      if (CollectionUtils.isEmpty(releaseMessages)) {
        break;
      }
      releaseMessages.forEach(this::mergeReleaseMessage); // 合并到 ReleaseMessage 缓存
      int scanned = releaseMessages.size(); // 获得新的 maxIdScanned ，取最后一条记录
      startId = releaseMessages.get(scanned - 1).getId();
      hasMore = scanned == 500; // 若拉取不足 500 条，说明无新消息了
      logger.info("Loaded {} release messages with startId {}", scanned, startId);
    }
  }

  private void populateDataBaseInterval() {
    scanInterval = bizConfig.releaseMessageCacheScanInterval(); // "apollo.release-message-cache-scan.interval" ，默认为 1
    scanIntervalTimeUnit = bizConfig.releaseMessageCacheScanIntervalTimeUnit();  // 默认秒，不可配置
  }

  //only for test use
  private void reset() throws Exception {
    executorService.shutdownNow();
    initialize();
    afterPropertiesSet();
  }
}
