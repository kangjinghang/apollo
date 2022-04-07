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
package com.ctrip.framework.apollo.biz.grayReleaseRule;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.GrayReleaseRule;
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.message.ReleaseMessageListener;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.repository.GrayReleaseRuleRepository;
import com.ctrip.framework.apollo.common.constants.NamespaceBranchStatus;
import com.ctrip.framework.apollo.common.dto.GrayReleaseRuleItemDTO;
import com.ctrip.framework.apollo.common.utils.GrayReleaseRuleItemTransformer;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;

import com.google.common.collect.TreeMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class GrayReleaseRulesHolder implements ReleaseMessageListener, InitializingBean { // 缓存Holder ，用于提高对 GrayReleaseRule 的读取速度
  private static final Logger logger = LoggerFactory.getLogger(GrayReleaseRulesHolder.class);
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
  private static final Splitter STRING_SPLITTER =
      Splitter.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR).omitEmptyStrings();

  @Autowired
  private GrayReleaseRuleRepository grayReleaseRuleRepository;
  @Autowired
  private BizConfig bizConfig;

  private int databaseScanInterval; // 数据库扫描频率，单位：秒
  private ScheduledExecutorService executorService; // ExecutorService 对象
  //store configAppId+configCluster+configNamespace -> GrayReleaseRuleCache map
  private Multimap<String, GrayReleaseRuleCache> grayReleaseRuleCache; // GrayReleaseRuleCache 缓存。 KEY：configAppId+configCluster+configNamespace ，通过 {@link #assembleGrayReleaseRuleKey(String, String, String)} 生成。注意，KEY 中不包含 BranchName
  //store clientAppId+clientNamespace+ip -> ruleId map
  private Multimap<String, Long> reversedGrayReleaseRuleCache; // GrayReleaseRuleCache 缓存2。KEY：clientAppId+clientNamespace+ip ，通过 {@link #assembleReversedGrayReleaseRuleKey(String, String, String)} 生成。注意，KEY 中不包含 ClusterName
  //an auto increment version to indicate the age of rules
  private AtomicLong loadVersion; // 加载版本号

  public GrayReleaseRulesHolder() {
    loadVersion = new AtomicLong();
    grayReleaseRuleCache = Multimaps.synchronizedSetMultimap(
        TreeMultimap.create(String.CASE_INSENSITIVE_ORDER, Ordering.natural()));
    reversedGrayReleaseRuleCache = Multimaps.synchronizedSetMultimap(
        TreeMultimap.create(String.CASE_INSENSITIVE_ORDER, Ordering.natural()));
    executorService = Executors.newScheduledThreadPool(1, ApolloThreadFactory
        .create("GrayReleaseRulesHolder", true));
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    populateDataBaseInterval();  // 从 ServerConfig 中，读取任务的周期配置
    //force sync load for the first time
    periodicScanRules(); // 初始拉取 GrayReleaseRuleCache 到缓存
    executorService.scheduleWithFixedDelay(this::periodicScanRules, // 定时拉取 GrayReleaseRuleCache 到缓存
        getDatabaseScanIntervalSecond(), getDatabaseScanIntervalSecond(), getDatabaseScanTimeUnit()
    );
  }
  // 基于 ReleaseMessage "近实时"通知，更新缓存
  @Override
  public void handleMessage(ReleaseMessage message, String channel) {
    logger.info("message received - channel: {}, message: {}", channel, message);
    String releaseMessage = message.getMessage();
    if (!Topics.APOLLO_RELEASE_TOPIC.equals(channel) || Strings.isNullOrEmpty(releaseMessage)) { // 只处理 APOLLO_RELEASE_TOPIC 的消息
      return;
    }
    List<String> keys = STRING_SPLITTER.splitToList(releaseMessage); // 获得 appId cluster namespace 参数
    //message should be appId+cluster+namespace
    if (keys.size() != 3) {
      logger.error("message format invalid - {}", releaseMessage);
      return;
    }
    String appId = keys.get(0);
    String cluster = keys.get(1);
    String namespace = keys.get(2);
    // 获得对应的 GrayReleaseRule 数组
    List<GrayReleaseRule> rules = grayReleaseRuleRepository
        .findByAppIdAndClusterNameAndNamespaceName(appId, cluster, namespace);
    // 合并到 GrayReleaseRule 缓存中
    mergeGrayReleaseRules(rules);
  }
  // 拉取 GrayReleaseRuleCache 到缓存
  private void periodicScanRules() {
    Transaction transaction = Tracer.newTransaction("Apollo.GrayReleaseRulesScanner",
        "scanGrayReleaseRules");
    try {
      loadVersion.incrementAndGet(); // 递增加载版本号
      scanGrayReleaseRules(); // 从数据卷库中，扫描所有 GrayReleaseRules ，并合并到缓存中
      transaction.setStatus(Transaction.SUCCESS);
    } catch (Throwable ex) {
      transaction.setStatus(ex);
      logger.error("Scan gray release rule failed", ex);
    } finally {
      transaction.complete();
    }
  }
  // 若匹配上灰度规则，返回对应的 Release 编号
  public Long findReleaseIdFromGrayReleaseRule(String clientAppId, String clientIp, String clientLabel, String
      configAppId, String configCluster, String configNamespaceName) {
    String key = assembleGrayReleaseRuleKey(configAppId, configCluster, configNamespaceName);
    if (!grayReleaseRuleCache.containsKey(key)) { // 判断 grayReleaseRuleCache 中是否存在
      return null;
    }
    //create a new list to avoid ConcurrentModificationException
    List<GrayReleaseRuleCache> rules = Lists.newArrayList(grayReleaseRuleCache.get(key));
    for (GrayReleaseRuleCache rule : rules) { // 循环 GrayReleaseRuleCache 数组，获得匹配的 Release 编号
      //check branch status
      if (rule.getBranchStatus() != NamespaceBranchStatus.ACTIVE) {  // 校验 GrayReleaseRuleCache 对应的子 Namespace 的状态是否为有效
        continue;
      }
      if (rule.matches(clientAppId, clientIp, clientLabel)) { // 是否匹配灰度规则。若是，则返回。
        return rule.getReleaseId();
      }
    }
    return null;
  }

  /**
   * Check whether there are gray release rules for the clientAppId, clientIp, namespace
   * combination. Please note that even there are gray release rules, it doesn't mean it will always
   * load gray releases. Because gray release rules actually apply to one more dimension - cluster.
   */
  public boolean hasGrayReleaseRule(String clientAppId, String clientIp, String namespaceName) {
    return reversedGrayReleaseRuleCache.containsKey(assembleReversedGrayReleaseRuleKey(clientAppId,
        namespaceName, clientIp)) || reversedGrayReleaseRuleCache.containsKey
        (assembleReversedGrayReleaseRuleKey(clientAppId, namespaceName, GrayReleaseRuleItemDTO
            .ALL_IP));
  }

  private void scanGrayReleaseRules() {
    long maxIdScanned = 0;
    boolean hasMore = true;

    while (hasMore && !Thread.currentThread().isInterrupted()) { // 循环顺序分批加载 GrayReleaseRule ，直到结束或者线程打断
      List<GrayReleaseRule> grayReleaseRules = grayReleaseRuleRepository
          .findFirst500ByIdGreaterThanOrderByIdAsc(maxIdScanned);  // 顺序分批加载 GrayReleaseRule 500 条
      if (CollectionUtils.isEmpty(grayReleaseRules)) {
        break;
      }
      mergeGrayReleaseRules(grayReleaseRules); // 合并到 GrayReleaseRule 缓存
      int rulesScanned = grayReleaseRules.size();  // 获得新的 maxIdScanned ，取最后一条记录
      maxIdScanned = grayReleaseRules.get(rulesScanned - 1).getId();
      //batch is 500
      hasMore = rulesScanned == 500; // 若拉取不足 500 条，说明无 GrayReleaseRule 了
    }
  }
  // 合并 GrayReleaseRule 到缓存中
  private void mergeGrayReleaseRules(List<GrayReleaseRule> grayReleaseRules) {
    if (CollectionUtils.isEmpty(grayReleaseRules)) {
      return;
    }
    for (GrayReleaseRule grayReleaseRule : grayReleaseRules) { // !!! 注意，下面我们说的“老”，指的是已经在缓存中，但是实际不一定“老”。
      if (grayReleaseRule.getReleaseId() == null || grayReleaseRule.getReleaseId() == 0) { // 无对应的 Release 编号，记未灰度发布，则无视
        //filter rules with no release id, i.e. never released
        continue;
      }
      String key = assembleGrayReleaseRuleKey(grayReleaseRule.getAppId(), grayReleaseRule
          .getClusterName(), grayReleaseRule.getNamespaceName()); // 创建 grayReleaseRuleCache 的 KEY
      //create a new list to avoid ConcurrentModificationException // 从缓存 grayReleaseRuleCache 读取，并创建数组，避免并发
      List<GrayReleaseRuleCache> rules = Lists.newArrayList(grayReleaseRuleCache.get(key));
      GrayReleaseRuleCache oldRule = null; // 获得子 Namespace 对应的老的 GrayReleaseRuleCache 对象
      for (GrayReleaseRuleCache ruleCache : rules) {
        if (ruleCache.getBranchName().equals(grayReleaseRule.getBranchName())) {
          oldRule = ruleCache;
          break;
        }
      }
      // 忽略，若不存在老的 GrayReleaseRuleCache ，并且当前 GrayReleaseRule 对应的分支不处于激活( 有效 )状态
      //if old rule is null and new rule's branch status is not active, ignore
      if (oldRule == null && grayReleaseRule.getBranchStatus() != NamespaceBranchStatus.ACTIVE) {
        continue;
      }
      // 若新的 GrayReleaseRule 为新增或更新，进行缓存更新
      //use id comparison to avoid synchronization
      if (oldRule == null || grayReleaseRule.getId() > oldRule.getRuleId()) {
        addCache(key, transformRuleToRuleCache(grayReleaseRule));  // 添加新的 GrayReleaseRuleCache 到缓存中
        if (oldRule != null) {
          removeCache(key, oldRule); // 移除老的 GrayReleaseRuleCache 出缓存中
        }
      } else {  // 老的 GrayReleaseRuleCache 对应的分支处于激活( 有效 )状态，更新加载版本号。
        // 例如，定时轮询，有可能，早于 #handleMessage(...) 拿到对应的新的 GrayReleaseRule 记录，那么此时规则编号是相等的，不符合上面的条件，但是符合这个条件。
        // 再例如，两次定时轮询，第二次和第一次的规则编号是相等的，不符合上面的条件，但是符合这个条件。
        if (oldRule.getBranchStatus() == NamespaceBranchStatus.ACTIVE) {
          //update load version
          oldRule.setLoadVersion(loadVersion.get());
        } else if ((loadVersion.get() - oldRule.getLoadVersion()) > 1) { // 保留两轮，适用于 GrayReleaseRule.branchStatus 为 DELETED 或 MERGED 的情况
          //remove outdated inactive branch rule after 2 update cycles
          removeCache(key, oldRule);
        }
      }
    }
  }
  // 添加新的 GrayReleaseRuleCache 到缓存中
  private void addCache(String key, GrayReleaseRuleCache ruleCache) {
    if (ruleCache.getBranchStatus() == NamespaceBranchStatus.ACTIVE) { // 为什么这里判断状态？因为删除灰度，或者灰度全量发布的情况下，是无效的，所以不添加到 reversedGrayReleaseRuleCache 中
      for (GrayReleaseRuleItemDTO ruleItemDTO : ruleCache.getRuleItems()) {
        for (String clientIp : ruleItemDTO.getClientIpList()) {
          reversedGrayReleaseRuleCache.put(assembleReversedGrayReleaseRuleKey(ruleItemDTO
              .getClientAppId(), ruleCache.getNamespaceName(), clientIp), ruleCache.getRuleId());
        }
      }
    }
    grayReleaseRuleCache.put(key, ruleCache); // 添加到 grayReleaseRuleCache。这里为什么可以添加？因为添加到 grayReleaseRuleCache 中是个对象，可以判断状态
  }
  // 移除老 的 GrayReleaseRuleCache 出缓存
  private void removeCache(String key, GrayReleaseRuleCache ruleCache) {
    grayReleaseRuleCache.remove(key, ruleCache); // 移除出 grayReleaseRuleCache
    for (GrayReleaseRuleItemDTO ruleItemDTO : ruleCache.getRuleItems()) { // 移除出 reversedGrayReleaseRuleCache
      for (String clientIp : ruleItemDTO.getClientIpList()) {
        reversedGrayReleaseRuleCache.remove(assembleReversedGrayReleaseRuleKey(ruleItemDTO
            .getClientAppId(), ruleCache.getNamespaceName(), clientIp), ruleCache.getRuleId());
      }
    }
  }
  // 将 GrayReleaseRule 转换成 GrayReleaseRuleCache 对象
  private GrayReleaseRuleCache transformRuleToRuleCache(GrayReleaseRule grayReleaseRule) {
    Set<GrayReleaseRuleItemDTO> ruleItems; // 转换出 GrayReleaseRuleItemDTO 数组
    try {
      ruleItems = GrayReleaseRuleItemTransformer.batchTransformFromJSON(grayReleaseRule.getRules());
    } catch (Throwable ex) {
      ruleItems = Sets.newHashSet();
      Tracer.logError(ex);
      logger.error("parse rule for gray release rule {} failed", grayReleaseRule.getId(), ex);
    }
    // 创建 GrayReleaseRuleCache 对象，并返回
    GrayReleaseRuleCache ruleCache = new GrayReleaseRuleCache(grayReleaseRule.getId(),
        grayReleaseRule.getBranchName(), grayReleaseRule.getNamespaceName(), grayReleaseRule
        .getReleaseId(), grayReleaseRule.getBranchStatus(), loadVersion.get(), ruleItems);

    return ruleCache;
  }

  private void populateDataBaseInterval() {
    databaseScanInterval = bizConfig.grayReleaseRuleScanInterval(); // "apollo.gray-release-rule-scan.interval" ，默认为 60
  }

  private int getDatabaseScanIntervalSecond() {
    return databaseScanInterval;
  }

  private TimeUnit getDatabaseScanTimeUnit() {
    return TimeUnit.SECONDS;
  }

  private String assembleGrayReleaseRuleKey(String configAppId, String configCluster, String
      configNamespaceName) {
    return STRING_JOINER.join(configAppId, configCluster, configNamespaceName);
  }

  private String assembleReversedGrayReleaseRuleKey(String clientAppId, String
      clientNamespaceName, String clientIp) {
    return STRING_JOINER.join(clientAppId, clientNamespaceName, clientIp);
  }

}
