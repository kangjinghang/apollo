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
package com.ctrip.framework.apollo.adminservice.aop;


import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.Item;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.entity.NamespaceLock;
import com.ctrip.framework.apollo.biz.service.ItemService;
import com.ctrip.framework.apollo.biz.service.NamespaceLockService;
import com.ctrip.framework.apollo.biz.service.NamespaceService;
import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.exception.ServiceException;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;


/**
 * 一个namespace在一次发布中只能允许一个人修改配置
 * 通过数据库lock表来实现
 */
@Aspect
@Component
public class NamespaceAcquireLockAspect {
  private static final Logger logger = LoggerFactory.getLogger(NamespaceAcquireLockAspect.class);

  private final NamespaceLockService namespaceLockService;
  private final NamespaceService namespaceService;
  private final ItemService itemService;
  private final BizConfig bizConfig;

  public NamespaceAcquireLockAspect(
      final NamespaceLockService namespaceLockService,
      final NamespaceService namespaceService,
      final ItemService itemService,
      final BizConfig bizConfig) {
    this.namespaceLockService = namespaceLockService;
    this.namespaceService = namespaceService;
    this.itemService = itemService;
    this.bizConfig = bizConfig;
  }


  //create item
  @Before("@annotation(PreAcquireNamespaceLock) && args(appId, clusterName, namespaceName, item, ..)")
  public void requireLockAdvice(String appId, String clusterName, String namespaceName,
                                ItemDTO item) {
    acquireLock(appId, clusterName, namespaceName, item.getDataChangeLastModifiedBy()); // 尝试锁定
  }

  //update item
  @Before("@annotation(PreAcquireNamespaceLock) && args(appId, clusterName, namespaceName, itemId, item, ..)")
  public void requireLockAdvice(String appId, String clusterName, String namespaceName, long itemId,
                                ItemDTO item) {
    acquireLock(appId, clusterName, namespaceName, item.getDataChangeLastModifiedBy()); // 尝试锁定
  }

  //update by change set
  @Before("@annotation(PreAcquireNamespaceLock) && args(appId, clusterName, namespaceName, changeSet, ..)")
  public void requireLockAdvice(String appId, String clusterName, String namespaceName,
                                ItemChangeSets changeSet) {
    acquireLock(appId, clusterName, namespaceName, changeSet.getDataChangeLastModifiedBy());  // 尝试锁定
  }

  //delete item
  @Before("@annotation(PreAcquireNamespaceLock) && args(itemId, operator, ..)")
  public void requireLockAdvice(long itemId, String operator) {
    Item item = itemService.findOne(itemId); // 获得 Item 对象。若不存在，抛出 BadRequestException 异常
    if (item == null){
      throw new BadRequestException("item not exist.");
    }
    acquireLock(item.getNamespaceId(), operator); // 尝试锁定
  }

  void acquireLock(String appId, String clusterName, String namespaceName,
                           String currentUser) {
    if (bizConfig.isNamespaceLockSwitchOff()) { // 当关闭锁定 Namespace 开关时，直接返回
      return;
    }
    // 获得 Namespace 对象
    Namespace namespace = namespaceService.findOne(appId, clusterName, namespaceName);
    // 尝试锁定
    acquireLock(namespace, currentUser);
  }

  void acquireLock(long namespaceId, String currentUser) {
    if (bizConfig.isNamespaceLockSwitchOff()) { // 当关闭锁定 Namespace 开关时，直接返回
      return;
    }
    // 获得 Namespace 对象
    Namespace namespace = namespaceService.findOne(namespaceId);
    // 尝试锁定
    acquireLock(namespace, currentUser);

  }

  private void acquireLock(Namespace namespace, String currentUser) {
    if (namespace == null) { // 当 Namespace 为空时，抛出 BadRequestException 异常
      throw new BadRequestException("namespace not exist.");
    }

    long namespaceId = namespace.getId();
    // 获得 NamespaceLock 对象
    NamespaceLock namespaceLock = namespaceLockService.findLock(namespaceId);
    if (namespaceLock == null) {
      try {
        tryLock(namespaceId, currentUser);  // 锁定
        //lock success
      } catch (DataIntegrityViolationException e) {
        //lock fail
        namespaceLock = namespaceLockService.findLock(namespaceId);  // 锁定失败，获得 NamespaceLock 对象
        checkLock(namespace, namespaceLock, currentUser); // 校验锁定人是否是当前管理员
      } catch (Exception e) {
        logger.error("try lock error", e);
        throw e;
      }
    } else {
      //check lock owner is current user
      checkLock(namespace, namespaceLock, currentUser); // 校验锁定人是否是当前管理员
    }
  }

  private void tryLock(long namespaceId, String user) {
    NamespaceLock lock = new NamespaceLock(); // 创建 NamespaceLock 对象
    lock.setNamespaceId(namespaceId);
    lock.setDataChangeCreatedBy(user);  // 管理员
    lock.setDataChangeLastModifiedBy(user);  // 管理员
    namespaceLockService.tryLock(lock); // 保存 NamespaceLock 对象
  }

  private void checkLock(Namespace namespace, NamespaceLock namespaceLock,
                         String currentUser) {
    if (namespaceLock == null) { // 当 NamespaceLock 不存在，抛出 ServiceException 异常
      throw new ServiceException(
          String.format("Check lock for %s failed, please retry.", namespace.getNamespaceName()));
    }

    String lockOwner = namespaceLock.getDataChangeCreatedBy();
    if (!lockOwner.equals(currentUser)) { // 校验锁定人是否是当前管理员。若不是，抛出 BadRequestException 异常
      throw new BadRequestException(
          "namespace:" + namespace.getNamespaceName() + " is modified by " + lockOwner);
    }
  }


}
