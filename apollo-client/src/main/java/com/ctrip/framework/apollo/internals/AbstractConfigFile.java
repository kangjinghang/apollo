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
import com.ctrip.framework.apollo.core.utils.DeferredLoggerFactory;
import com.ctrip.framework.apollo.enums.ConfigSourceType;
import com.ctrip.framework.apollo.util.factory.PropertiesFactory;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigFileChangeListener;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.enums.PropertyChangeType;
import com.ctrip.framework.apollo.model.ConfigFileChangeEvent;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.google.common.collect.Lists;

/** 实现 ConfigFile、RepositoryChangeListener 接口，ConfigFile 抽象类，实现了 1）异步通知监听器、2）计算属性变化等等特性，是 AbstractConfig + DefaultConfig 的功能子集
 * @author Jason Song(song_s@ctrip.com)
 */
public abstract class AbstractConfigFile implements ConfigFile, RepositoryChangeListener {
  private static final Logger logger = DeferredLoggerFactory.getLogger(AbstractConfigFile.class);
  private static ExecutorService m_executorService; // ExecutorService 对象，用于配置变化时，异步通知 ConfigChangeListener 监听器们。静态属性，所有 Config 共享该线程池。
  protected final ConfigRepository m_configRepository;
  protected final String m_namespace; // Namespace 的名字
  protected final AtomicReference<Properties> m_configProperties; // 配置 Properties 的缓存引用
  private final List<ConfigFileChangeListener> m_listeners = Lists.newCopyOnWriteArrayList(); // ConfigChangeListener 集合
  protected final PropertiesFactory propertiesFactory;

  private volatile ConfigSourceType m_sourceType = ConfigSourceType.NONE;

  static {
    m_executorService = Executors.newCachedThreadPool(ApolloThreadFactory
        .create("ConfigFile", true));
  }

  public AbstractConfigFile(String namespace, ConfigRepository configRepository) {
    m_configRepository = configRepository;
    m_namespace = namespace;
    m_configProperties = new AtomicReference<>();
    propertiesFactory = ApolloInjector.getInstance(PropertiesFactory.class);
    initialize(); // 初始化
  }

  private void initialize() {
    try {
      m_configProperties.set(m_configRepository.getConfig()); // 初始化 m_configProperties
      m_sourceType = m_configRepository.getSourceType();
    } catch (Throwable ex) {
      Tracer.logError(ex);
      logger.warn("Init Apollo Config File failed - namespace: {}, reason: {}.",
          m_namespace, ExceptionUtil.getDetailMessage(ex));
    } finally {
      //register the change listener no matter config repository is working or not
      //so that whenever config repository is recovered, config could get changed
      m_configRepository.addChangeListener(this); // // 注册到 ConfigRepository 中，从而实现每次配置发生变更时，更新配置缓存 m_configProperties
    }
  }
  // 获得 Namespace 名字
  @Override
  public String getNamespace() {
    return m_namespace;
  }

  protected abstract void update(Properties newProperties);
  // 当 ConfigRepository 读取到配置发生变更时，计算配置变更集合，并通知监听器们
  @Override
  public synchronized void onRepositoryChange(String namespace, Properties newProperties) {
    if (newProperties.equals(m_configProperties.get())) { // 忽略，若未变更
      return;
    }
    Properties newConfigProperties = propertiesFactory.getPropertiesInstance(); // 读取新的 Properties 对象
    newConfigProperties.putAll(newProperties);
    // 获得【旧】值
    String oldValue = getContent();
    // 更新为【新】值
    update(newProperties); // 抽象方法，更新为【新】值。该方法需要子类自己去实现
    m_sourceType = m_configRepository.getSourceType();
    // 获得新值
    String newValue = getContent();
    // 计算变化类型
    PropertyChangeType changeType = PropertyChangeType.MODIFIED;

    if (oldValue == null) {
      changeType = PropertyChangeType.ADDED;
    } else if (newValue == null) {
      changeType = PropertyChangeType.DELETED;
    }
    // 通知监听器们
    this.fireConfigChange(new ConfigFileChangeEvent(m_namespace, oldValue, newValue, changeType));

    Tracer.logEvent("Apollo.Client.ConfigChanges", m_namespace);
  }

  @Override
  public void addChangeListener(ConfigFileChangeListener listener) {
    if (!m_listeners.contains(listener)) {
      m_listeners.add(listener);
    }
  }

  @Override
  public boolean removeChangeListener(ConfigFileChangeListener listener) {
    return m_listeners.remove(listener);
  }

  @Override
  public ConfigSourceType getSourceType() {
    return m_sourceType;
  }
  // 触发配置变更监听器们
  private void fireConfigChange(final ConfigFileChangeEvent changeEvent) {
    for (final ConfigFileChangeListener listener : m_listeners) {
      m_executorService.submit(new Runnable() {
        @Override
        public void run() {
          String listenerName = listener.getClass().getName();
          Transaction transaction = Tracer.newTransaction("Apollo.ConfigFileChangeListener", listenerName);
          try {
            listener.onChange(changeEvent); // 通知监听器
            transaction.setStatus(Transaction.SUCCESS);
          } catch (Throwable ex) {
            transaction.setStatus(ex);
            Tracer.logError(ex);
            logger.error("Failed to invoke config file change listener {}", listenerName, ex);
          } finally {
            transaction.complete();
          }
        }
      });
    }
  }
}
