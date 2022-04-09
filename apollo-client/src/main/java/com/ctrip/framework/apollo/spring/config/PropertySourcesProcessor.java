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
package com.ctrip.framework.apollo.spring.config;

import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.spring.property.AutoUpdateConfigChangeListener;
import com.ctrip.framework.apollo.spring.util.SpringInjector;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;

import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.env.CompositePropertySource;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;

import java.util.Collection;
import java.util.Iterator;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;

/**
 * Apollo Property Sources processor for Spring Annotation Based Application. <br /> <br />
 *
 * The reason why PropertySourcesProcessor implements {@link BeanFactoryPostProcessor} instead of
 * {@link org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor} is that lower versions of
 * Spring (e.g. 3.1.1) doesn't support registering BeanDefinitionRegistryPostProcessor in ImportBeanDefinitionRegistrar
 * - {@link com.ctrip.framework.apollo.spring.annotation.ApolloConfigRegistrar}
 *
 * @author Jason Song(song_s@ctrip.com)
 */
public class PropertySourcesProcessor implements BeanFactoryPostProcessor, EnvironmentAware, PriorityOrdered {
  private static final Multimap<Integer, String> NAMESPACE_NAMES = LinkedHashMultimap.create(); // Namespace 名字集合。KEY：优先级。VALUE：Namespace 名字集合
  private static final Set<BeanFactory> AUTO_UPDATE_INITIALIZED_BEAN_FACTORIES = Sets.newConcurrentHashSet();
  // ConfigPropertySource 工厂。在 NAMESPACE_NAMES 中的每一个 Namespace ，都会创建成对应的 ConfigPropertySource 对象( 基于 Apollo Config 的 PropertySource 实现类 )，并添加到 environment 中
  private final ConfigPropertySourceFactory configPropertySourceFactory = SpringInjector
      .getInstance(ConfigPropertySourceFactory.class);
  private ConfigUtil configUtil;
  private ConfigurableEnvironment environment; // Spring ConfigurableEnvironment 对象

  public static boolean addNamespaces(Collection<String> namespaces, int order) {
    return NAMESPACE_NAMES.putAll(order, namespaces);
  }

  @Override
  public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
    this.configUtil = ApolloInjector.getInstance(ConfigUtil.class);
    initializePropertySources(); // 初始化 PropertySource 们
    initializeAutoUpdatePropertiesFeature(beanFactory); // 初始化 AutoUpdateConfigChangeListener 对象，实现属性的自动更新
  }
  // 初始化 PropertySource 们
  private void initializePropertySources() {
    if (environment.getPropertySources().contains(PropertySourcesConstants.APOLLO_PROPERTY_SOURCE_NAME)) {
      //already initialized  // 若 environment 已经有 APOLLO_PROPERTY_SOURCE_NAME 属性源，说明已经初始化，直接返回。
      return;
    }
    CompositePropertySource composite; // 创建 CompositePropertySource 对象，组合多个 Namespace 的 ConfigPropertySource
    if (configUtil.isPropertyNamesCacheEnabled()) {
      composite = new CachedCompositePropertySource(PropertySourcesConstants.APOLLO_PROPERTY_SOURCE_NAME);
    } else {
      composite = new CompositePropertySource(PropertySourcesConstants.APOLLO_PROPERTY_SOURCE_NAME);
    }

    //sort by order asc // 按照优先级，顺序遍历 Namespace
    ImmutableSortedSet<Integer> orders = ImmutableSortedSet.copyOf(NAMESPACE_NAMES.keySet());
    Iterator<Integer> iterator = orders.iterator();

    while (iterator.hasNext()) {
      int order = iterator.next();
      for (String namespace : NAMESPACE_NAMES.get(order)) {
        Config config = ConfigService.getConfig(namespace);  // 创建 Apollo Config 对象
        // 创建 Namespace 对应的 ConfigPropertySource 对象添加到 composite 中
        composite.addPropertySource(configPropertySourceFactory.getConfigPropertySource(namespace, config));
      }
    }

    // clean up
    NAMESPACE_NAMES.clear();
    // 若有 APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME 属性源，添加到其后
    // add after the bootstrap property source or to the first
    if (environment.getPropertySources()
        .contains(PropertySourcesConstants.APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME)) {

      // ensure ApolloBootstrapPropertySources is still the first
      ensureBootstrapPropertyPrecedence(environment);

      environment.getPropertySources()
          .addAfter(PropertySourcesConstants.APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME, composite);
    } else { // 若没 APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME 属性源，添加到首个
      environment.getPropertySources().addFirst(composite);
    }
  }

  private void ensureBootstrapPropertyPrecedence(ConfigurableEnvironment environment) {
    MutablePropertySources propertySources = environment.getPropertySources();

    PropertySource<?> bootstrapPropertySource = propertySources
        .get(PropertySourcesConstants.APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME);

    // not exists or already in the first place
    if (bootstrapPropertySource == null || propertySources.precedenceOf(bootstrapPropertySource) == 0) {
      return;
    }

    propertySources.remove(PropertySourcesConstants.APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME);
    propertySources.addFirst(bootstrapPropertySource);
  }
  // 初始化 AutoUpdateConfigChangeListener 对象，实现 Spring Placeholder 的自动更新功能
  private void initializeAutoUpdatePropertiesFeature(ConfigurableListableBeanFactory beanFactory) {
    if (!configUtil.isAutoUpdateInjectedSpringPropertiesEnabled() ||
        !AUTO_UPDATE_INITIALIZED_BEAN_FACTORIES.add(beanFactory)) { // 若未开启属性的自动更新功能，直接返回
      return;
    }
    // 创建 AutoUpdateConfigChangeListener 对象
    AutoUpdateConfigChangeListener autoUpdateConfigChangeListener = new AutoUpdateConfigChangeListener(
        environment, beanFactory);
    // 循环，向 ConfigPropertySource 注册配置变更器
    List<ConfigPropertySource> configPropertySources = configPropertySourceFactory.getAllConfigPropertySources();
    for (ConfigPropertySource configPropertySource : configPropertySources) {
      configPropertySource.addChangeListener(autoUpdateConfigChangeListener);
    }
  }

  @Override
  public void setEnvironment(Environment environment) {
    //it is safe enough to cast as all known environment is derived from ConfigurableEnvironment
    this.environment = (ConfigurableEnvironment) environment;
  }

  @Override
  public int getOrder() {
    //make it as early as possible
    return Ordered.HIGHEST_PRECEDENCE; // 最高优先级
  }

  // for test only
  static void reset() {
    NAMESPACE_NAMES.clear();
    AUTO_UPDATE_INITIALIZED_BEAN_FACTORIES.clear();
  }
}
