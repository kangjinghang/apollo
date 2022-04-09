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
package com.ctrip.framework.apollo.spring.property;

import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.ctrip.framework.apollo.spring.util.SpringInjector;
import com.google.gson.Gson;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.TypeConverter;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.env.Environment;
import org.springframework.util.CollectionUtils;

/** 自动更新配置监听器
 * Create by zhangzheng on 2018/3/6
 */
public class AutoUpdateConfigChangeListener implements ConfigChangeListener{
  private static final Logger logger = LoggerFactory.getLogger(AutoUpdateConfigChangeListener.class);
  // 是否带上 Field 参数，因为 Spring 3.2.0+ 才有该方法
  private final boolean typeConverterHasConvertIfNecessaryWithFieldParameter;
  private final Environment environment;
  private final ConfigurableBeanFactory beanFactory;
  private final TypeConverter typeConverter;
  private final PlaceholderHelper placeholderHelper;
  private final SpringValueRegistry springValueRegistry;
  private final Gson gson;

  public AutoUpdateConfigChangeListener(Environment environment, ConfigurableListableBeanFactory beanFactory){
    this.typeConverterHasConvertIfNecessaryWithFieldParameter = testTypeConverterHasConvertIfNecessaryWithFieldParameter();
    this.beanFactory = beanFactory;
    this.typeConverter = this.beanFactory.getTypeConverter();
    this.environment = environment;
    this.placeholderHelper = SpringInjector.getInstance(PlaceholderHelper.class);
    this.springValueRegistry = SpringInjector.getInstance(SpringValueRegistry.class);
    this.gson = new Gson();
  }

  @Override
  public void onChange(ConfigChangeEvent changeEvent) {
    Set<String> keys = changeEvent.changedKeys();  // 获得更新的 KEY 集合
    if (CollectionUtils.isEmpty(keys)) {
      return;
    }
    for (String key : keys) {  // 循环 KEY 集合，更新 StringValue
      // 1. check whether the changed key is relevant // 忽略，若不在 SpringValueRegistry 中
      Collection<SpringValue> targetValues = springValueRegistry.get(beanFactory, key);
      if (targetValues == null || targetValues.isEmpty()) {
        continue;
      }

      // 2. update the value // 循环，更新 SpringValue
      for (SpringValue val : targetValues) {
        updateSpringValue(val);
      }
    }
  }

  private void updateSpringValue(SpringValue springValue) {
    try {
      Object value = resolvePropertyValue(springValue);  // 解析值，返回正确类型的
      springValue.update(value); // 更新 StringValue

      logger.info("Auto update apollo changed value successfully, new value: {}, {}", value,
          springValue);
    } catch (Throwable ex) {
      logger.error("Auto update apollo changed value failed, {}", springValue.toString(), ex);
    }
  }

  /**
   * Logic transplanted from DefaultListableBeanFactory
   * @see org.springframework.beans.factory.support.DefaultListableBeanFactory#doResolveDependency(org.springframework.beans.factory.config.DependencyDescriptor, java.lang.String, java.util.Set, org.springframework.beans.TypeConverter)
   */
  private Object resolvePropertyValue(SpringValue springValue) {
    // value will never be null, as @Value and @ApolloJsonValue will not allow that
    Object value = placeholderHelper
        .resolvePropertyValue(beanFactory, springValue.getBeanName(), springValue.getPlaceholder());

    if (springValue.isJson()) {  // 如果值数据结构是 JSON 类型，则使用 Gson 解析成对应值的类型
      value = parseJsonValue((String)value, springValue.getGenericType());
    } else {
      if (springValue.isField()) { // 如果类型为 Field
        // org.springframework.beans.TypeConverter#convertIfNecessary(java.lang.Object, java.lang.Class, java.lang.reflect.Field) is available from Spring 3.2.0+
        if (typeConverterHasConvertIfNecessaryWithFieldParameter) {
          value = this.typeConverter
              .convertIfNecessary(value, springValue.getTargetType(), springValue.getField());
        } else {
          value = this.typeConverter.convertIfNecessary(value, springValue.getTargetType());
        }
      } else { // 如果类型为 Method
        value = this.typeConverter.convertIfNecessary(value, springValue.getTargetType(),
            springValue.getMethodParameter());
      }
    }

    return value;
  }

  private Object parseJsonValue(String json, Type targetType) {
    try {
      return gson.fromJson(json, targetType);
    } catch (Throwable ex) {
      logger.error("Parsing json '{}' to type {} failed!", json, targetType, ex);
      throw ex;
    }
  }
  // 是否使用带 Field 方法参数的 TypeConverter#convertIfNecessary(Object, Class, Field) 方法，因为 Spring 3.2.0+ 才有该方法
  private boolean testTypeConverterHasConvertIfNecessaryWithFieldParameter() {
    try {
      TypeConverter.class.getMethod("convertIfNecessary", Object.class, Class.class, Field.class);
    } catch (Throwable ex) {
      return false;
    }

    return true;
  }
}
