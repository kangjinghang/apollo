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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.NamespaceHandlerSupport;
import org.springframework.core.Ordered;
import org.springframework.util.SystemPropertyUtils;
import org.w3c.dom.Element;

import com.ctrip.framework.apollo.core.ConfigConsts;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class NamespaceHandler extends NamespaceHandlerSupport {

  private static final Splitter NAMESPACE_SPLITTER = Splitter.on(",").omitEmptyStrings()
      .trimResults();

  @Override
  public void init() {
    registerBeanDefinitionParser("config", new BeanParser());
  }

  static class BeanParser extends AbstractSingleBeanDefinitionParser {
    // 返回 Bean 对应的类为 ConfigPropertySourcesProcessor
    @Override
    protected Class<?> getBeanClass(Element element) {
      return ConfigPropertySourcesProcessor.class;
    }

    @Override
    protected boolean shouldGenerateId() {
      return true;
    }

    private String resolveNamespaces(Element element) {
      String namespaces = element.getAttribute("namespaces");
      if (Strings.isNullOrEmpty(namespaces)) {
        //default to application
        return ConfigConsts.NAMESPACE_APPLICATION;
      }
      return SystemPropertyUtils.resolvePlaceholders(namespaces);
    }
    // 解析 XML 配置
    @Override
    protected void doParse(Element element, BeanDefinitionBuilder builder) {
      String namespaces = this.resolveNamespaces(element); // 解析 namespaces 属性，默认为 "application"

      int order = Ordered.LOWEST_PRECEDENCE;
      String orderAttribute = element.getAttribute("order"); // 解析 order 属性，默认为 Ordered.LOWEST_PRECEDENCE

      if (!Strings.isNullOrEmpty(orderAttribute)) {
        try {
          order = Integer.parseInt(orderAttribute);
        } catch (Throwable ex) {
          throw new IllegalArgumentException(
              String.format("Invalid order: %s for namespaces: %s", orderAttribute, namespaces));
        }
      }
      PropertySourcesProcessor.addNamespaces(NAMESPACE_SPLITTER.splitToList(namespaces), order); // 添加到 PropertySourcesProcessor
    }
  }
}
