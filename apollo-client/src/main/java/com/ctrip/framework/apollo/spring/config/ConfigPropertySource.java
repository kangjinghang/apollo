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

import com.ctrip.framework.apollo.ConfigChangeListener;
import java.util.Set;

import org.springframework.core.env.EnumerablePropertySource;

import com.ctrip.framework.apollo.Config;

/**
 * Property source wrapper for Config。基于 Apollo Config 的 PropertySource 实现类
 *
 * @author Jason Song(song_s@ctrip.com)
 */
public class ConfigPropertySource extends EnumerablePropertySource<Config> {
  private static final String[] EMPTY_ARRAY = new String[0];

  ConfigPropertySource(String name, Config source) {  // 此处的 Apollo Config 作为 source
    super(name, source);
  }

  @Override
  public boolean containsProperty(String name) {
    return this.source.getProperty(name, null) != null;
  }

  @Override
  public String[] getPropertyNames() {
    Set<String> propertyNames = this.source.getPropertyNames(); // 从 Config 中，获得属性名集合
    if (propertyNames.isEmpty()) {
      return EMPTY_ARRAY;
    }
    return propertyNames.toArray(new String[propertyNames.size()]); // 转换成 String 数组，返回
  }

  @Override
  public Object getProperty(String name) {
    return this.source.getProperty(name, null);
  }
  // 添加 ConfigChangeListener 到 Config 中
  public void addChangeListener(ConfigChangeListener listener) {
    this.source.addChangeListener(listener);
  }
}
