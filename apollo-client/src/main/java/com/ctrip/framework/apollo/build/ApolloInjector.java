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
package com.ctrip.framework.apollo.build;

import com.ctrip.framework.apollo.exceptions.ApolloConfigException;
import com.ctrip.framework.apollo.internals.Injector;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.foundation.internals.ServiceBootstrap;

/** Apollo 注入器，实现依赖注入( DI，全称“Dependency Injection” )
 * @author Jason Song(song_s@ctrip.com)
 */
public class ApolloInjector {
  private static volatile Injector s_injector; // 注入器
  private static final Object lock = new Object(); // 锁

  private static Injector getInjector() {
    if (s_injector == null) {  // 若 Injector 不存在，则进行获得
      synchronized (lock) {
        if (s_injector == null) { // 若 Injector 不存在，则进行获得
          try {
            s_injector = ServiceBootstrap.loadFirst(Injector.class); // 基于 JDK SPI 加载对应的 Injector 实现对象
          } catch (Throwable ex) {
            ApolloConfigException exception = new ApolloConfigException("Unable to initialize Apollo Injector!", ex);
            Tracer.logError(exception);
            throw exception;
          }
        }
      }
    }

    return s_injector;
  }

  public static <T> T getInstance(Class<T> clazz) {
    try {
      return getInjector().getInstance(clazz);
    } catch (Throwable ex) {
      Tracer.logError(ex);
      throw new ApolloConfigException(String.format("Unable to load instance for type %s!", clazz.getName()), ex);
    }
  }

  public static <T> T getInstance(Class<T> clazz, String name) {
    try {
      return getInjector().getInstance(clazz, name);
    } catch (Throwable ex) {
      Tracer.logError(ex);
      throw new ApolloConfigException(
          String.format("Unable to load instance for type %s and name %s !", clazz.getName(), name), ex);
    }
  }
}
