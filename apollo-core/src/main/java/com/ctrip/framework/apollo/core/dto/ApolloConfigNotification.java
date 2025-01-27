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
package com.ctrip.framework.apollo.core.dto;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class ApolloConfigNotification {
  private String namespaceName; // Namespace 名字
  private long notificationId; // 最新通知编号，目前使用 ReleaseMessage.id
  private volatile ApolloNotificationMessages messages; // 通知消息集合

  //for json converter
  public ApolloConfigNotification() {
  }

  public ApolloConfigNotification(String namespaceName, long notificationId) {
    this.namespaceName = namespaceName;
    this.notificationId = notificationId;
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  public long getNotificationId() {
    return notificationId;
  }

  public void setNamespaceName(String namespaceName) {
    this.namespaceName = namespaceName;
  }

  public ApolloNotificationMessages getMessages() {
    return messages;
  }

  public void setMessages(ApolloNotificationMessages messages) {
    this.messages = messages;
  }

  public void addMessage(String key, long notificationId) {
    if (this.messages == null) {
      synchronized (this) {
        if (this.messages == null) {
          this.messages = new ApolloNotificationMessages(); // 创建 ApolloNotificationMessages 对象
        }
      }
    }
    this.messages.put(key, notificationId); // 添加到 messages 中
  }

  @Override
  public String toString() {
    return "ApolloConfigNotification{" +
        "namespaceName='" + namespaceName + '\'' +
        ", notificationId=" + notificationId +
        '}';
  }
}
