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
package com.ctrip.framework.apollo.common.entity;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import java.time.Instant;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreRemove;
import javax.persistence.PreUpdate;

@MappedSuperclass
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
public abstract class BaseEntity { // 基础实体抽象类

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "Id")
  private long id; // 编号

  @Column(name = "IsDeleted", columnDefinition = "Bit default '0'")
  protected boolean isDeleted = false; // 是否删除

  @Column(name = "DeletedAt", columnDefinition = "Bigint default '0'")
  protected long deletedAt; // 数据删除时间

  @Column(name = "DataChange_CreatedBy", nullable = false)
  private String dataChangeCreatedBy; // 数据创建人，例如在 Portal 系统中，使用系统的管理员账号，即 UserPO.username 字段

  @Column(name = "DataChange_CreatedTime", nullable = false)
  private Date dataChangeCreatedTime; //  数据创建时间

  @Column(name = "DataChange_LastModifiedBy")
  private String dataChangeLastModifiedBy; // 数据最后更新人，例如在 Portal 系统中，使用系统的管理员账号，即 UserPO.username 字段

  @Column(name = "DataChange_LastTime")
  private Date dataChangeLastModifiedTime; // 数据最后更新时间

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public boolean isDeleted() {
    return isDeleted;
  }

  public void setDeleted(boolean deleted) {
    isDeleted = deleted;
    // also set deletedAt value as epoch millisecond
    this.deletedAt = System.currentTimeMillis();
  }

  public long getDeletedAt() {
    return deletedAt;
  }

  public void setDeletedAt(long deletedAt) {
    this.deletedAt = deletedAt;
  }

  public String getDataChangeCreatedBy() {
    return dataChangeCreatedBy;
  }

  public void setDataChangeCreatedBy(String dataChangeCreatedBy) {
    this.dataChangeCreatedBy = dataChangeCreatedBy;
  }

  public Date getDataChangeCreatedTime() {
    return dataChangeCreatedTime;
  }

  public void setDataChangeCreatedTime(Date dataChangeCreatedTime) {
    this.dataChangeCreatedTime = dataChangeCreatedTime;
  }

  public String getDataChangeLastModifiedBy() {
    return dataChangeLastModifiedBy;
  }

  public void setDataChangeLastModifiedBy(String dataChangeLastModifiedBy) {
    this.dataChangeLastModifiedBy = dataChangeLastModifiedBy;
  }

  public Date getDataChangeLastModifiedTime() {
    return dataChangeLastModifiedTime;
  }

  public void setDataChangeLastModifiedTime(Date dataChangeLastModifiedTime) {
    this.dataChangeLastModifiedTime = dataChangeLastModifiedTime;
  }

  @PrePersist
  protected void prePersist() { // 保存前置方法
    if (this.dataChangeCreatedTime == null) {
        dataChangeCreatedTime = new Date();
    }
    if (this.dataChangeLastModifiedTime == null) {
        dataChangeLastModifiedTime = new Date();
    }
  }

  @PreUpdate
  protected void preUpdate() { // 更新前置方法
    this.dataChangeLastModifiedTime = new Date();
  }

  @PreRemove
  protected void preRemove() { // 删除前置方法
    this.dataChangeLastModifiedTime = new Date();
  }

  protected ToStringHelper toStringHelper() {
    return MoreObjects.toStringHelper(this).omitNullValues().add("id", id)
        .add("dataChangeCreatedBy", dataChangeCreatedBy)
        .add("dataChangeCreatedTime", dataChangeCreatedTime)
        .add("dataChangeLastModifiedBy", dataChangeLastModifiedBy)
        .add("dataChangeLastModifiedTime", dataChangeLastModifiedTime);
  }

  public String toString(){
    return toStringHelper().toString();
  }
}
