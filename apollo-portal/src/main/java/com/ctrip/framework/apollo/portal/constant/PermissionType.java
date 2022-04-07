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
package com.ctrip.framework.apollo.portal.constant;

public interface PermissionType {

  /**
   * system level permission
   */
  String CREATE_APPLICATION = "CreateApplication"; // 创建 Application
  String MANAGE_APP_MASTER = "ManageAppMaster"; // 管理 Application

  /**
   * APP level permission
   */
  // 创建 Namespace
  String CREATE_NAMESPACE = "CreateNamespace";
  // 创建 Cluster
  String CREATE_CLUSTER = "CreateCluster";

  /**
   * 分配用户权限的权限
   */
  String ASSIGN_ROLE = "AssignRole";

  /**
   * namespace level permission
   */
  // 修改 Namespace
  String MODIFY_NAMESPACE = "ModifyNamespace";
  // 发布 Namespace
  String RELEASE_NAMESPACE = "ReleaseNamespace";


}
