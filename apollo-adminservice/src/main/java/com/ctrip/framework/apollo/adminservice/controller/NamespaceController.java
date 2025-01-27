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
package com.ctrip.framework.apollo.adminservice.controller;

import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.service.NamespaceService;
import com.ctrip.framework.apollo.common.dto.NamespaceDTO;
import com.ctrip.framework.apollo.common.dto.PageDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.exception.NotFoundException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.List;
import java.util.Map;

@RestController
public class NamespaceController {

  private final NamespaceService namespaceService;

  public NamespaceController(final NamespaceService namespaceService) {
    this.namespaceService = namespaceService;
  }
  // 创建 Namespace
  @PostMapping("/apps/{appId}/clusters/{clusterName}/namespaces")
  public NamespaceDTO create(@PathVariable("appId") String appId,
                             @PathVariable("clusterName") String clusterName,
                             @Valid @RequestBody NamespaceDTO dto) { // 校验 NamespaceDTO 的 namespaceName 格式正确。
    Namespace entity = BeanUtils.transform(Namespace.class, dto); // 将 NamespaceDTO 转换成 Namespace 对象
    Namespace managedEntity = namespaceService.findOne(appId, clusterName, entity.getNamespaceName());
    if (managedEntity != null) { // 判断 name 在 Cluster 下是否已经存在对应的 Namespace 对象。若已经存在，抛出 BadRequestException 异常
      throw new BadRequestException("namespace already exist.");
    }
    // 保存 Namespace 对象
    entity = namespaceService.save(entity);
    // 将保存的 Namespace 对象转换成 NamespaceDTO
    return BeanUtils.transform(NamespaceDTO.class, entity);
  }

  @DeleteMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName:.+}")
  public void delete(@PathVariable("appId") String appId,
                     @PathVariable("clusterName") String clusterName,
                     @PathVariable("namespaceName") String namespaceName, @RequestParam String operator) {
    Namespace entity = namespaceService.findOne(appId, clusterName, namespaceName);
    if (entity == null) {
      throw new NotFoundException("namespace not found for %s %s %s", appId, clusterName,
          namespaceName);
    }

    namespaceService.deleteNamespace(entity, operator);
  }

  @GetMapping("/apps/{appId}/clusters/{clusterName}/namespaces")
  public List<NamespaceDTO> find(@PathVariable("appId") String appId,
                                 @PathVariable("clusterName") String clusterName) {
    List<Namespace> groups = namespaceService.findNamespaces(appId, clusterName);
    return BeanUtils.batchTransform(NamespaceDTO.class, groups);
  }

  @GetMapping("/namespaces/{namespaceId}")
  public NamespaceDTO get(@PathVariable("namespaceId") Long namespaceId) {
    Namespace namespace = namespaceService.findOne(namespaceId);
    if (namespace == null) {
        throw new NotFoundException("namespace not found for %s", namespaceId);
    }
    return BeanUtils.transform(NamespaceDTO.class, namespace);
  }

  /**
   * the returned content's size is not fixed. so please carefully used.
   */
  @GetMapping("/namespaces/find-by-item")
  public PageDTO<NamespaceDTO> findByItem(@RequestParam String itemKey, Pageable pageable) {
    Page<Namespace> namespacePage = namespaceService.findByItem(itemKey, pageable);

    List<NamespaceDTO> namespaceDTOS = BeanUtils.batchTransform(NamespaceDTO.class, namespacePage.getContent());

    return new PageDTO<>(namespaceDTOS, pageable, namespacePage.getTotalElements());
  }

  @GetMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName:.+}")
  public NamespaceDTO get(@PathVariable("appId") String appId,
                          @PathVariable("clusterName") String clusterName,
                          @PathVariable("namespaceName") String namespaceName) {
    Namespace namespace = namespaceService.findOne(appId, clusterName, namespaceName);
    if (namespace == null) {
      throw new NotFoundException("namespace not found for %s %s %s", appId, clusterName,
          namespaceName);
    }
    return BeanUtils.transform(NamespaceDTO.class, namespace);
  }

  @GetMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/associated-public-namespace")
  public NamespaceDTO findPublicNamespaceForAssociatedNamespace(@PathVariable String appId,
                                                                @PathVariable String clusterName,
                                                                @PathVariable String namespaceName) {
    Namespace namespace = namespaceService.findPublicNamespaceForAssociatedNamespace(clusterName, namespaceName);

    if (namespace == null) {
      throw new NotFoundException("public namespace not found. namespace:%s", namespaceName);
    }

    return BeanUtils.transform(NamespaceDTO.class, namespace);
  }

  /**
   * cluster -> cluster has not published namespaces?
   */
  @GetMapping("/apps/{appId}/namespaces/publish_info")
  public Map<String, Boolean> namespacePublishInfo(@PathVariable String appId) {
    return namespaceService.namespacePublishInfo(appId);
  }


}
