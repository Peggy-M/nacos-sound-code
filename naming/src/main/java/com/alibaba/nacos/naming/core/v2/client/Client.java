/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.core.v2.client;

import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.pojo.Subscriber;

import java.util.Collection;

/**
 * Nacos naming client.
 *
 * <p>The abstract concept of the client stored by on the server of Nacos naming module. It is used to store which
 * services the client has published and subscribed.
 *
 * @author xiweng.yy
 */
public interface Client {
    
    /**
     * Get the unique id of current client.
     *
     * @return id of client
     */
    //获取当前客户端的唯一 Id
    String getClientId();
    
    /**
     * Whether is ephemeral of current client.
     *
     * @return true if client is ephemeral, otherwise false
     */
    //当前客户的是否短暂的
    boolean isEphemeral();
    
    /**
     * Set the last time for updating current client as current time.
     */
    //设置该客户端的最后更新时间为当前的时间
    void setLastUpdatedTime();
    
    /**
     * Get the last time for updating current client.
     *
     * @return last time for updating
     */
    //获取最近一次客户更新的时间
    long getLastUpdatedTime();
    
    /**
     * Add a new instance for service for current client.
     *
     * @param service             publish service
     * @param instancePublishInfo instance
     * @return true if add successfully, otherwise false
     */
    //为当前客户端添加服务的新实例
    boolean addServiceInstance(Service service, InstancePublishInfo instancePublishInfo);
    
    /**
     * Remove service instance from client.
     *
     * @param service service of instance
     * @return instance info if exist, otherwise {@code null}
     */
    //从客户端删除服务实例
    InstancePublishInfo removeServiceInstance(Service service);
    
    /**
     * Get instance info of service from client.
     *
     * @param service service of instance
     * @return instance info
     */
    //从客户端获取实例的服务信息
    InstancePublishInfo getInstancePublishInfo(Service service);
    
    /**
     * Get all published service of current client.
     *
     * @return published services
     */
    //获取当前客户端已经发布的所有信息
    Collection<Service> getAllPublishedService();
    
    /**
     * Add a new subscriber for target service.
     *
     * @param service    subscribe service
     * @param subscriber subscriber
     * @return true if add successfully, otherwise false
     */
    //为目标服务添加新的订阅者
    boolean addServiceSubscriber(Service service, Subscriber subscriber);
    
    /**
     * Remove subscriber for service.
     *
     * @param service service of subscriber
     * @return true if remove successfully, otherwise false
     */
    //删除服务的订阅者
    boolean removeServiceSubscriber(Service service);
    
    /**
     * Get subscriber of service from client.
     *
     * @param service service of subscriber
     * @return subscriber
     */
    //从客户端获取服务的订阅者
    Subscriber getSubscriber(Service service);
    
    /**
     * Get all subscribe service of current client.
     *
     * @return subscribe services
     */
    //获取当前客户的所有订阅服务
    Collection<Service> getAllSubscribeService();
    
    /**
     * Generate sync data.
     *
     * @return sync data
     */
    ClientSyncData generateSyncData();
    
    /**
     * Whether current client is expired.
     *
     * @param currentTime unified current timestamp
     * @return true if client has expired, otherwise false
     */
    boolean isExpire(long currentTime);
    
    /**
     * Release current client and release resources if neccessary.
     */
    void release();
}
