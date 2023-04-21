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

package com.alibaba.nacos.client.naming.core;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.remote.NamingClientProxy;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Service information update service.
 *
 * @author xiweng.yy
 */
public class ServiceInfoUpdateService implements Closeable {
    
    private static final long DEFAULT_DELAY = 1000L;
    
    private static final int DEFAULT_UPDATE_CACHE_TIME_MULTIPLE = 6;
    
    private final Map<String, ScheduledFuture<?>> futureMap = new HashMap<String, ScheduledFuture<?>>();
    
    private final ServiceInfoHolder serviceInfoHolder;
    
    private final ScheduledExecutorService executor;
    
    private final NamingClientProxy namingClientProxy;
    
    private final InstancesChangeNotifier changeNotifier;
    
    public ServiceInfoUpdateService(Properties properties, ServiceInfoHolder serviceInfoHolder,
            NamingClientProxy namingClientProxy, InstancesChangeNotifier changeNotifier) {
        this.executor = new ScheduledThreadPoolExecutor(initPollingThreadCount(properties),
                new NameThreadFactory("com.alibaba.nacos.client.naming.updater"));
        this.serviceInfoHolder = serviceInfoHolder;
        this.namingClientProxy = namingClientProxy;
        this.changeNotifier = changeNotifier;
    }
    
    private int initPollingThreadCount(Properties properties) {
        if (properties == null) {
            return UtilAndComs.DEFAULT_POLLING_THREAD_COUNT;
        }
        return ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.NAMING_POLLING_THREAD_COUNT),
                UtilAndComs.DEFAULT_POLLING_THREAD_COUNT);
    }
    
    /**
     * Schedule update if absent.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters
     */
    public void scheduleUpdateIfAbsent(String serviceName, String groupName, String clusters) {
        String serviceKey = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        if (futureMap.get(serviceKey) != null) {
            return;
        }
        //双重检测锁
        synchronized (futureMap) {
            if (futureMap.get(serviceKey) != null) {
                return;
            }
            //构建一个定时处理的任务,最终这里的 future 就是构建的定时任务,该任务用于在 run 中执行
            ScheduledFuture<?> future = addTask(new UpdateTask(serviceName, groupName, clusters));
            futureMap.put(serviceKey, future);
        }
    }
    
    private synchronized ScheduledFuture<?> addTask(UpdateTask task) {
        //执行延时函数,延时时间为 1000L * MICRO_SCALE = 1S
        return executor.schedule(task, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Stop to schedule update if contain task.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters
     */
    public void stopUpdateIfContain(String serviceName, String groupName, String clusters) {
        String serviceKey = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        if (!futureMap.containsKey(serviceKey)) {
            return;
        }
        synchronized (futureMap) {
            if (!futureMap.containsKey(serviceKey)) {
                return;
            }
            futureMap.remove(serviceKey);
        }
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executor, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
    
    public class UpdateTask implements Runnable {
        
        long lastRefTime = Long.MAX_VALUE;
        
        private final String serviceName;
        
        private final String groupName;
        
        private final String clusters;
        
        private final String groupedServiceName;
        
        private final String serviceKey;
    
        /**
         * the fail situation. 1:can't connect to server 2:serviceInfo's hosts is empty
         */
        private int failCount = 0;
        
        public UpdateTask(String serviceName, String groupName, String clusters) {
            this.serviceName = serviceName;
            this.groupName = groupName;
            this.clusters = clusters;
            this.groupedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
            this.serviceKey = ServiceInfo.getKey(groupedServiceName, clusters);
        }
        
        @Override
        public void run() {
            long delayTime = DEFAULT_DELAY;
            
            try {
                //判断更改通知对象 serviceName 是否订阅
                if (!changeNotifier.isSubscribed(groupName, serviceName, clusters) && !futureMap.containsKey(serviceKey)) {
                    NAMING_LOGGER
                            .info("update task is stopped, service:" + groupedServiceName + ", clusters:" + clusters);
                    return;
                }
                //获取缓存中的信息
                ServiceInfo serviceObj = serviceInfoHolder.getServiceInfoMap().get(serviceKey);
                //缓存为空
                if (serviceObj == null) {
                    //生成一个服务实例对象
                    serviceObj = namingClientProxy.queryInstancesOfService(serviceName, groupName, clusters, 0, false);
                    //处理更新或添加到本地的缓存当中
                    serviceInfoHolder.processServiceInfo(serviceObj);
                    //更新最后一次的时间
                    lastRefTime = serviceObj.getLastRefTime();
                    return;
                }
                //过期服务，如果说，服务的更新时间是小于等于缓存刷新的时间的
                //那就说明本地的缓存不是最新的,而当前的服务实例信息也不是客户端最新的,
                //这个时候就需要从 注册中心 中重新的进行一次查询，获取最的服务实例信息并更新本地缓存
                if (serviceObj.getLastRefTime() <= lastRefTime) {
                    serviceObj = namingClientProxy.queryInstancesOfService(serviceName, groupName, clusters, 0, false);
                    //更新处理本地的缓存
                    serviceInfoHolder.processServiceInfo(serviceObj);
                }
                //刷新更新的当前时间
                lastRefTime = serviceObj.getLastRefTime();
                if (CollectionUtils.isEmpty(serviceObj.getHosts())) {
                    incFailCount();
                    return;
                }
                //下次的更新缓存时间设置为缓存中的默认基数 (cacheMillis = 1000) * 6
                // TODO multiple time can be configured.
                delayTime = serviceObj.getCacheMillis() * DEFAULT_UPDATE_CACHE_TIME_MULTIPLE;
                // 重置失败数量为 0
                // 可能会出现一些异常,比如调用 queryInstancesOfService 方法的时候
                // 没有 ServiceInfo 连接不到则会出现异常
                resetFailCount();
            } catch (Throwable e) {
                incFailCount();
                NAMING_LOGGER.warn("[NA] failed to update serviceName: " + groupedServiceName, e);
            } finally {
                // 下次调度刷新时间，下次执行的时间与failCount 失败的次数有关，failCount=0，则下次调度时间为6秒，最长为1分钟
                // 当无异常的情况下 failCount 始终都是 0 则默认的时间一直都 6 s
                executor.schedule(this, Math.min(delayTime << failCount, DEFAULT_DELAY * 60), TimeUnit.MILLISECONDS);
            }
        }
    
        private void incFailCount() {
            int limit = 6;
            if (failCount == limit) {
                return;
            }
            failCount++;
        }
    
        private void resetFailCount() {
            failCount = 0;
        }
    }
}
