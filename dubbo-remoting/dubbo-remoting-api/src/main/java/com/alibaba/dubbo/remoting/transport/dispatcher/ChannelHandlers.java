/*
 * Copyright 1999-2011 Alibaba Group.
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
package com.alibaba.dubbo.remoting.transport.dispatcher;


import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.Dispatcher;
import com.alibaba.dubbo.remoting.exchange.support.header.HeartbeatHandler;
import com.alibaba.dubbo.remoting.transport.MultiMessageHandler;
import com.alibaba.dubbo.rpc.RpcContext;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chao.liuc
 *
 */
public class ChannelHandlers {

    private static Map<String, String> SIDE_THREAD_HANDLER = new HashMap<String, String>(2);

    static {
        SIDE_THREAD_HANDLER.put(Constants.PROVIDER_SIDE, Constants.PROVIDER_THREAD_CHANNEL_HANDLER);
        SIDE_THREAD_HANDLER.put(Constants.CONSUMER_SIDE, Constants.CONSUMER_THREAD_CHANNEL_HANDLER);
    }

    public static ChannelHandler wrap(ChannelHandler handler, URL url){
        return ChannelHandlers.getInstance().wrapInternal(handler, url);
    }

    protected ChannelHandlers() {}

    protected ChannelHandler wrapInternal(ChannelHandler handler, URL url) {
        return new MultiMessageHandler(new HeartbeatHandler(wrapThreadPool(handler, url)));
    }

    private static ChannelHandlers INSTANCE = new ChannelHandlers();

    protected static ChannelHandlers getInstance() {
        return INSTANCE;
    }

    static void setTestingChannelHandlers(ChannelHandlers instance) {
        INSTANCE = instance;
    }

    private ChannelHandler wrapThreadPool(ChannelHandler handler, URL url) {
        ChannelHandler dispatcherHandler = ExtensionLoader.getExtensionLoader(Dispatcher.class)
                .getAdaptiveExtension().dispatch(handler, url);
        if (dispatcherHandler instanceof WrappedChannelHandler) {
            // 服务端线程池与消费端线程池要区分存
            // 添加到上下文中，供监控过滤器使用
            String threadHandler = SIDE_THREAD_HANDLER.get(url.getParameter(Constants.SIDE_KEY));
            if (StringUtils.isNotEmpty(threadHandler) && !RpcContext.getReadOnlyContext().containsKey(threadHandler)) {
                RpcContext.getReadOnlyContext().put(threadHandler, dispatcherHandler);
            }
        }
        return dispatcherHandler;
    }
}