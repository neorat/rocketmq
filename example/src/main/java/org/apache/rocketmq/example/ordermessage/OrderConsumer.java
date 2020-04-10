/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.ordermessage;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class OrderConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ordered_group_name_consumer2");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumeThreadMin(10);
        consumer.setConsumeThreadMax(10);
//        consumer.setConsumeTimeout(100);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("TradeOrderTopic", "tags");

        //多线程消费，同队列内也无序
        consumer.registerMessageListener(new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                if (msgs == null || msgs.isEmpty()){
                    return ConsumeOrderlyStatus.SUCCESS;
                }
                //设置自动提交
                context.setAutoCommit(true);
                msgs.stream()
                    .forEach(msg -> {
                        try {
                            String messageBody = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                            System.out.println("Handle Order Message: t: " +  Thread.currentThread().getName() + ",keys: " + msg.getKeys() + ",messageBody: " + messageBody);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

}
