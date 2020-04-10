package org.apache.rocketmq.example.ordermessage;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * Copyright (c) 2019,www.akulaku.com All rights reserved.
 * <p>
 * <br/>
 *
 * @author laoshu(linxb)
 * @date 2020/4/8 18:06
 * @since v1.0.0
 **/
public class OrderProducer {
    private static final String[] ORDER_MESSAGES = {"购物车","下单","结算","支付","发货","确认收货","完成"};

    public static void main(String[] args) throws Exception{

        DefaultMQProducer producer = new DefaultMQProducer("ordered_group_name_producer2");
        producer.setNamesrvAddr("localhost:9876");
        producer.setSendMsgTimeout(30_000);
        producer.start();

        //注：要实现顺序消费，必须同步发送消息
        for (int i = 0;i < 1;i++){
            String orderId = "" + (i + 1);
            for (int j = 0,size = ORDER_MESSAGES.length;j < size;j++){
                String message = "Order-" + orderId + "-" + ORDER_MESSAGES[j];
                String keys = message;
                byte[] messageBody = message.getBytes(RemotingHelper.DEFAULT_CHARSET);
                Message mqMsg = new Message("TradeOrderTopic", "tags", keys, messageBody);
                SendResult sendResult = producer.send(mqMsg, ((mqs, msg, arg) -> {
                    Integer id = (Integer) arg;
                    int index = id % mqs.size();
                    return mqs.get(index);
                }),i);

                System.out.printf("%s%n", sendResult);
            }
        }

        producer.shutdown();

    }
}
