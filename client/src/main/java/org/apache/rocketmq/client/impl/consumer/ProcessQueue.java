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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;

/**
 * Queue consumption snapshot (队列消费快照）
 * <br/>
 * 已拉取，待处理消息缓存<br/>
 * 消费模式：顺序、并行<br/>
 * 消费进度管理<br/>
 * rebalance负载均衡<br/>
 *
 * <p>部分摘抄自: https://www.jianshu.com/p/5e8ef21c283c</p>
 *

 */
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME =
        Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
    private final InternalLogger log = ClientLogger.getLog();
    private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();
    // 用来保存拉取到的消息,有序
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
    /**
     * 当前处理队列已拉取，未消费消息数；拉取消息后放进来时会增加，消费成功提交时会减少
     */
    private final AtomicLong msgCount = new AtomicLong();
    /**
     * 当前处理队列已拉取，未消费消息字节大小
     */
    private final AtomicLong msgSize = new AtomicLong();
    //消费锁，主要在顺序消费和移除ProcessQueue的时候使用
    private final Lock lockConsume = new ReentrantLock();
    /**
     * A subset of msgTreeMap, will only be used when orderly consume
     */
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();
    // 记录了废弃ProcessQueue的时候lockConsume的次数
    private final AtomicLong tryUnlockTimes = new AtomicLong(0);
    // ProcessQueue中保存的消息里的最大offset，为ConsumeQueue的offset
    private volatile long queueOffsetMax = 0L;
    /**
     * 返回true代表这个ProcessQueue被废弃了，具体出现的原因大概如下：
     * <ul>
     *     <li>rebalance之后，原来存在的MessageQueue不在本地新分配的MessageQueue之中，则把对应的ProcessQueue废弃。<br/>
     *     举个栗子：0123这个4个队列，一开始分配给A消费者，这时候启动一个B消费者后，A消费者分配了01这两个队列，那么原来34队列的dropped就会设置成true</li>
     *     <li>rebalance的时候，会将未订阅的topic下对应的dropped设置成true</li>
     *     <li>还有就是上面isPullExpired讨论的情况</li>
     *     <li>当拉取消息的时候，如果broker返回OFFSET_ILLEGAL，那么这时候将对应的ProcessQueue废弃</li>
     *     <li>consumer关闭(调用shutdown方法)的时候也会废弃</li>
     * </ul>
     */
    private volatile boolean dropped = false;
    // 上次执行拉取消息的时间
    private volatile long lastPullTimestamp = System.currentTimeMillis();
    // 上次获取消息消费时记录的时间（不是消费成功提交时的时间）
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();
    private volatile boolean locked = false;
    // 上次锁定的时间
    private volatile long lastLockTimestamp = System.currentTimeMillis();
    // 是否正在消费(只在存入消息，获取消息时设置)，只适用于顺序消费
    private volatile boolean consuming = false;
    // 表示最近一次拉取消息时，还有多少消息等待拉取。该参数为调整线程池的时候提供了数据参考
    private volatile long msgAccCnt = 0;

    /**
     * 顺序消费的时候使用，消费之前会判断一下ProcessQueue锁定时间是否超过阈值(默认30000ms)，
     * 如果没有超时，代表还是持有锁，具体细节在顺序消费的时候会详细说明.负载
     * @return
     */
    public boolean isLockExpired() {
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    /**
     * 判断距离上次拉取消息时间间隔是否超过阈值。(消极模式下才会有用，)<br/>
     * 在拉取的时候更新lastPullTimestamp的值，然后在rebalance的时候会去判断ProcessQueue已经超过一定的时间没有去拉取消息，<br/>
     * 如果是的话，则将ProcessQueue废弃(setDropped(true))且从ProcessQueue和MessageQueue的对应关系中移除该ProcessQueue
     *
     * <p>根据打的日志推测，这个应该是个BUG，在某种情况下，拉取会停止，导致时间没有更新，这时候重建ProcessQueue，具体是什么原因，这点不太清楚</p>
     * @return
     *
     * @see ConsumeType#CONSUME_PASSIVELY
     */
    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * 并发消费模式下，定时清理消费时间超过15分钟的消息
     * <br/?【检查是否有超时消息，有则上报broker重新投递，并从队列中移除（只适用非顺序消费）】。
     * @param pushConsumer
     */
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        // 顺序消费不处理
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return;
        }

        //最多只校验队列的前16条
        int loop = msgTreeMap.size() < 16 ? msgTreeMap.size() : 16;
        for (int i = 0; i < loop; i++) {
            MessageExt msg = null;
            try {
                this.lockTreeMap.readLock().lockInterruptibly();
                try {
                    //检查队头消息是否消费超时，如果超时，后续上报broker重新投递后会从队列中移除
                    //now - msg#PROPERTY_CONSUME_START_TIMESTAMP > threshold ?
                    //PROPERTY_CONSUME_START_TIMESTAMP为拉取消息时的时间戳？
                    if (!msgTreeMap.isEmpty() && System.currentTimeMillis() - Long.parseLong(MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue())) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                        msg = msgTreeMap.firstEntry().getValue();
                    } else {
                        //如果队头消息都没超时，则后续消费更加不会超时。检查可以直接结束
                        break;
                    }
                } finally {
                    this.lockTreeMap.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            try {
                // 将消息发回Broker，等待重试（让其重新投递此消息），且延迟级别为3
                // 该效果是消费失败重试原理类似。所以消费者可能会重复消费消息。
                pushConsumer.sendMessageBack(msg, 3);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                try {
                    this.lockTreeMap.writeLock().lockInterruptibly();
                    try {
                        //从判定为超时，经过消息发加broker处理，到现在为止，消息还没消费完毕，则从本队列中移除
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        this.lockTreeMap.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }

    /**
     * 把刚刚拉取的消息存入到等待消费处理队列（上写锁，更新计数，字节数）
     * @param msgs
     * @return 顺序消费有用，返回true表示可以消费
     */
    public boolean putMessage(final List<MessageExt> msgs) {
        // 返回值，顺序消费有用，返回true表示可以消费
        boolean dispatchToConsume = false;
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                //本次新增有效消息计数
                int validMsgCnt = 0;
                for (MessageExt msg : msgs) {
                    //以offset升序存入新拉取到的，本地过滤后的消息
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    if (null == old) {
                        //计算有效消息数（不包含覆盖的=>什么时候会覆盖？offset出错，重复拉取？）
                        validMsgCnt++;
                        //本处理队列中的最大offset
                        this.queueOffsetMax = msg.getQueueOffset();
                        //累计未消费消息body字节数
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }
                //增加等待消费消息条数
                msgCount.addAndGet(validMsgCnt);

                //有待处理消息，并且没有其他消息在消费中，则返回可分派消费，设置状态为消费中
                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    dispatchToConsume = true;
                    //保证单线程消费？只适用于顺序消费模式，在获取消息消费为空时，设置为false
                    this.consuming = true;
                }

                if (!msgs.isEmpty()) {
                    MessageExt messageExt = msgs.get(msgs.size() - 1);
                    //获取本次拉取消息时，broker的最大offset
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if (property != null) {
                        //计算拉取消息时，还有多少消息等待拉取：broker的最大offset - 本次拉取的消息的最大offset
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }

        return dispatchToConsume;
    }

    /**
     * 获取当前待处理队列中，首尾两个消息的offset差值，异常返回或者队列为空返回0
     * @return
     */
    public long getMaxSpan() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    /**
     * 写锁，移除消费，更新最后消费时间戳，待消费消息条数，字节数
     * @param msgs
     * @return 返回下一个消费起始位置，异常则返回-1
     */
    public long removeMessage(final List<MessageExt> msgs) {
        long result = -1;
        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!msgTreeMap.isEmpty()) {
                    result = this.queueOffsetMax + 1;
                    int removedCnt = 0;
                    for (MessageExt msg : msgs) {
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        if (prev != null) {
                            removedCnt--;
                            msgSize.addAndGet(0 - msg.getBody().length);
                        }
                    }
                    msgCount.addAndGet(removedCnt);

                    if (!msgTreeMap.isEmpty()) {
                        result = msgTreeMap.firstKey();
                    }
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }

        return result;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    /**
     * 消费失败回滚操作,全部回滚（不管是否部分成功）（写锁），
     * <br/>
     * 消费中队列的消息放回原待处理队列，并清空消费中队列
     *
     * @see #makeMessageToCosumeAgain(List)
     */
    public void rollback() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    /**
     * 消费成功，提交
     * @return 返回下一个消息起始位置，提交失败则返回-1
     */
    public long commit() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                //已成功消费最大offset
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
                //扣减相应待消费消息数
                msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size());
                //扣减相应待消费消息字节数
                for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                    msgSize.addAndGet(0 - msg.getBody().length);
                }
                this.consumingMsgOrderlyTreeMap.clear();
                if (offset != null) {
                    return offset + 1;
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("commit exception", e);
        }

        return -1;
    }

    /**
     * 消费失败回滚操作，【上层可以控制（示实现）：offset小的连续成功的可以不回滚，失败及之后的记录才回滚】（写锁）
     * <br/>
     * 把消费失败的，消费中队列的消息放回原待处理队列
     *
     * @param msgs
     *
     * @see #rollback()
     */
    public void makeMessageToCosumeAgain(List<MessageExt> msgs) {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    /**
     * 批量从处理获取消息去消费。从队列头开始获取。【只适用于顺序消费模式】
     * 从调用者看，也只有顺序消费服务有调用
     * <br/>
     * 该方法顺序消费模式中使用的，取到该消息后就会调用我们定义的MessageListener进行消费
     * @param batchSize
     * @return
     *
     * @see ConsumeMessageOrderlyService.ConsumeRequest#run
     */
    public List<MessageExt> takeMessags(final int batchSize) {
        List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            //记录从本处理队列获取批量消息去消费的开始时间
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    for (int i = 0; i < batchSize; i++) {
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            result.add(entry.getValue());
                            //把待处理消息转移到顺序待消费队列
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                        } else {
                            break;
                        }
                    }
                }
                //如果为空，则设置状态为没有消费
                if (result.isEmpty()) {
                    consuming = false;
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    public boolean hasTempMessage() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
        }

        return true;
    }

    public void clear() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }

    public Lock getLockConsume() {
        return lockConsume;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public long getMsgAccCnt() {
        return msgAccCnt;
    }

    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
        } finally {
            this.lockTreeMap.readLock().unlock();
        }
    }

    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }

    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }

}
