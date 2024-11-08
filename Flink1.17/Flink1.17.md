# 第4章 Flink运行时架构

## 并行度

并行度的优先级：
    代码：算子 > 代码：env > 提交时指定 > 配置文件

## 算子链

1、算子之间的传输关系：
     一对一
     重分区

 2、算子 串在一起的条件：
    1） 一对一
    2） 并行度相同

 3、关于算子链的api：
    1）全局禁用算子链：env.disableOperatorChaining();
    2）某个算子不参与链化：  算子A.disableChaining(),  算子A不会与 前面 和 后面的 算子 串在一起
    3）从某个算子开启新链条：  算子A.startNewChain()， 算子A不与 前面串在一起，从A开始正常链化

## 任务槽

1、slot特点：
    1）均分隔离内存，不隔离cpu
    2）可以共享：
          同一个job中，不同算子的子任务 才可以共享 同一个slot，同时在运行的
          前提是，属于同一个 slot共享组，默认都是“default”

 2、slot数量 与 并行度 的关系

​    1）slot是一种静态的概念，表示最大的并发上限
​          并行度是一种动态的概念，表示 实际运行 占用了 几个

​	2）要求： slot数量 >= job并行度（算子最大并行度），job才能运行

​    3）注意：如果是yarn模式，动态申请
   	   申请的TM数量 = job并行度 / 每个TM的slot数，向上取整
  	    比如session： 一开始 0个TaskManager，0个slot
   	     --》 提交一个job，并行度10
​      	  --》 10/3，向上取整，申请4个tm， 使用10个slot，剩余2个slot





# 第6章 窗口

## 窗口的原理

触发器、移除器： 现成的几个窗口，都有默认的实现，一般不需要自定义

以 时间类型的 滚动窗口 为例，分析原理：

1、窗口什么时候触发 输出？
        时间进展 >= 窗口的最大时间戳（end - 1ms）

2、窗口是怎么划分的？
        start= 向下取整，取窗口长度的整数倍
        end = start + 窗口长度

​        窗口左闭右开 ==》 属于本窗口的 最大时间戳 = end - 1ms

3、窗口的生命周期？
        创建： 属于本窗口的第一条数据来的时候，现new的，放入一个singleton单例的集合中
        销毁（关窗）： 时间进展 >=  窗口的最大时间戳（end - 1ms） + 允许迟到的时间（默认0）



## 乱序与迟到

1、乱序与迟到的区别
     乱序： 数据的顺序乱了， 时间小的 比 时间大的 晚来
     迟到： 数据的时间戳 < 当前的watermark

2、乱序、迟到数据的处理
1） watermark中指定 乱序等待时间
2） 如果开窗，设置窗口允许迟到
     =》 推迟关窗时间，在关窗之前，迟到数据来了，还能被窗口计算，来一条迟到数据触发一次计算
     =》 关窗后，迟到数据不会被计算
3） 关窗后的迟到数据，放入侧输出流

如果 watermark等待3s，窗口允许迟到2s， 为什么不直接 watermark等待5s 或者 窗口允许迟到5s？
 =》 watermark等待时间不会设太大 ===》 影响的计算延迟
         如果3s ==》 窗口第一次触发计算和输出，  13s的数据来 。  13-3=10s
         如果5s ==》 窗口第一次触发计算和输出，  15s的数据来 。  15-5=10s
 =》 窗口允许迟到，是对 大部分迟到数据的 处理， 尽量让结果准确
         如果只设置 允许迟到5s， 那么 就会导致 频繁 重新输出

设置经验
 1、watermark等待时间，设置一个不算特别大的，一般是秒级，在 乱序和 延迟 取舍
 2、设置一定的窗口允许迟到，只考虑大部分的迟到数据，极端小部分迟到很久的数据，不管
 3、极端小部分迟到很久的数据， 放到侧输出流。 获取到之后可以做各种处理





# 第7章 处理函数

## 定时器

1、keyed才有

2、事件时间定时器，通过watermark来触发的
   watermark >= 注册的时间
   注意： watermark = 当前最大事件时间 - 等待时间 -1ms， 因为 -1ms，所以会推迟一条数据
       比如， 5s的定时器，
       如果 等待=3s， watermark = 8s - 3s -1ms = 4999ms,不会触发5s的定时器
       需要 watermark = 9s -3s -1ms = 5999ms ，才能去触发 5s的定时器

3、在process中获取当前watermark，显示的是上一次的watermark
   =》因为process还没接收到这条数据对应生成的新watermark





# 第9章 容错机制

检查点算法的总结

1、Barrier对齐： 一个Task 收到 所有上游 同一个编号的 barrier之后，才会对自己的本地状态做 备份
     精准一次： 在barrier对齐过程中，barrier后面的数据 阻塞等待（不会越过barrier）
     至少一次： 在barrier对齐过程中，先到的barrier，其后面的数据 不阻塞 接着计算

2、非Barrier对齐： 一个Task 收到 第一个 barrier时，就开始 执行备份，能保证 精准一次（flink 1.11出的新算法）
     先到的barrier，将 本地状态 备份， 其后面的数据接着计算输出
     未到的barrier，其 前面的数据 接着计算输出，同时 也保存到 备份中
     最后一个barrier到达 该Task时，这个Task的备份结束



