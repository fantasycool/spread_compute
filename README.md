# spread_compute
分布式数据切分组件，实现任务注册，数据shuffle,定时任务启动

对于一些轻量的数据计算的场景，比如按照用户做切分，分配不同的机器执行。 考虑以下几个场景： 1：对于那些数据bucket之间关联性不大的情况，只需要做到均衡分配到不同的worker节点上执行的情况。 2：离线任务需要统一管理,能够像crontab一样定时自动触发任务，但是又需要分布式执行的情况。

整体结构：master/producer/consumer的结构

使用者只需要实现producer和consumer的接口,实现Producer接口来产出数据,数据会自动被水平切分到不同consumer进行执行,并且调用实现后的consumerservice的接口.

type ProducerService interface { ProduceData() ([]interface{}, error) }

type ConsumerService interface { ConsumeData(datas []interface{}) error }

可以采用两种触发执行任务: 1:http请求master的方式触发 2:定时触发的方式

TODO 1: crontab模式 2: groupId的信息自动持久化 3: 数据增量分发切分. 4: streaming 模式下分发.
