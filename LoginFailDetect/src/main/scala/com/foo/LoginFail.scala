package com.foo

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 状态编程的实现模式
  *
  */
//定义一个输入的登录事件流
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

object LoginFail {
  def main(args: Array[String]): Unit = {

      //1.创建执行环境
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      //显示地定义Time的类型,默认是Pro
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      //设置并行度
      env.setParallelism(1)

    val loginStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
        LoginEvent(1, "192.168.0.2", "fail", 1558430843),
        LoginEvent(1, "192.168.0.3", "fail", 1558430844),
        LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime*1000)
      .filter(_.eventType=="fail")
      .keyBy(_.userId) //按照userId做分流
      .process(new MatchFunction)

    loginStream.print

    env.execute("Login Fail Detect Job")

  }
}
//自定义keyedProcessFunction
class MatchFunction extends KeyedProcessFunction[Long,LoginEvent,LoginEvent] {
//直接定义我们的状态变量懒加载
  lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginState",classOf[LoginEvent]))
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, out: Collector[LoginEvent]): Unit = {
    loginState.add(value)
    //注定定时器,设定为2s后触发
    ctx.timerService().registerEventTimeTimer(value.eventTime*1000 + 2*1000)


  }

  //实现onTime的触发操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {
    //从状态中获取所有的url的访问量
    val allLogins: ListBuffer[LoginEvent] = ListBuffer()
    import scala.collection.JavaConversions._
    for(login<-loginState.get()){
      allLogins += login
    }
    //清楚状态
    loginState.clear()
//判断 重复的次数 大于2进行报警
    if(allLogins.length >= 2){
      out.collect(allLogins.head)
    }
  }
}
