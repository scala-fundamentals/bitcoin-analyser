package coinyser.draft

import com.pusher.client.Pusher
import com.pusher.client.channel.SubscriptionEventListener
import com.pusher.client.connection.{ConnectionEventListener, ConnectionState, ConnectionStateChange}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class BitstampReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  @transient
  lazy val pusher = new Pusher("de504dc5763aeef9ff52")

  def onStart(): Unit = {
    pusher.connect(new ConnectionEventListener {
      def onConnectionStateChange(change: ConnectionStateChange): Unit = {
        println("State changed to " + change.getCurrentState + " from " + change.getPreviousState)
      }

      def onError(message: String, code: String, e: Exception): Unit = {
        println("There was a problem connecting!")
      }
    }, ConnectionState.ALL)

    val channel = pusher.subscribe("live_trades")
    channel.bind("trade", new SubscriptionEventListener() {
      override def onEvent(channel: String, event: String, data: String): Unit = {
        store(data)
      }
    })

  }

  def onStop(): Unit = {
    pusher.disconnect()
    pusher.unsubscribe("live_trades")
  }


}
