import org.knowm.xchange.ExchangeFactory
import org.knowm.xchange.bitstamp.BitstampExchange
import org.knowm.xchange.currency.CurrencyPair

val bitstamp = ExchangeFactory.INSTANCE.createExchange(classOf[BitstampExchange].getName)
val marketDataService = bitstamp.getMarketDataService
val ticker = marketDataService.getTicker(CurrencyPair.BTC_USD)
val orders = marketDataService.getOrderBook(new CurrencyPair("BTC", "USD"))
orders.getAsks
// With orders, that's quite a lot of data
// 1- Put raw stuff in Kafka. Use an API ?
// https://github.com/monix/monix-kafka

// 2 - Listen to topic with Spark, write to parquet

// 3- with spark structured streaming, compute a view saved to parquet
// 4- show visualization in Zep



