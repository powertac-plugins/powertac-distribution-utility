package org.powertac.distributionutility

import grails.test.*

import java.util.Set

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Instant

import org.powertac.common.Broker
import org.powertac.common.Competition
import org.powertac.common.Orderbook
import org.powertac.common.TimeService
import org.powertac.common.AbstractCustomer
import org.powertac.common.CustomerInfo
import org.powertac.common.MarketTransaction
import org.powertac.common.BalancingTransaction
import org.powertac.common.DistributionTransaction
import org.powertac.common.Rate
import org.powertac.common.Tariff
import org.powertac.common.TariffSpecification
import org.powertac.common.TariffSubscription
import org.powertac.common.TariffTransaction
import org.powertac.common.Timeslot
import org.powertac.common.enumerations.CustomerType
import org.powertac.common.enumerations.PowerType
import org.powertac.common.enumerations.TariffTransactionType


class DistributionUtilityServiceTests extends GrailsUnitTestCase
{
  def accountingService
  def timeService
  def tariffMarketService
  def distributionUtilityService

  List<Broker> brokerList = []
  List tariffSpecList = []
  List tariffList = []
  List messageList = []
  Instant exp
  DateTime start
  Competition competition
  CustomerInfo customerInfo
  AbstractCustomer customer

  protected void setUp()
  {
    // Clear the database and set up a new start time.
    TariffSpecification.list()*.delete()
    Tariff.list()*.delete
    super.setUp()
    start = new DateTime(2011, 1, 1, 12, 0, 0, 0, DateTimeZone.UTC)
    timeService.currentTime = start.toInstant()
    Timeslot ts = new Timeslot(serialNumber: 4,
        startInstant: start.toInstant(),
        endInstant: new Instant(start.millis + TimeService.HOUR),
        enabled: false)
    assert(ts.save())
    assert(Timeslot.currentTimeslot() == ts)
    exp = new Instant(start.millis + TimeService.WEEK * 10)

    // Create 3 test brokers
    Broker broker1 = new Broker (username: 'testBroker1', password: 'testPassword')
    assert(broker1.save())
    brokerList[0] = broker1;
    Broker broker2 = new Broker (username: 'testBroker2', password: 'testPassword')
    assert(broker2.save())
    brokerList[1] = broker2;
    Broker broker3= new Broker (username: 'testBroker3', password: 'testPassword')
    assert(broker3.save())
    brokerList[2] = broker3;

    // Create customer
    customerInfo = new CustomerInfo(name:"Wilfred", customerType: CustomerType.CustomerHousehold)
    if (!customerInfo.validate()) {
      customerInfo.errors.each { println it.toString() }
      fail("Could not save customer")
    }
    assert(customerInfo.save())
    customer = new AbstractCustomer(customerInfo: customerInfo)
    customer.init()
    if (!customer.validate()) {
      customer.errors.each { println it.tostring() }
      fail("Could not save customer")
    }
    assert(customer.save())
    
    // create a Competition, needed for initialization
    if (Competition.count() == 0) {
      competition = new Competition(name: 'du-test')
      assert competition.save()
    }
    else {
      competition = Competition.list().first()
    }

    // mock the brokerProxyService
    def brokerProxy =
      [sendMessage: { broker, message ->
        messageList << message
      },
      sendMessages: { broker, messageList ->
        messageList.each { message ->
          messageList << message
        }
      },
      broadcastMessage: { message ->
        messageList << message
      },
      broadcastMessages: { messageList ->
        messageList.each { message ->
          messageList << message
        }
      },
      registerBrokerTariffListener: { thing ->
        println "tariff listener registration"
      }]
    accountingService.brokerProxyService = brokerProxy
    accountingService.pendingTransactions.clear()
  }

  protected void tearDown() 
  {
    super.tearDown()
    TariffTransaction.list()*.delete()
    BalancingTransaction.list()*.delete()
    DistributionTransaction.list()*.delete()
    brokerList = []
    tariffSpecList = []
    tariffList = []
  }

  void testGetMarketBalance() 
  {
    BigDecimal balance = 0.0

    // Create two tariff specifications, one for consumption and one for production
    TariffSpecification tariffSpec1 =
        new TariffSpecification(broker: brokerList[0],
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec1.addToRates(new Rate(value: 0.1))
    assert(tariffSpec1.save())

    TariffSpecification tariffSpec2 =
        new TariffSpecification(broker: brokerList[0],
        powerType: PowerType.PRODUCTION,
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec2.addToRates(new Rate(value:0.1))
    assert(tariffSpec2.save())

    // Create a tariff for each specification
    Tariff tariff1 = new Tariff(tariffSpec: tariffSpec1)
    tariff1.init()
    assert(tariff1.save())

    Tariff tariff2 = new Tariff(tariffSpec: tariffSpec2)
    tariff2.init()
    assert(tariff2.save())

    // Subscribe customers to each tariff
    TariffSubscription tsubConsume =
        tariffMarketService.subscribeToTariff(tariff1, customer, 5)
    TariffSubscription tsubProduce =
        tariffMarketService.subscribeToTariff(tariff2, customer, 5)

    tsubConsume.usePower(500.0)
    balance -= 500.0
    assertEquals("correct balance", balance,
        distributionUtilityService.getMarketBalance(brokerList[0]),
        1e-6)

    tsubProduce.usePower(-500.0)
    balance += 500.0
    assertEquals("correct balance", balance,
        distributionUtilityService.getMarketBalance(brokerList[0]),
        1e-6)

    tsubConsume.usePower(50000)
    balance -= 50000
    assertEquals("correct balance", balance,
        distributionUtilityService.getMarketBalance(brokerList[0]),
        1e-6)
  }

  void testNegImbalancedMarket() 
  {
    BigDecimal powerUse = 50.0

    // Create a tariff specification for each broker
    TariffSpecification tariffSpec1 =
        new TariffSpecification(broker: brokerList[0],
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec1.addToRates(new Rate(value: 0.1))
    assert(tariffSpec1.save())
    tariffSpecList[0] = tariffSpec1

    TariffSpecification tariffSpec2=
        new TariffSpecification(broker: brokerList[1],
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec2.addToRates(new Rate(value: 0.1))
    assert(tariffSpec2.save())
    tariffSpecList[1] = tariffSpec2

    TariffSpecification tariffSpec3 =
        new TariffSpecification(broker: brokerList[2],
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec3.addToRates(new Rate(value: 0.1))
    assert(tariffSpec3.save())
    tariffSpecList[2] = tariffSpec3

    // Create a tariff for each specification
    Tariff tariff1 = new Tariff(tariffSpec: tariffSpecList[0])
    tariff1.init()
    assert(tariff1.save())
    tariffList[0] = tariff1

    Tariff tariff2 = new Tariff(tariffSpec: tariffSpecList[1])
    tariff2.init()
    assert(tariff2.save())
    tariffList[1] = tariff2

    Tariff tariff3 = new Tariff(tariffSpec: tariffSpecList[2])
    tariff3.init()
    assert(tariff3.save())
    tariffList[2] = tariff3

    // Subscribe customers to each tariff
    TariffSubscription tsub1 =
        tariffMarketService.subscribeToTariff(tariffList[0], customer, 5)
    TariffSubscription tsub2 =
        tariffMarketService.subscribeToTariff(tariffList[1], customer, 5)
    TariffSubscription tsub3 =
        tariffMarketService.subscribeToTariff(tariffList[2], customer, 5)

    // Negatively balance market, each broker has equal load (in kWh).
    tsub1.usePower(powerUse)
    tsub2.usePower(powerUse)
    tsub3.usePower(powerUse)

    BigDecimal marketBalance = powerUse * -3 // Compute market balance in MWh

    distributionUtilityService.balanceTimeslot(Timeslot.currentTimeslot(),
        brokerList)

    assertEquals("correct number of balance tx", 3, BalancingTransaction.count ())
    assertEquals("correct number of distro tx", 3, DistributionTransaction.count())
    double distributionFees = 0.0 
    BalancingTransaction.list().each { tx ->
      marketBalance -= tx.quantity
    }
    assertEquals("correct balancing transactions", 0.0, marketBalance, 1e-6)
    DistributionTransaction.list().each { tx ->
      distributionFees += tx.charge
    }
    assertEquals("correct fee transactions",
       powerUse * 3 * distributionUtilityService.distributionFee,
       distributionFees, 1e-6)
  }

  void testPosImbalancedMarket()
  {
    BigDecimal powerUse = -50.0

    // Create a production tariff specification for each broker
    TariffSpecification tariffSpec1 =
        new TariffSpecification(broker: brokerList[0],
        powerType: PowerType.PRODUCTION,
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec1.addToRates(new Rate(value: 0.1))
    assert(tariffSpec1.save())
    tariffSpecList[0] = tariffSpec1

    TariffSpecification tariffSpec2=
        new TariffSpecification(broker: brokerList[1],
        powerType: PowerType.PRODUCTION,
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec2.addToRates(new Rate(value: 0.1))
    assert(tariffSpec2.save())
    tariffSpecList[1] = tariffSpec2

    TariffSpecification tariffSpec3 =
        new TariffSpecification(broker: brokerList[2],
        powerType: PowerType.PRODUCTION,
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec3.addToRates(new Rate(value: 0.1))
    assert(tariffSpec3.save())
    tariffSpecList[2] = tariffSpec3

    // Create a tariff for each specification
    Tariff tariff1 = new Tariff(tariffSpec: tariffSpecList[0])
    tariff1.init()
    assert(tariff1.save())
    tariffList[0] = tariff1

    Tariff tariff2 = new Tariff(tariffSpec: tariffSpecList[1])
    tariff2.init()
    assert(tariff2.save())
    tariffList[1] = tariff2

    Tariff tariff3 = new Tariff(tariffSpec: tariffSpecList[2])
    tariff3.init()
    assert(tariff3.save())
    tariffList[2] = tariff3

    // Subscribe customers to each tariff
    TariffSubscription tsub1 =
        tariffMarketService.subscribeToTariff(tariffList[0], customer, 5)
    TariffSubscription tsub2 =
        tariffMarketService.subscribeToTariff(tariffList[1], customer, 5)
    TariffSubscription tsub3 =
        tariffMarketService.subscribeToTariff(tariffList[2], customer, 5)

    // Negatively balance market, each broker has equal load (in kWh).
    tsub1.usePower(powerUse)
    tsub2.usePower(powerUse)
    tsub3.usePower(powerUse)

    BigDecimal marketBalance = powerUse * -3 // Compute market balance in MWh

    distributionUtilityService.balanceTimeslot(Timeslot.currentTimeslot(),
        brokerList)
        
    assertEquals("correct number of balance tx", 3, BalancingTransaction.count ())
    assertEquals("correct number of distro tx", 3, DistributionTransaction.count())
    double distributionFees = 0.0
    BalancingTransaction.list().each { tx ->
      marketBalance -= tx.quantity
    }
    assertEquals("correct balancing transactions", 0.0, marketBalance, 1e-6)
    DistributionTransaction.list().each { tx ->
      distributionFees += tx.charge
    }
    assertEquals("correct fee transactions",
      powerUse * 3 * distributionUtilityService.distributionFee,
      distributionFees, 1e-6)
  }

  void testIndividualBrokerBalancing() 
  {
    BigDecimal balance = 0.0

    // Create tariff specifications for each broker
    TariffSpecification tariffSpec1 =
        new TariffSpecification(broker: brokerList[0],
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec1.addToRates(new Rate(value: 0.1))
    assert(tariffSpec1.save())

    TariffSpecification tariffSpec2 =
        new TariffSpecification(broker: brokerList[1],
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec2.addToRates(new Rate(value: 0.1))
    assert(tariffSpec2.save())

    TariffSpecification tariffSpec3 =
        new TariffSpecification(broker: brokerList[2],
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec3.addToRates(new Rate(value: 0.1))
    assert(tariffSpec3.save())

    // Create a tariff for each specification
    Tariff tariff1 = new Tariff(tariffSpec: tariffSpec1)
    tariff1.init()
    assert(tariff1.save())

    Tariff tariff2 = new Tariff(tariffSpec: tariffSpec2)
    tariff2.init()
    assert(tariff2.save())

    Tariff tariff3 = new Tariff(tariffSpec: tariffSpec3)
    tariff3.init()
    assert(tariff3.save())

    // Subscribe customers to each tariff
    TariffSubscription tsub1 =
        tariffMarketService.subscribeToTariff(tariff1, customer, 5)
    TariffSubscription tsub2 =
        tariffMarketService.subscribeToTariff(tariff2, customer, 5)
    TariffSubscription tsub3 =
        tariffMarketService.subscribeToTariff(tariff3, customer, 5)

    // Test positively balanced broker
    tsub1.usePower(19654852)
    tsub1.usePower(-54862)
    // Test balanced broker (mtxs should not contain a market transaction for this broker)
    tsub2.usePower(500000)
    tsub2.usePower(-500000)
    // Test negatively balanced broker
    tsub3.usePower(-8796542)
    tsub3.usePower(5423)

    // Compute market balance
    brokerList.each  { b ->
      balance += distributionUtilityService.getMarketBalance(b)
    }
    distributionUtilityService.balanceTimeslot(Timeslot.currentTimeslot(), brokerList)

    // ensure each broker was balanced correctly
    int i = 0
    BalancingTransaction.list().each { tx ->
      // match correct broker to tx
      while( (i < brokerList.size()) && (brokerList[i].id != tx.broker.id) ){
        i++
      }
      if ( i < brokerList.size() ) {
        assertEquals("broker correctly balanced", 0.0,
            (distributionUtilityService.getMarketBalance(brokerList[i]) - tx.quantity),
            1e-6)
        balance -= tx.quantity
      }
    }

    assertEquals("market fully balanced", 0.0, balance, 1e-6)
  }

  void testScenario1BalancingCharges() 
  {
    // Create a tariff specification for each broker
    TariffSpecification tariffSpec1 =
        new TariffSpecification(broker: brokerList[0],
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec1.addToRates(new Rate(value: 0.1))
    assert(tariffSpec1.save())
    tariffSpecList[0] = tariffSpec1

    TariffSpecification tariffSpec2=
        new TariffSpecification(broker: brokerList[1],
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec2.addToRates(new Rate(value: 0.1))
    assert(tariffSpec2.save())
    tariffSpecList[1] = tariffSpec2

    TariffSpecification tariffSpec3 =
        new TariffSpecification(broker: brokerList[2],
        expiration: exp,
        minDuration: TimeService.WEEK * 8)
    tariffSpec3.addToRates(new Rate(value: 0.1))
    assert(tariffSpec3.save())
    tariffSpecList[2] = tariffSpec3

    // Create a tariff for each specification
    Tariff tariff1 = new Tariff(tariffSpec: tariffSpecList[0])
    tariff1.init()
    assert(tariff1.save())
    tariffList[0] = tariff1

    Tariff tariff2 = new Tariff(tariffSpec: tariffSpecList[1])
    tariff2.init()
    assert(tariff2.save())
    tariffList[1] = tariff2

    Tariff tariff3 = new Tariff(tariffSpec: tariffSpecList[2])
    tariff3.init()
    assert(tariff3.save())
    tariffList[2] = tariff3

    // Subscribe customers to each tariff
    TariffSubscription tsub1 =
        tariffMarketService.subscribeToTariff(tariffList[0], customer, 5)
    TariffSubscription tsub2 =
        tariffMarketService.subscribeToTariff(tariffList[1], customer, 5)
    TariffSubscription tsub3 =
        tariffMarketService.subscribeToTariff(tariffList[2], customer, 5)

    // Balance brokers such that balances are: 2, -4, and 0 (MWh) respectively
    tsub1.usePower(-200)
    tsub2.usePower(400)

    //List solution = distributionUtilityService.computeNonControllableBalancingCharges(brokerList)
    distributionUtilityService.balanceTimeslot(Timeslot.currentTimeslot(), brokerList)

    // Correct solution list is [-4, 14, 2]
    BalancingTransaction btx = BalancingTransaction.findByBroker(brokerList[0])
    assertNotNull("non-null btx, broker 1", btx)
    assertEquals("correct balancing charge broker1", -4, btx.charge, 1e-6)
    btx = BalancingTransaction.findByBroker(brokerList[1])
    assertNotNull("non-null btx, broker 2", btx)
    assertEquals("correct balancing charge broker2", 14, btx.charge, 1e-6)
    btx = BalancingTransaction.findByBroker(brokerList[2])
    assertNotNull("non-null btx, broker 3", btx)
    assertEquals("correct balancing charge broker3", 2, btx.charge, 1e-6)
  }
  
  void testSpotPrice ()
  {
    // add some new timeslots
    Timeslot ts0 = Timeslot.currentTimeslot()
    long start = timeService.currentTime.millis
    Timeslot ts1 = new Timeslot(serialNumber: 1,
      startInstant: new Instant(start - TimeService.HOUR * 3),
      endInstant: new Instant(start - TimeService.HOUR * 2),
      enabled: false)
    assert ts1.save()
    Timeslot ts2 = new Timeslot(serialNumber: 2,
      startInstant: new Instant(start - TimeService.HOUR * 2),
      endInstant: new Instant(start - TimeService.HOUR),
      enabled: false)
    assert ts2.save()
    Timeslot ts3 = new Timeslot(serialNumber: 3,
      startInstant: new Instant(start - TimeService.HOUR),
      endInstant: new Instant(start),
      enabled: false)
    assert ts3.save()
    
    // add some orderbooks
    Orderbook ob = new Orderbook(transactionId: "ob3.3",
      dateExecuted: new Instant(start - TimeService.HOUR * 3),
      timeslot: ts3, clearingPrice: 33.0)
    ts3.addToOrderbooks(ob)
    assert ob.save()
    ob = new Orderbook(transactionId: "ob3.2",
      dateExecuted: new Instant(start - TimeService.HOUR * 2),
      timeslot: ts3, clearingPrice: 32.0)
    ts3.addToOrderbooks(ob)
    assert ob.save()
    ob = new Orderbook(transactionId: "ob0.2",
      dateExecuted: new Instant(start - TimeService.HOUR * 2),
      timeslot: ts0, clearingPrice: 20.2)
    ts0.addToOrderbooks(ob)
    assert ob.save()
    // this should be the spot price
    ob = new Orderbook(transactionId: "ob0.1",
      dateExecuted: new Instant(start - TimeService.HOUR),
      timeslot: ts0, clearingPrice: 20.1)
    ts0.addToOrderbooks(ob)
    assert ob.save()
    
    // make sure we can retrieve current spot price
    assertEquals("correct spot price", 0.0201, distributionUtilityService.getSpotPrice())
  }
}
