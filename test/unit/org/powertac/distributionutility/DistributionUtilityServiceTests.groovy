package org.powertac.distributionutility

import grails.test.GrailsUnitTestCase
import grails.test.*

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Instant

import org.powertac.common.Broker
import org.powertac.common.TimeService
// import org.powertac.common.TimeSlot
import org.powertac.common.AbstactCustomer
import org.powertac.common.CustomerInfo
import org.powertac.common.Tariff
import org.powertac.common.TariffSpecification
import org.powertac.common.TariffSubscription
import org.powertac.common.TariffTransaction
import org.powertac.common.enumerations.CustomerType
import org.powertac.common.enumerations.PowerType
import org.powertac.common.enumerations.TariffTransactionType


class DistributionUtilityServiceTests extends GrailsUnitTestCase 
{
	def timeService
	List brokerList = []
	List tariffSpecList = []
	List tariffList = []
	DateTime start	
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
		Instant exp = new Instant(start.millis + TimeService.WEEK * 10)
		
		// Create 5 test brokers
		for( int i = 0; i < 5; i++ ){
			Broker broker = new Broker (username: 'testBroker' + i.toString(), password: 'testPassword')
			assert(broker.save())
			brokerList[i] = broker;
		}
		
		// Create a tariff specification for each broker
		for ( int i = 0; i < 5; i++ ){
			TariffSpecification tariffSpec = 
				new TariffSpecification(brokerId: brokerList[i].getId(),
				expiration: exp,
				minDuration: TimeService.WEEK * 8)
			tariffSpec.addToRates(new Rate(value: 0.1))
			tariffSpec.save()
			tariffSpecList[i] = tariffSpec
		}
		
		// Create a tariff for each specification
		for ( int i = 0; i < 5; i++ ){
			Tariff tariff = new Tariff(tariffSpec: tariffSpecList[i])
			tariff.init()
			assert(tariff.save())
		}
		
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
    }

    protected void tearDown() {
        super.tearDown()
		TariffTransaction.list()*.delete()
    }

    void testNegImbalancedMarket() {
		def accountingService
		def distributionUtilityService
		
		// TODO - Add tariff transactions to negatively bias market. Sum tariffs
		// for market balance. 
		accountingService.addTariffTransaction(...)
		
		// Run for a few timeslots?
		
		List marketTxs = 
			distributionUtilityService.balanceTimeslot(Timeslot.currentTimeslot(), brokerList, marketBalance)
		
		// TODO - make sure marketTxs balance the tariff transactions.
    }
	
	void testBalancedMarket() {
		def accountingService	
		def distributionUtilityService		
		
		// Run for a few timeslots?
		
		List marketTxs = 
			distributionUtilityService.balanceTimeslot(Timeslot.currentTimeslot(), brokerList, 0.0)
			
		assertNull("Market already balanced, no Market Transactions produced.", marketTxs)
	}
	
	void testPosImbalancedMarket() {
		def accountingService
		def distributionUtilityService
		
		// TODO - Add tariff transactions to positively bias market. Sum tariffs
		// for market balance.
		accountingService.addTariffTransaction(...)
		
		// Run for a few timeslots?
		
		List marketTxs =
			distributionUtilityService.balanceTimeslot(Timeslot.currentTimeslot(), brokerList, marketBalance)
		
		// TODO - make sure marketTxs balance the tariff transactions.
	}
}
