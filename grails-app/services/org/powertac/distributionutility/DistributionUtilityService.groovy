/*
 * Copyright 2009-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS,  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package org.powertac.distributionutility

import java.util.List;
import java.math.BigDecimal;

import org.joda.time.Instant;
import org.powertac.common.Timeslot;
import org.powertac.common.interfaces.DistributionUtility
import org.powertac.common.interfaces.TimeslotPhaseProcessor

class DistributionUtilityService implements DistributionUtility, TimeslotPhaseProcessor
{
  def accountingService
  
  static transactional = true
  
  // Rate per kWH
  BigDecimal imbalancePenaltyRate = 0.1

  public void activate (Instant time, int phaseNumber) 
  {
    // Compute overall balance
	// TODO compute balance of each individual broker  
	
	List brokerList = Broker.list()
	if ( brokerList == null ){
		log.error("Failed to retrieve broker list")
		return
	}
	
	BigDecimal netProduction = 0.0
	for ( b in brokerList ){
		netProduction += accountingService.getCurrentMarketPosition(b)
		netLoad += accountingService.getCurrentNetLoad(b)
	}
	
	BigDecimal marketBalance = netProduction - netLoad
	
	  
	// Run the balancing market
	// Transactions are posted to the Accounting Service and Brokers are notified of balancing transactions
	List txList = balanceTimeslot(timeslot.currentTimeslot(), brokerList, marketBalance)

  }
  
  public List balanceTimeslot (Timeslot currentTimeslot, List brokerList, BigDecimal marketBalance) 
  {
	if ( marketBalance == 0.0 )
		return null
	else{
		List balancingMarketTxs = []
		BigDecimal imbalanceQuantity = marketBalance / brokerList.size()
		
		// Charge each broker an equal amount if marketBalance is neg, credit each broker if pos
		for ( b in brokerList ){
			balancingMarketTxs.add( accountingService.addMarketTransaction( b,
																		   	currentTimeslot,
																			(imbalanceQuantity * imbalancePenaltyRate),
																			imbalanceQuantity ) )
		}
		return balancingMarketTxs
	}
  }

}
