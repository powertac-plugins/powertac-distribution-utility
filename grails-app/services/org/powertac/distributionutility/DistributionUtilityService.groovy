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

import java.util.List
import java.math.BigDecimal

import org.joda.time.Instant
import org.powertac.common.Broker
import org.powertac.common.Timeslot
import org.powertac.common.interfaces.TimeslotPhaseProcessor

class DistributionUtilityService implements TimeslotPhaseProcessor
{

def accountingService
  
  static transactional = true
  
  // Rate per kWH
  BigDecimal imbalancePenaltyRate = 0.1

  public void activate (Instant time, int phaseNumber) 
  {	
	List brokerList = Broker.list()
	if ( brokerList == null ){
		log.error("Failed to retrieve broker list")
		return
	}
	  
	// Run the balancing market
	// Transactions are posted to the Accounting Service and Brokers are notified of balancing transactions
	List txList = balanceTimeslot(timeslot.currentTimeslot(), brokerList)

  }
  
  /**
   * Returns the difference between a broker's current market position and its net load.  
   * Note: market position is computed in MWh and net load is computed in kWh, conversion
   * is needed to compute the difference.
   *
   * @return a broker's current energy balance within its market. Pos for over-production,
   * neg for under-production
   */
  public BigDecimal brokerBalance(Broker broker){
	  return accountingService.getCurrentMarketPosition(broker) - 
	  		 (accountingService.getCurrentNetLoad(broker) / 1000.0)
  }
  
  /**
   * Generates a list of MarketTransactions that balance the overall market.  Transactions
   * are generated on a per-broker basis depending on the broker's balance within its own market.
   * 
   * @return List of MarketTransactions 
   */
  public List balanceTimeslot (Timeslot currentTimeslot, List brokerList) 
  {
	  List balancingMarketTxs = []
	  BigDecimal imbalanceQuantity
		
	  // Charge each broker an equal amount if marketBalance is neg, credit each broker if pos
	  for ( b in brokerList ){
		  imbalanceQuantity = brokerBalance(b)
		  if( imbalanceQuantity != 0.0 ){
			  balancingMarketTxs.add( accountingService.addMarketTransaction( b,
			  	 															  currentTimeslot,
																			  (imbalanceQuantity * imbalancePenaltyRate),
																			  imbalanceQuantity ) )
		  }
	  }
	  return balancingMarketTxs
  }

}
