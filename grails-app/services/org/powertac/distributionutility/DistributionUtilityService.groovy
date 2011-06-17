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
//import java.math.BigDecimal

import org.ojalgo.matrix.BasicMatrix
import org.ojalgo.matrix.BigMatrix
import org.ojalgo.optimisation.quadratic.QuadraticSolver;
import org.ojalgo.optimisation.quadratic.QuadraticSolver.Builder
import org.ojalgo.type.StandardType

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
  public BigDecimal getMarketBalance(Broker broker){
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
    BigDecimal balance

    // Charge each broker an equal amount if marketBalance is neg, credit each broker if pos
    // TODO need to rewrite so that all brokers receive a market transaction, even if they are not charged
    for ( b in brokerList ){
      balance = getMarketBalance(b)
      if( balance != 0.0 ){
        balancingMarketTxs.add( accountingService.addMarketTransaction( b,
            currentTimeslot,
            (balance * imbalancePenaltyRate),
            balance ) )
      }
    }
    return balancingMarketTxs
  }

  public List computeNonControllableBalancingCharges(List brokerList)
  {
    QuadraticSolver myQuadraticSolver
    BasicMatrix[] inputMatrices = new BigMatrix[6]
    int numOfBrokers = brokerList.size()
    double P = 3 // market price in day ahead market, TODO: find out where to get it
    double c0 = 6 // cost function per unit of energy produced by the DU, TODO: find out where to get it
    double x = 0 // total market balance
    double[] brokerBalance = new double[numOfBrokers]

    double[][] AE = new double[1][numOfBrokers]  // equality constraints lhs
    double[][] BE = new double[1][1]  // equality constraints rhs
    double[][] Q = new double[numOfBrokers][numOfBrokers]  // quadratic objective
    double[][] C = new double[numOfBrokers][1]  // linear objective
    double[][] AI = new double[numOfBrokers + 1][numOfBrokers]  // inequality constraints lhs
    double[][] BI = new double[numOfBrokers + 1][1]  // inequality constraints rhs

    for(int i = 0; i < numOfBrokers; i++){
      x += brokerBalance[i] = getMarketBalance(brokerList[i])
    }

    // Initialize all the matrices with the proper values
    for(int i = 0; i < numOfBrokers; i++){
      AE[0][i] = 0
      C[i][0] = brokerBalance[i] * P
      for(int j = 0; j < numOfBrokers; j++){
        if(i == j){
          Q[i][j] = 1
          AI[i][j] = -1
        }
        else{
          Q[i][j] = 0
          AI[i][j] = 0
        }
      }
      AI[numOfBrokers][i] = -1
      BI[i][0] = brokerBalance[i] * P
    }
    BE[0][0] = 0
    BI[numOfBrokers][0] = x * c0

    // format the above data for the solver
    inputMatrices[0] = BigMatrix.FACTORY.copy(AE)
    inputMatrices[1] = BigMatrix.FACTORY.copy(BE)
    inputMatrices[2] = BigMatrix.FACTORY.copy(Q)
    inputMatrices[3] = BigMatrix.FACTORY.copy(C)
    inputMatrices[4] = BigMatrix.FACTORY.copy(AI)
    inputMatrices[5] = BigMatrix.FACTORY.copy(BI)

    // create a new builder to initialize the solver with Q and C
    final Builder tmpBuilder = new QuadraticSolver.Builder(inputMatrices[2].round(StandardType.DECIMAL_032).toPrimitiveStore(),
        inputMatrices[3].round(StandardType.DECIMAL_032).negate().toPrimitiveStore())
    // input the equality constraints
    tmpBuilder.equalities(inputMatrices[0].round(StandardType.DECIMAL_032).toPrimitiveStore(),
        inputMatrices[1].round(StandardType.DECIMAL_032).toPrimitiveStore())
    // input the inequality constraints
    tmpBuilder.inequalities(inputMatrices[4].round(StandardType.DECIMAL_032).toPrimitiveStore(),
        inputMatrices[5].round(StandardType.DECIMAL_032).toPrimitiveStore())
    // configure the solver
    myQuadraticSolver = tmpBuilder.build()

    // solve the system, and return the result as a list of balancing charges
    BasicMatrix result = myQuadraticSolver.solve().getSolution()
    List solutionList = []
    for(int i = 0; i < numOfBrokers; i++){
      solutionList.add(result.toScalar(i, 0).getReal())
    }
    return solutionList
  }

}
