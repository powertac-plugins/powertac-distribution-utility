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
import org.powertac.common.Competition
import org.powertac.common.Orderbook
import org.powertac.common.PluginConfig
import org.powertac.common.Timeslot
import org.powertac.common.TimeService
import org.powertac.common.interfaces.TimeslotPhaseProcessor

class DistributionUtilityService implements TimeslotPhaseProcessor
{
  static transactional = true

  def timeService
  def accountingService
  def competitionControlService
  def randomSeedService

  Random randomGen
  
  // Rate per kWH
  //BigDecimal imbalancePenaltyRate = 0.1
  
  BigDecimal distributionFee = 0.01
  BigDecimal balancingCost = 0.06
  BigDecimal defaultSpotPrice = 30.0 // per mwh
  
  int simulationPhase = 2 // after customer, before accounting
  
  /**
   * Computes actual distribution and balancing costs by random selection
   */
  void init (PluginConfig config)
  {
    competitionControlService?.registerTimeslotPhase(this, simulationPhase)
    BigDecimal distributionFeeMin = config.configuration['distributionFeeMin']?.toBigDecimal()
    BigDecimal distributionFeeMax = config.configuration['distributionFeeMax']?.toBigDecimal()
    BigDecimal balancingCostMin = config.configuration['balancingCostMin']?.toBigDecimal()
    BigDecimal balancingCostMax = config.configuration['balancingCostMax']?.toBigDecimal()
    long randomSeed = randomSeedService.nextSeed('DistributionUtilityService', 'du', 'model')
    randomGen = new Random(randomSeed)
    distributionFee = (distributionFeeMin +
                       randomGen.nextDouble() * (distributionFeeMax -
                                                 distributionFeeMin))
    balancingCost = (balancingCostMin +
                       randomGen.nextDouble() * (balancingCostMax -
                                                 balancingCostMin))
    
    log.info "Configured DU: distro fee = $distributionFee, balancing cost = $balancingCost"
  }

  void activate (Instant time, int phaseNumber)
  {
    List brokerList = Broker.findAllByWholesale (false)
    if ( brokerList == null ){
      log.error("Failed to retrieve retail broker list")
      return
    }

    // Run the balancing market
    // Transactions are posted to the Accounting Service and Brokers are notified of balancing transactions
    balanceTimeslot(Timeslot.currentTimeslot(), brokerList)
  }

  /**
   * Returns the difference between a broker's current market position and its net load.  
   * Note: market position is computed in MWh and net load is computed in kWh, conversion
   * is needed to compute the difference.
   *
   * @return a broker's current energy balance within its market. Pos for over-production,
   * neg for under-production
   */
  BigDecimal getMarketBalance(Broker broker)
  {
    return accountingService.getCurrentMarketPosition(broker) * 1000.0 -
            accountingService.getCurrentNetLoad(broker)
  }
  
  /**
   * Returns the spot market price - the clearing price for the current timeslot
   * in the most recent trading period.
   */
  BigDecimal getSpotPrice ()
  {
    BigDecimal result = defaultSpotPrice
    // most recent trade is determined by Competition parameters
    Competition comp = Competition.currentCompetition()
    int offset = comp.deactivateTimeslotsAhead
    Instant executed = new Instant(timeService.currentTime.millis - 
      offset * comp.timeslotLength * TimeService.MINUTE)
    // orderbooks have timeslot and execution time
    Orderbook ob = 
      Orderbook.findByDateExecutedAndTimeslot(executed, Timeslot.currentTimeslot())
    if (ob != null) {
      result = ob.clearingPrice
      if (result == null) {
        result = ob.determineClearingPrice()
      }
      if (result == null) {
        result = defaultSpotPrice
      }
    }
    else {
      log.info "null Orderbook"
    }
    return result / 1000.0 // convert to kwh
  }

  /**
   * Generates a list of Transactions that balance the overall market.  Transactions
   * are generated on a per-broker basis depending on the broker's balance within its own market.
   * 
   * @return List of MarketTransactions 
   */
  void balanceTimeslot (Timeslot currentTimeslot, List brokerList)
  {
    List brokerBalance = brokerList.collect { broker ->
      getMarketBalance(broker)
    }
    List balanceCharges = 
      computeNonControllableBalancingCharges(brokerList, brokerBalance)

    def chargeInfo = []
    // Add transactions for distribution and balancing
    brokerList.eachWithIndex { broker, b ->
      // first, charge for distribution
      def netLoad = accountingService.getCurrentNetLoad(broker)
      accountingService.addDistributionTransaction(broker, netLoad, netLoad * distributionFee)

      // then charge for balancing
      def balanceCharge = balanceCharges[b]
      chargeInfo << [broker.username, netLoad, balanceCharge]
      if( balanceCharge != 0.0 ){
        accountingService.addBalancingTransaction(broker, brokerBalance[b], balanceCharge)
      }
    }
    log.info "balancing charges: ${chargeInfo}"
  }

  List computeNonControllableBalancingCharges(List brokerList, List balanceList)
  {
    QuadraticSolver myQuadraticSolver
    BasicMatrix[] inputMatrices = new BigMatrix[6]
    int numOfBrokers = brokerList.size()
    double P = getSpotPrice() // market price in day ahead market
    double c0 = balancingCost // cost function per unit of energy produced by the DU
    double x = 0 // total market balance
    double[] brokerBalance = new double[numOfBrokers]

    double[][] AE = new double[1][numOfBrokers]  // equality constraints lhs
    double[][] BE = new double[1][1]  // equality constraints rhs
    double[][] Q = new double[numOfBrokers][numOfBrokers]  // quadratic objective
    double[][] C = new double[numOfBrokers][1]  // linear objective
    double[][] AI = new double[numOfBrokers + 1][numOfBrokers]  // inequality constraints lhs
    double[][] BI = new double[numOfBrokers + 1][1]  // inequality constraints rhs

    for(int i = 0; i < numOfBrokers; i++){
      x += brokerBalance[i] = balanceList[i]
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
      solutionList.add(result.doubleValue(i, 0))
    }
    return solutionList
  }

}
