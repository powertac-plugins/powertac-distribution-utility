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

import org.powertac.common.interfaces.DistributionUtility
import org.powertac.common.Timeslot
import org.powertac.common.command.CashDoUpdateCmd
import org.powertac.common.command.PositionDoUpdateCmd

class DistributionUtilityService implements DistributionUtility {

  static transactional = true

  List balanceTimeslot(Timeslot currentTimeslot) {
    List commands = []
    commands.add(new CashDoUpdateCmd())
    commands.add(new PositionDoUpdateCmd())
    log.info "balanceTimeslot commands ${commands}"
    return commands
  }
}
