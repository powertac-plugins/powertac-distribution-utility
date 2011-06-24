package org.powertac.distributionutility

import java.util.List;

import org.powertac.common.Competition;
import org.powertac.common.PluginConfig
import org.powertac.common.interfaces.InitializationService

class DistributionUtilityInitializationService
implements InitializationService
{

  static transactional = true
  
  def distributionUtilityService // autowire

  @Override
  public void setDefaults ()
  {
    PluginConfig config = new PluginConfig(roleName:'DistributionUtility', name: '',
        configuration: ['distributionFeeMin': '0.003',
                        'distributionFeeMax': '0.03',
                        'balancingCostMin': '0.02',
                        'balancingCostMax': '0.06',
                        'defaultSpotPrice': '50.0'])
    config.save()
  }

  @Override
  public String initialize (Competition competition,
                            List<String> completedInits)
  {
    PluginConfig duConfig = PluginConfig.findByRoleName('DistributionUtility')
    if (duConfig == null) {
      log.error "PluginConfig for DistributionUtility does not exist"
      return 'fail'
    }
    else {
      distributionUtilityService.init(duConfig)
      return 'DistributionUtility'
    }
  }
}
