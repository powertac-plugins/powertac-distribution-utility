package org.powertac.distributionutility

import grails.test.*
import org.powertac.common.Competition
import org.powertac.common.PluginConfig

class DistributionUtilityInitializationServiceTests extends GrailsUnitTestCase 
{
  def distributionUtilityService
  def distributionUtilityInitializationService
  Competition comp
   
  protected void setUp () 
  {
    super.setUp()
    PluginConfig.findByRoleName('DistributionUtility')?.delete()
    
    // create a Competition, needed for initialization
    if (Competition.count() == 0) {
      comp = new Competition(name: 'du-init-test')
      assert comp.save()
    }
    else {
      comp = Competition.list().first()
    }
    distributionUtilityInitializationService.setDefaults()
  }

  protected void tearDown () 
  {
    super.tearDown()
  }

  void testSetDefaults () 
  {
    PluginConfig config = PluginConfig.findByRoleName('DistributionUtility')
    assertNotNull("config not null", config)
    assertEquals("correct field", '0.03', config.configuration['distributionFeeMax'])
  }
  
  // this test fails when you run it, but not in debug mode??
  void testInit ()
  {
    assertEquals("same DU", distributionUtilityService, distributionUtilityInitializationService.distributionUtilityService)
    assertEquals("correct default", 0.04, distributionUtilityService.balancingCost)
    def result = distributionUtilityInitializationService.initialize(comp, [])
    assertNotNull("non-null result", result)
    assertEquals("correct result", 'DistributionUtility', result)
    assertFalse("not default", 0.04 != distributionUtilityService.balancingCost)
    assertTrue("in range 1", 0.02 <= distributionUtilityService.balancingCost)
    assertTrue("in range 2", 0.06 >= distributionUtilityService.balancingCost)
  }
}
