/*
 * Copyright © 2016, UChicago Argonne, LLC
 * All Rights Reserved
 * ARCANE (ANL-SF-15-108)
 * Michael J. North, Argonne National Laboratory
 * Pam Sydelko, Argonne National Laboratory
 * Ignacio Martinez-Moyano
 * 
 * OPEN SOURCE LICENSE
 * 
 * Under the terms of Contract No. DE-AC02-06CH11357 with UChicago
 * Argonne, LLC, the U.S. Government retains certain rights in this
 * software.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1.	Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer. 
 * 2.	Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 * 3.	Neither the names of UChicago Argonne, LLC or the Department of Energy
 *      nor the names of its contributors may be used to endorse or promote
 *      products derived from this software without specific prior written
 *      permission. 
 *  
 * ****************************************************************************
 * DISCLAIMER
 * 
 * THE SOFTWARE IS SUPPLIED “AS IS” WITHOUT WARRANTY OF ANY KIND.
 * 
 * NEITHER THE UNTED STATES GOVERNMENT, NOR THE UNITED STATES DEPARTMENT OF
 * ENERGY, NOR UCHICAGO ARGONNE, LLC, NOR ANY OF THEIR EMPLOYEES, MAKES ANY
 * WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LEGAL LIABILITY OR
 * RESPONSIBILITY FOR THE ACCURACY, COMPLETENESS, OR USEFULNESS OF ANY
 * INFORMATION, DATA, APPARATUS, PRODUCT, OR PROCESS DISCLOSED, OR REPRESENTS
 * THAT ITS USE WOULD NOT INFRINGE PRIVATELY OWNED RIGHTS.
 * 
 ******************************************************************************
 *
 * @author Michael J. North
 * @version 1.0
 * 
*/
package gov.anl.cue.arcane.test

import scala.Ordering

import org.junit.Test

import gov.anl.cue.arcane.engine.matrix.MatrixEngine
import gov.anl.cue.arcane.engine.matrix.MatrixModel
import gov.anl.cue.arcane.spark.ARCANEEngine

// Define the main object.
class testRun {

  def run(inputFile: String, outputFolder: String, outputCount: Int, steps: Int):
   Array[(Double, MatrixModel)] = {

    // Run the scenario.
    val population = ARCANEEngine.runToFindPopulation("local", true, inputFile, 5)
    
    // Collect the results.
    val models = population.takeOrdered(outputCount)(Ordering[Double].on(_._1))

    // Save the results.
    for (i <- 0 to (outputCount - 1)) {
      
      // Make the next file name.
      var outputFile = outputFolder + "//MatrixModel_" + (i + 1) + ".xlsx"
      
      // Save the next model.
      models(i)._2.write(outputFile)
      
    }
    
    // Return the results.
    return (models)

  }

	/**  Define a production run method.
   *
   */
  @Test
  def testMainRun() {
    
    // Request a run.
    var models = run("input//test_scenario", "input//test_scenario//output", 10, 60)    

    // Check the results.
    for (i <- 0 to (models.length - 1)) {
      
      // Check the next model fitness value.
      assert(models(0)._1 == 3.25)
      
    }
    
  }

}