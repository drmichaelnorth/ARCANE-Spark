
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
package gov.anl.cue.arcane.spark

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import gov.anl.cue.arcane.engine.matrix.MatrixEngine
import gov.anl.cue.arcane.engine.matrix.MatrixModel
import org.apache.spark.SparkConf

object ARCANEEngine {
  
	/**  Define the  model runner.
   *
   * @param runType the kind of run to complete
   * @param usePopulationSize use the population size as
   *        read from the Matrix Engine file in the URL
   * @param fileName the Matrix Engine file to load
   * @param steps the number of steps to execute
   */
  def run(runType: String, usePopulationSize: Boolean,
      fileName: String, steps: Int): Array[Double] = {
    
    // Complete the requested runs.
    this.runToFindPopulation(runType, usePopulationSize, fileName, steps).keys.collect
    
  }
    
	/**  Define the  model runner.
   *
   * @param runType the kind of run to complete
   * @param usePopulationSize use the population size as
   *        read from the Matrix Engine file in the URL
   * @param fileName the Matrix Engine file to load
   * @param steps the number of steps to execute
   */
  def runToFindPopulation(runType: String, usePopulationSize: Boolean,
      fileName: String, steps: Int): RDD[(Double, MatrixModel)] = {
    
        // Read in the matrix engine.
    val matrixEngine = MatrixEngine.read(fileName)
       
    // Create a Spark configuration.
    val configuration = new SparkConf().setAppName("ARCANEEngine")
    
    // Set the URL.
    if (usePopulationSize) {
      configuration.setMaster(runType + "[" +
          matrixEngine.populationSize + "]")
    } else {
      configuration.setMaster(runType)
    }
    
    // Create a Spark context.
    val context = new SparkContext(configuration)

    // Broadcast the matrix engine.   
    val broadcastEngine = context.broadcast(matrixEngine)

    // Setup parallel partitions.
    val partitions = context.parallelize(0 to (matrixEngine.populationSize - 1))

    // Load the population.
    var population = loadPopulation(partitions, broadcastEngine)
    
    // Complete the needed evolutionary cycles.
    for (step <- 1 to steps) {
      
      // Step the population forward.
      population = fillPopulation(
        killPopulation(population, broadcastEngine, context),
        broadcastEngine, context)
        
    }
    
    // Return the collected results.
   return (population)
    
  }
  
	/**  Define the population loader.
   *
   * @param partitions the partitions to process
   * @param broadcastEngine the shared Matrix Engine
   */
  def loadPopulation(partitions: RDD[Int],
      broadcastEngine: Broadcast[MatrixEngine]):
      RDD[(Double, MatrixModel)] = {
    
    // Load the given population.
    partitions.map(loadModel(broadcastEngine))

  }
  
	/**  Define the model loader.
   *
   * @param broadcastEngine the shared Matrix Engine
   * @param partitionIndex the partition to process
   */
  def loadModel(broadcastEngine: Broadcast[MatrixEngine])
    (partitionIndex: Int): (Double, MatrixModel) = { 
      
      // Get the broadcast value.
      val modelIndex = (partitionIndex % broadcastEngine.value
          .inputPopulation.size())
          
      // Copy the model.
      val model = broadcastEngine.value.inputPopulation
          .get(modelIndex).copy
      
      // Set the matrix engine, if needed.
      if (model.getMatrixEngine() == null) {
        model.matrixEngine = broadcastEngine.value
      }
      
      // Find the fitness value and return the results.
      (model.calculateFitnessValue, model)
       
  }
  
	/**  Define the population killer.
   *
   * @param broadcastEngine the shared Matrix Engine
   * @param context the shared Spark context
   */
  def killPopulation(population: RDD[(Double, MatrixModel)],
    broadcastEngine: Broadcast[MatrixEngine], context: SparkContext):
    RDD[(Double, MatrixModel)] = {
      
      // Sample the population.
      val sample = population.sample(false,
        Math.min(100.0, broadcastEngine.value.populationSize) /
        broadcastEngine.value.populationSize).sortByKey().
        keys.collect()
        
      // Find the sample cutoff index.
      val killIndex = (100 * broadcastEngine.value.killFraction).toInt
      
      // Check to make sure the cutoff index is within the population.
      if (killIndex < sample.length) {
        
        // Find the cutoff value using the cutoff index.
        val killValue = sample(killIndex)
        
        // Broadcast the cutoff value.
        val broadcastKillValue = context.broadcast(killValue)
        
        // Use the cutoff point and sort the list.
        population.filter(killModel(broadcastKillValue))
        
      } else {
        
        // Return the default result.
        population
        
      }
        
      
  }
  
  /**  Define the model killer.
   *
   * @param broadcastKillValue the shared killing threshold
   * @param tuple the item to check
   */
  def killModel(broadcastKillValue: Broadcast[Double])
    (tuple: (Double, MatrixModel)): Boolean = {
    
    // Check the filter threshold.
    (tuple._1 >= broadcastKillValue.value)
    
  }
  
	/**  Define the population filler.
   *
   * @param broadcastEngine the shared Matrix Engine
   * @param context the shared Spark context
   */
  def fillPopulation(population: RDD[(Double, MatrixModel)],
    broadcastEngine: Broadcast[MatrixEngine], context: SparkContext):
    RDD[(Double, MatrixModel)] = {
      
     // Create and split a new population.
     val newPopulation = population.filter(
       selectModel(broadcastEngine)).randomSplit(
       Array(1 - broadcastEngine.value.crossoverProbability,
       broadcastEngine.value.crossoverProbability),
       broadcastEngine.value.getRandomNumberFromTo(0, Long.MaxValue).toLong)
         
     // Mutate the mutation branch of the population and then
     // calculate the new fitness value.
     val mutationPopulation = newPopulation(0).map({ tuple =>
       
       // Mutate the tuple.
       tuple._2.mutate()
       
       // Return the new fitness value.
       (tuple._2.calculateFitnessValue.toDouble, tuple._2)
       
     })
 
     // Crossover the crossover branch of the population and then
     // calculate the new fitness value.  
     val crossOverPopulation = joinModels(newPopulation(1),
       shuffleModels(broadcastEngine, newPopulation(1))).map({ tuple =>
         
       // Make sure that the engines are defined.
       if (tuple._1._2.getMatrixEngine() == null) {
         tuple._1._2.matrixEngine = broadcastEngine.value
       }
       if (tuple._2._2.getMatrixEngine() == null) {
         tuple._2._2.matrixEngine = broadcastEngine.value
       }

       // Crossover the two given models, if possible, while avoiding nulls.
       var  childModel = tuple._1._2.crossOver(tuple._2._2)
       
       // Return the new fitness value with the given model.
       (childModel.calculateFitnessValue.toDouble, childModel)
       
     })
     
      //  and merge in the new populations.
     population.union(mutationPopulation)
     population.union(crossOverPopulation)
 
  }

  /**  Define the model selector.
   *
   * @param broadcastEngine the shared Matrix Engine
   * @param tuple the item to filter
   */
  def selectModel(broadcastEngine: Broadcast[MatrixEngine])
    (tuple: (Double, MatrixModel)): Boolean = {
    
    // Check the filter threshold.
    (broadcastEngine.value.getRandomNumberFromTo(0.0, 1.0) <=
      broadcastEngine.value.killFraction)
    
  }
  
  /**  Define the model shuffler.
   *
   * @param broadcastEngine the shared Matrix Engine
   * @param a the model to shuffle
   */
  def shuffleModels[A: reflect.ClassTag](broadcastEngine:
      Broadcast[MatrixEngine], a: RDD[A]): RDD[A] = {
    
    // Shuffle the given models.
    val shuffled = a.map(broadcastEngine.value
        .getRandomNumberFromTo(0, Integer.MAX_VALUE).toInt -> _).sortByKey()
    
    // Return the results.
    shuffled.values

  }

  /**  Define the model joiner.
   *
   * @param a the first model
   * @param b the second model
   */
  def joinModels[A: reflect.ClassTag, B](a: RDD[A], b: RDD[B]):
    RDD[(A, B)] = {
    
    // Get the first indexed list.
    val aWithIndices = a.zipWithIndex.map({ case (x, y) => (y, x) })
    
    // Get the second indexed list.
    val bWithIndices = b.zipWithIndex.map({ case (x, y) => (y, x) })
    
    // Join and return the results.
    aWithIndices.join(bWithIndices).values
    
  }

}