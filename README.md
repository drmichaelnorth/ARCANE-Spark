The AnticipatoRy Complex Adaptive Network Extrapolation (ARCANE) library
is a genetic algorithm system for automatically generating system dynamics
models that differ in both their embedded parameters and their fundamental
equations while maintaining dimensional consistency. It is intended to be
used for anticipatory modeling of systems such as complex networks. This
is an Apache Spark harness for ARCANE.

The system can be activated using a one line call:

	    ARCANEEngine.run("local", true, "input//matrixscenario_1", 5)

The parameters are as follows:

* The string for the Apache Spark URL or the kind of run to complete
* A boolean for the use the population size as read from the Matrix Engine file in the URL
* The path to the Matrix Engine file to load
* The number of genetic algorithm steps to execute

A reference for the underlying ARCANE API can be found
[here](http://drmichaelnorth.github.io/ARCANE-Spark/docs).