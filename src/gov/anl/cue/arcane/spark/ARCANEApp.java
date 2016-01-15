/*
 * 
 */
package gov.anl.cue.arcane.spark;

/**
 * The Class ARCANEApp.
 */
public class ARCANEApp {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		
	    // Test the engine.
	    System.out.println(ARCANEEngine.
	    		run("local", true, "input//matrixscenario_1", 5));
		
	}

}
