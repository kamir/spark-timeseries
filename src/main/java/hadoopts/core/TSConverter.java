/*
 * The TSConverter only works on individual TimeSeries objects.
 */
package hadoopts.core;

import java.util.logging.Logger;
import org.apache.commons.math3.fitting.WeightedObservedPoints;

/**
 *
 * @author training
 */
public class TSConverter {
    
    
    
    private static final Logger LOG = Logger.getLogger(TSConverter.class.getName());

    // prepares data for polynomial fit using Apache Commons Math3 ...
    public WeightedObservedPoints getWeightedObservedPoints(TimeSeries ts) {
        
        WeightedObservedPoints obs = new WeightedObservedPoints();
        
        double[][] data = ts.getData();
        int z=ts.getXValues().size() ;
        
        for(int i=0; i<z; i++) {
            obs.add(data[i][0],data[i][1]);
        }
        
        return obs;     
    }
     
    
}
