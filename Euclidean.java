package Tools;

import Tools.ClusterCenter;
import Tools.Vector;

public class Euclidean {
    public static final double E_Distance(ClusterCenter v1, Vector v2) {
            double sum = 0;
            int dimension = v2.dimension();
            for (int i = 0; i < dimension; i++) {
                    sum += Math.pow(v2.getValues(i)- v1.getCenterValues(i),2);
            }
            return Math.sqrt(sum);
    }
}
