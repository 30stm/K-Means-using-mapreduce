package Tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ClusterCenter implements WritableComparable<ClusterCenter> {

        private Vector center;
//        private Vector[] centers;

        public ClusterCenter() {
                super();
                this.center = null;
        }

        public ClusterCenter(ClusterCenter center) {
                super();
                this.center = new Vector(center.center);
        }

        public ClusterCenter(Vector center) {
                super();
                this.center = center;
        }
//
//        public ClusterCenter(Vector... center) {
//            super();
//            this.centers = center;
//    }
        
        public boolean converged(ClusterCenter c) {
                return compareTo(c) == 0 ? false : true;
        }

        public void write(DataOutput out) throws IOException {
                center.write(out);
        }
        

        
        public void readFields(DataInput in) throws IOException {
                this.center = new Vector();
                center.readFields(in);
        }

        public int compareTo(ClusterCenter o) {
                return center.compareTo(o.getCenter());
        }

        /**
         * @return the center
         */
        public Vector getCenter() {
                return center;
        }

        public Double getCenterValues(int i) {
            return center.getValues(i);
        }
        
        public String toString() {
                return center.toString();
        }

}
