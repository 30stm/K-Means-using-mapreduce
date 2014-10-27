package Tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

public class Vector implements WritableComparable<Vector> {

        private double[] vector;

        public Vector() {
                super();
        }

        public Vector(Vector v) {
                super();
                int l = v.vector.length;
                this.vector = new double[l];
                System.arraycopy(v.vector, 0, this.vector, 0, l);
        }
        
        public Vector(double x, double y, double z, double i) {
            super();
            this.vector = new double[] {x, y, z, i};
    }
        
        public Vector(double[] values) {
            super();
            this.vector = values;
    }

        public void write(DataOutput out) throws IOException {
                out.writeInt(vector.length);
                for (int i = 0; i < vector.length; i++)
                        out.writeDouble(vector[i]);
        }

        public void readFields(DataInput in) throws IOException {
                int size = in.readInt();
                vector = new double[size];
                for (int i = 0; i < size; i++)
                        vector[i] = in.readDouble();
        }
        public int compareTo(Vector o) {

                boolean equals = true;
                for (int i = 0; i < vector.length; i++) {
                        if (vector[i] != o.vector[i]) {
                                equals = false;
                                break;
                        }
                }
                if (equals) 
                        return 0;
                else
                        return 1;
        }

        public double[] getVector() {
                return vector;
        }
        
        public double[] add(double[] vector2) {
            for(int i=0;i<vector.length;i++){
            	vector[i] += vector2[i];
            }
            return vector;
        }

        public double[] divide(int p) {
            for(int i=0;i<vector.length;i++){
            	vector[i] = vector[i] / p;
            }
            return vector;
        }
        
        public double getValues(int i) {
            return vector[i];
        }
        
        
        public void setVector(double[] vector) {
                this.vector = vector;
        }
        
        public int dimension() {
            return this.vector.length;
    }

        @Override
        public String toString() {
                return Arrays.toString(vector);
        }

}
