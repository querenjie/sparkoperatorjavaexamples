package order;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 封装要比较的Keys
 */
public class KeyWrapper4 implements Ordered<KeyWrapper4>, Serializable {
    private int firstKey;
    private int secondKey;

    public KeyWrapper4(int firstKey, int secondKey) {
        this.firstKey = firstKey;
        this.secondKey = secondKey;
    }

    public int getFirstKey() {
        return firstKey;
    }

    public void setFirstKey(int firstKey) {
        this.firstKey = firstKey;
    }

    public int getSecondKey() {
        return secondKey;
    }

    public void setSecondKey(int secondKey) {
        this.secondKey = secondKey;
    }

    /**
     * firstKey从小到大，secondKey从大到小
     * @param that
     * @return
     */
    public int compare(KeyWrapper4 that) {
        if (this.getFirstKey() != that.getFirstKey()) {
            return this.getFirstKey() - that.getFirstKey();
        }
        return that.getSecondKey() - this.getSecondKey();
    }

    public boolean $less(KeyWrapper4 that) {
        if (this.getFirstKey() < that.getFirstKey()) {
            return true;
        } else if (this.getFirstKey() == that.getFirstKey() && this.getSecondKey() < that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public boolean $greater(KeyWrapper4 that) {
        if (this.getFirstKey() > that.getFirstKey()) {
            return true;
        } else if (this.getFirstKey() == that.getFirstKey() && this.getSecondKey() > that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public boolean $less$eq(KeyWrapper4 that) {
        if ($less(that) || this.getFirstKey() == that.getFirstKey() && this.getSecondKey() == that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public boolean $greater$eq(KeyWrapper4 that) {
        if ($greater(that) || this.getFirstKey() == that.getFirstKey() && this.getSecondKey() == that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public int compareTo(KeyWrapper4 that) {
        return compare(that);
    }
}
