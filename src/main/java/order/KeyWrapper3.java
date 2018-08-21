package order;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 封装要比较的Keys
 */
public class KeyWrapper3 implements Ordered<KeyWrapper3>, Serializable {
    private int firstKey;
    private int secondKey;

    public KeyWrapper3(int firstKey, int secondKey) {
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
     * firstKey从大到小，secondKey从小到大
     * @param that
     * @return
     */
    public int compare(KeyWrapper3 that) {
        if (this.getFirstKey() != that.getFirstKey()) {
            return that.getFirstKey() - this.getFirstKey();
        }
        return this.getSecondKey() - that.getSecondKey();
    }

    public boolean $less(KeyWrapper3 that) {
        if (this.getFirstKey() < that.getFirstKey()) {
            return true;
        } else if (this.getFirstKey() == that.getFirstKey() && this.getSecondKey() < that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public boolean $greater(KeyWrapper3 that) {
        if (this.getFirstKey() > that.getFirstKey()) {
            return true;
        } else if (this.getFirstKey() == that.getFirstKey() && this.getSecondKey() > that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public boolean $less$eq(KeyWrapper3 that) {
        if ($less(that) || this.getFirstKey() == that.getFirstKey() && this.getSecondKey() == that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public boolean $greater$eq(KeyWrapper3 that) {
        if ($greater(that) || this.getFirstKey() == that.getFirstKey() && this.getSecondKey() == that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public int compareTo(KeyWrapper3 that) {
        return compare(that);
    }
}
