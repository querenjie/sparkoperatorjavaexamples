package order;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 封装要比较的Keys
 */
public class KeyWrapper2 implements Ordered<KeyWrapper2>, Serializable {
    private int firstKey;
    private int secondKey;

    public KeyWrapper2(int firstKey, int secondKey) {
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
     * 两个key都是从大到小排序
     * @param that
     * @return
     */
    public int compare(KeyWrapper2 that) {
        if (this.getFirstKey() != that.getFirstKey()) {
            return that.getFirstKey() - this.getFirstKey();
        }
        return that.getSecondKey() - this.getSecondKey();
    }

    public boolean $less(KeyWrapper2 that) {
        if (this.getFirstKey() < that.getFirstKey()) {
            return true;
        } else if (this.getFirstKey() == that.getFirstKey() && this.getSecondKey() < that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public boolean $greater(KeyWrapper2 that) {
        if (this.getFirstKey() > that.getFirstKey()) {
            return true;
        } else if (this.getFirstKey() == that.getFirstKey() && this.getSecondKey() > that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public boolean $less$eq(KeyWrapper2 that) {
        if ($less(that) || this.getFirstKey() == that.getFirstKey() && this.getSecondKey() == that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public boolean $greater$eq(KeyWrapper2 that) {
        if ($greater(that) || this.getFirstKey() == that.getFirstKey() && this.getSecondKey() == that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public int compareTo(KeyWrapper2 that) {
        return compare(that);
    }
}
