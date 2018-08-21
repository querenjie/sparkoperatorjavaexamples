package order;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 封装要比较的Keys
 */
public class KeyWrapper implements Ordered<KeyWrapper>, Serializable {
    private int firstKey;
    private int secondKey;

    public KeyWrapper(int firstKey, int secondKey) {
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
     * 从小到大的排序
     * @param that
     * @return
     */
    public int compare(KeyWrapper that) {
        if (this.getFirstKey() != that.getFirstKey()) {
            return this.getFirstKey() - that.getFirstKey();
        }
        return this.getSecondKey() - that.getSecondKey();
    }

    public boolean $less(KeyWrapper that) {
        if (this.getFirstKey() < that.getFirstKey()) {
            return true;
        } else if (this.getFirstKey() == that.getFirstKey() && this.getSecondKey() < that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public boolean $greater(KeyWrapper that) {
        if (this.getFirstKey() > that.getFirstKey()) {
            return true;
        } else if (this.getFirstKey() == that.getFirstKey() && this.getSecondKey() > that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public boolean $less$eq(KeyWrapper that) {
        if ($less(that) || this.getFirstKey() == that.getFirstKey() && this.getSecondKey() == that.getSecondKey()) {
            return true;
        }
        return false;
    }

    public boolean $greater$eq(KeyWrapper that) {
        if ($greater(that) || this.getFirstKey() == that.getFirstKey() && this.getSecondKey() == that.getSecondKey()) {
            return true;
        }
        return false;
    }

    /**
     * 从小到大的排序
     * @param that
     * @return
     */
    public int compareTo(KeyWrapper that) {
        if (this.getFirstKey() != that.getFirstKey()) {
            return this.getFirstKey() - that.getFirstKey();
        }
        return this.getSecondKey() - that.getSecondKey();
    }
}
