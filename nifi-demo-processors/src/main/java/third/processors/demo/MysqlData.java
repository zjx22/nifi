package third.processors.demo;

public class MysqlData {
    String name=null;
    Double lower=0.0;
    Double upper=0.0;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getLower() {
        return lower;
    }

    public void setLower(Double lower) {
        this.lower = lower;
    }

    public Double getUpper() {
        return upper;
    }

    public void setUpper(Double upper) {
        this.upper = upper;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    int level=0;
}
