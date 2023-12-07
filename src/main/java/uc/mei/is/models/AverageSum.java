package uc.mei.is.models;

public class AverageSum {
    private float total;
    private int quant;
    private String type;

    public AverageSum(String type) {
        this.total = 0;
        this.quant = 0;
        this.type = type;
    }

    public float getTotal() {
        return total;
    }

    public void setTop(float total) {
        this.total = total;
    }

    public int getQuant() {
        return quant;
    }

    public void setQuant(int quant) {
        this.quant = quant;
    }

    public void addQuant(int quant) {
        this.quant += quant;
    }

    public void addTotal(float total) {
        this.total += total;
    }

    public float getAverage() {
        return total/quant;
    }

}
