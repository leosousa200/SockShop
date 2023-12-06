package uc.mei.is.models;

public class RecordInfo {
    private String name;
    private String type;
    private float price;
    private int sockId;
    private int supplierId;

    public RecordInfo(){}

    public RecordInfo(String name, String type, float price, int sockId, int supplierId) {
        this.name = name;
        this.type = type;
        this.price = price;
        this.sockId = sockId;
        this.supplierId = supplierId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getSockId() {
        return sockId;
    }

    public void setSockId(int sockId) {
        this.sockId = sockId;
    }

    public int getSupplierId() {
        return supplierId;
    }

    public void setSupplierId(int supplierId) {
        this.supplierId = supplierId;
    }
}
