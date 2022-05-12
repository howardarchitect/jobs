package com.roark.model;

import java.math.BigDecimal;

public class Product {

    private Integer productID;
    private String prodName;
  
    private BigDecimal price;
    private Integer unit;
    private String ProductDesc;

    @Override
    public String toString() {
        return "Product{" +
                "productID=" + productID +
                ", productName='" + prodName + '\'' +
                ", ProductDesc='" + ProductDesc + '\'' +
                ", price=" + price +
                ", unit=" + unit +
                '}';
    }

    public Integer getProductID() {
        return productID;
    }

    public void setProductID(Integer productID) {
        this.productID = productID;
    }

    public String getProdName() {
        return prodName;
    }

    public void setProdName(String prodName) {
        this.prodName = prodName;
    }

    public String getProductDesc() {
        return ProductDesc;
    }

    public void setProductDesc(String productDesc) {
        ProductDesc = productDesc;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public Integer getUnit() {
        return unit;
    }

    public void setUnit(Integer unit) {
        this.unit = unit;
    }
}
