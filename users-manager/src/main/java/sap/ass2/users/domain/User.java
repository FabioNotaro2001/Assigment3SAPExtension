package sap.ass2.users.domain;

import sap.ddd.Entity;

public class User implements Entity<String> {
    private String id;      
    private int credit;     
    private double x;
    private double y;

    public User(String id) {
        this.id = id;         
        this.credit = 0;   
        this.x = 0;
        this.y = 0;    
    }

    public User(String id, int credit) {
        this.id = id;           
        this.credit = credit;   
    }

    public String getId() {
        return id;              
    }

    public int getCredit() {
        return credit;          
    }

    public double getX(){
        return this.x;
    }

    public double getY(){
        return this.y;
    }

    public void rechargeCredit(int deltaCredit) {
        credit += deltaCredit;  
    }

    public void decreaseCredit(int amount) {
        credit -= amount;       
        if (credit < 0) {      
            credit = 0;         
        }
    }

    public void move(double newX, double newY){
        this.x = newX;
        this.y = newY;
    }

    public String toString() {
        return "{ id: " + id + ", credit: " + credit +  ", position: (" + this.x +", " + this.y +") }";
    }

    public User applyEvent(UserEvent event) {
        if (this.id.equals(event.userId())) {
            this.credit += event.creditDelta();
            this.x += event.deltaX();
            this.y += event.deltaY();
        }
        return this;
    }
}
