package sap.ass2.admingui.domain;

public record User(String id, int credit, double x, double y) {
    public User update(int newCredit, double deltaX, double deltaY) {
        return new User(this.id, credit + newCredit, x + deltaX, y + deltaY);
    }
}
