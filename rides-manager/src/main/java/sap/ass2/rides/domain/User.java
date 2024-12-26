package sap.ass2.rides.domain;

import sap.ddd.Entity;

public record User(String id, int credit, double x, double y) implements Entity<String> {
    public User updateCredit(int newCredit) {
        return new User(this.id, newCredit, x, y);
    }

    @Override
    public String getId() {
        return id;
    }

    public User applyEvent(UserEvent event) {
        if (this.id.equals(event.userId())) {
            return new User(id, credit+event.creditDelta(), x + event.deltaX(), y + event.deltaY());
        }
        return this;
    }

    public static User from(UserEvent event){
        return new User(event.userId(), event.creditDelta(), event.deltaX(), event.deltaY());
    }
}
