package sap.ass2.admingui.library;

/** Interface for who listens to the users events (admin GUI). */
public interface UserEventObserver {
    void userUpdated(String userID, int creditChange, double deltaX, double deltaY);
}
