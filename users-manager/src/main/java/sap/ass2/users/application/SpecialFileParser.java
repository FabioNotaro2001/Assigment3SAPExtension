package sap.ass2.users.application;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import sap.ass2.users.domain.UserEvent;

public class SpecialFileParser {
    private Path path;

    public SpecialFileParser(Path path) {
        this.path = path;
    } 

    public synchronized List<UserEvent> getUserEvents() throws IOException {
        List<UserEvent> events = new ArrayList<>();
        try (Scanner scanner = new Scanner(this.path)) {
            while (scanner.hasNextLine()){
                String event = scanner.nextLine();
                var parts = event.split(" ");
                events.add(new UserEvent(parts[0], Integer.parseInt(parts[1]), Double.parseDouble(parts[2]), Double.parseDouble(parts[3])));
            }
            return events;
        }
    }

    public synchronized void addEvent(UserEvent event) throws IOException {
        try (var writer = new BufferedWriter(new FileWriter(this.path.toString(), true))) {
            writer.append(String.format("%s %d %f %f", event.userId(), event.creditDelta(), event.deltaX(), event.deltaY()));
            writer.newLine();
        }
    }
}

