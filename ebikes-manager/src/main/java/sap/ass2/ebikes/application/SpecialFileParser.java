package sap.ass2.ebikes.application;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;

import sap.ass2.ebikes.domain.Ebike.EbikeState;
import sap.ass2.ebikes.domain.EbikeEvent;
import sap.ass2.ebikes.domain.V2d;

public class SpecialFileParser {
    private Path path;

    public SpecialFileParser(Path path) {
        this.path = path;
    } 

    public synchronized List<EbikeEvent> getEbikeEvents() throws IOException {
        List<EbikeEvent> events = new ArrayList<>();
        try (Scanner scanner = new Scanner(this.path)) {
            while(scanner.hasNextLine()){
                String event = scanner.nextLine();
                var parts = event.split(" ");
                try{
                    String ebikeId = parts[0];
                    Optional<EbikeState> newState = parts[1].equals("-") ? Optional.empty() : Optional.of(EbikeState.valueOf(parts[1]));
                    V2d deltaPos = new V2d(Double.parseDouble(parts[2]), Double.parseDouble(parts[3]));
                    V2d deltaDir = new V2d(Double.parseDouble(parts[4]), Double.parseDouble(parts[5]));
                    double deltaSpeed = Double.parseDouble(parts[6]);
                    int deltaBatteryLevel = Integer.parseInt(parts[7]);
    
                    events.add(new EbikeEvent(ebikeId, newState, deltaPos, deltaDir, deltaSpeed, deltaBatteryLevel));
                } catch(Exception e){
                    System.out.println(String.format("Failed to parse event: '%s'", event));
                }
            }
            return events;
        }
    }

    public synchronized void addEvent(EbikeEvent event) throws IOException {
        try (var writer = new BufferedWriter(new FileWriter(this.path.toString(), true))) {
            writer.append(String.format("%s %s %f %f %f %f %f %d", event.ebikeId(), event.newState().map(EbikeState::toString).orElse("-"), 
                event.deltaPos().x(), event.deltaPos().y(), event.deltaDir().x(), event.deltaDir().y(), event.deltaSpeed(), event.deltaBatteryLevel()));
            writer.newLine();
        }
    }
}
