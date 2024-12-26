package sap.ass2.admingui.gui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GridLayout;
import java.awt.RenderingHints;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.swing.*;
import io.vertx.core.json.JsonObject;
import sap.ass2.admingui.library.*;
import sap.ass2.admingui.domain.*;

public class AdminGUI extends JFrame implements ActionListener, UserEventObserver, RideEventObserver, EbikeEventObserver {
	private VisualiserPanel centralPanel; 
    private JButton addEBikeButton; 
	
	private JList<String> usersList; 
	private JList<String> bikesList; 
	private JList<String> ridesList; 

	private DefaultListModel<String> usersModel; 
	private DefaultListModel<String> bikesModel; 
	private DefaultListModel<String> ridesModel; 

	private Map<String, User> users = new HashMap<>(); 
	private Map<String, Ebike> bikes = new HashMap<>(); 
	private Map<String, Ride> rides = new HashMap<>(); 

	private ApplicationAPI app;

    public AdminGUI(ApplicationAPI app) {
		this.app = app;

		this.usersModel = new DefaultListModel<>(); 
		this.bikesModel = new DefaultListModel<>(); 
		this.ridesModel = new DefaultListModel<>(); 

		this.setupModel(); 
		this.setupView(); 
    }

	private static User jsonObjToUser(JsonObject obj){
        return new User(obj.getString("userId"), obj.getInteger("credit"), obj.getDouble("x"), obj.getDouble("y"));
    }

	private static Ride jsonObjToRide(JsonObject obj){
        return new Ride(obj.getString("rideId"), obj.getString("userId"), obj.getString("bikeId"));
    }

	private static Ebike jsonObjToEbike(JsonObject obj){
        return new Ebike(obj.getString("ebikeId"), EbikeState.valueOf(obj.getString("state")), obj.getDouble("x"), obj.getDouble("y"), obj.getDouble("dirX"), obj.getDouble("dirY"), obj.getDouble("speed"), obj.getInteger("batteryLevel"));
    }
    
    protected void setupModel() {
        var ebikesFut = this.app.ebikes().subscribeToEbikeEvents(this);
		ebikesFut.onSuccess( ebikesArray -> {
			SwingUtilities.invokeLater(() -> {
				this.bikes.putAll(ebikesArray.stream().map(e -> jsonObjToEbike((JsonObject)e)).collect(Collectors.toMap(Ebike::id, ebike -> ebike)));
				this.bikesModel.addAll(bikes.values().stream().map(Ebike::toString).toList());
				this.refreshView();
			});
		}); 

        var ridesFut = this.app.rides().subscribeToRideEvents(this);
		ridesFut.onSuccess( ridesArray -> {
			SwingUtilities.invokeLater(() -> {
				this.rides.putAll(ridesArray.stream().map(r -> jsonObjToRide((JsonObject)r)).collect(Collectors.toMap(Ride::rideId, ride -> ride)));
				this.ridesModel.addAll(rides.values().stream().map(Ride::toString).toList());
			});
		}); 

		var usersFut = this.app.users().subscribeToUsersEvents(this);
		usersFut.onSuccess( usersArray -> {
			SwingUtilities.invokeLater(() -> {
				this.users.putAll(usersArray.stream().map(u -> jsonObjToUser((JsonObject)u)).collect(Collectors.toMap(User::id, user -> user))); 
				this.usersModel.addAll(users.values().stream().map(User::toString).toList());
			});
		}); 
    }

    protected void setupView() {
        setTitle("ADMIN GUI");        
        setSize(1000,600); 
        setResizable(false); 
        
        setLayout(new BorderLayout()); 

		addEBikeButton = new JButton("Add EBike"); 
		addEBikeButton.addActionListener(this); 
		
		JPanel topPanel = new JPanel(); 
		topPanel.add(addEBikeButton);	
	    add(topPanel, BorderLayout.NORTH); 

        centralPanel = new VisualiserPanel(800, 500, this); 
	    add(centralPanel, BorderLayout.CENTER); 
	    	    		
		addWindowListener(new WindowAdapter() { 
			public void windowClosing(WindowEvent ev) {
				System.exit(-1); 
			}
		});

		JPanel eastPanel = new JPanel(); 
		eastPanel.setLayout(new GridLayout(3, 1)); 
		eastPanel.setPreferredSize(new Dimension(300, 500)); 
		
		this.usersModel = getUsersModel(); 
		this.bikesModel = getBikesModel(); 
		this.ridesModel = getRidesModel(); 
		this.usersList = new JList<>(usersModel); 
		this.bikesList = new JList<>(bikesModel); 
		this.ridesList = new JList<>(ridesModel); 
		
		
		eastPanel.add(new JScrollPane(usersList));
		eastPanel.add(new JScrollPane(bikesList));
		eastPanel.add(new JScrollPane(ridesList));
		add(eastPanel, BorderLayout.EAST); 
    }

	private void addOrReplaceRide(Ride info) {
		var old = rides.put(info.rideId(), info);
		if (old == null) {
			ridesModel.addElement(info.toString()); 
		} else {
			ridesModel.clear(); 
			ridesModel.addAll(rides.values().stream().map(Ride::toString).toList());	
		}
	}

	private void addOrReplaceUser(User info) {
		var old = users.put(info.id(), info);
		if (old == null) {
			usersModel.addElement(info.toString());
		} else {
			usersModel.clear(); 
			usersModel.addAll(users.values().stream().map(User::toString).toList());
		}
	}

	private void addOrReplaceEBike(Ebike info) {
		var old = bikes.put(info.id(), info);
		if (old == null) {
			bikesModel.addElement(info.toString()); 
		} else {
			bikesModel.clear(); 
			bikesModel.addAll(bikes.values().stream().map(Ebike::toString).toList());	
		}
	}

	private void removeRide(String rideId) {
		rides.remove(rideId);
		ridesModel.clear();
		ridesModel.addAll(rides.values().stream().map(Ride::toString).toList());
	}

	private void removeEBike(String bikeId) {
		bikes.remove(bikeId);
		bikesModel.clear();
		bikesModel.addAll(bikes.values().stream().map(Ebike::toString).toList());
	}

	private DefaultListModel<String> getRidesModel() {
		DefaultListModel<String> ridesModel = new DefaultListModel<>();
		ridesModel.addAll(rides.values().stream().map(Ride::toString).toList());
		return ridesModel;
	}

	private DefaultListModel<String> getBikesModel() {
		DefaultListModel<String> bikesModel = new DefaultListModel<>();
		bikesModel.addAll(bikes.values().stream().map(Ebike::toString).toList());
		return bikesModel;
	}

	private DefaultListModel<String> getUsersModel() {
		DefaultListModel<String> usersModel = new DefaultListModel<>();
		usersModel.addAll(users.values().stream().map(User::toString).toList());
		return usersModel;
	}

    public void display() {
    	SwingUtilities.invokeLater(() -> {
    		this.setVisible(true);
			this.revalidate();
			this.repaint();
    	});
    }
    
    public void refreshView() {
    	centralPanel.refresh();
    }  

    @Override
	public void actionPerformed(ActionEvent e) {
        if (e.getSource() == this.addEBikeButton) {
	        JDialog d = new AddEBikeDialog(this, app); 
	        d.setVisible(true);
        }
	}
    
    public static class VisualiserPanel extends JPanel {
        private long dx; 
        private long dy; 
        private AdminGUI app; 
        
        public VisualiserPanel(int w, int h, AdminGUI app) {
            setSize(w, h); 
            dx = w / 2; 
            dy = h / 2; 
            this.app = app; 
        }

        public void paint(Graphics g) {
			Graphics2D g2 = (Graphics2D) g;
    		
    		g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
    		g2.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
    		g2.clearRect(0, 0, this.getWidth(), this.getHeight()); 

			app.bikes.values().forEach(b -> {
    			int x0 = (int) (dx + b.locX()); 
		        int y0 = (int) (dy - b.locY()); 
		        g2.drawOval(x0, y0, 10, 10); 
		        g2.drawString(b.id(), x0, y0 + 35);
			});

			app.users.values().forEach(u -> {
				int x = (int) (dx + u.x()); 
		        int y = (int) (dy - u.y()); 
		        g2.drawRect(x, y, 10, 10); 
		        g2.drawString(u.id(), x, y + 35);
			});
        }
        
        public void refresh() {
            repaint();
        }
    }

	@Override
	public void bikeUpdated(String bikeID, Optional<EbikeState> state, double locationX, double locationY, double directionX, double directionY, double speed, int batteryLevel) {
		var eBikeInfo = Optional.ofNullable(bikes.get(bikeID));

		if (eBikeInfo.isEmpty()) {
			eBikeInfo = Optional.of(new Ebike(bikeID, state.get(), locationX, locationY, directionX, directionY, speed, batteryLevel));
		} else {
			eBikeInfo = Optional.of(eBikeInfo.get().addEvent(state, locationX, locationY, directionX, directionY, speed, batteryLevel));
		}

		final var eBikeInfo2 = eBikeInfo;

		SwingUtilities.invokeLater(() -> {
			addOrReplaceEBike(eBikeInfo2.get()); 
			centralPanel.refresh(); 
		});
		
		if (eBikeInfo.get().state() != EbikeState.IN_USE) {
			var ride = rides.values().stream().filter(r -> r.ebikeId().equals(bikeID)).findFirst();
			if (ride.isPresent()) {
				SwingUtilities.invokeLater(() -> {
					removeRide(ride.get().rideId()); 
					centralPanel.refresh(); 
				});
			}
		}
	}

	@Override
	public void bikeRemoved(String bikeID) {
		SwingUtilities.invokeLater(() -> this.removeEBike(bikeID));
	}

	@Override
	public void rideStarted(String rideID, String userID, String bikeID) {
		SwingUtilities.invokeLater(() -> this.addOrReplaceRide(new Ride(rideID, userID, bikeID)));
	}

	@Override
	public void rideStep(String rideID, double x, double y, double directionX, double directionY, double speed, int batteryLevel){
		var ride = rides.get(rideID); 
		var bike = bikes.get(ride.ebikeId());
		var newBike = new Ebike(bike.id(), bike.state(), x, y, directionX, directionY, speed, batteryLevel);

		SwingUtilities.invokeLater(() -> {
			addOrReplaceEBike(newBike); 
			centralPanel.refresh();
		});
	}

	@Override
	public void rideEnded(String rideID, String reason) {
		SwingUtilities.invokeLater(() -> this.removeRide(rideID));
	}

	@Override
	public void userUpdated(String userID, int creditChange, double deltaX, double deltaY) {
		var user = Optional.ofNullable(users.get(userID));

		SwingUtilities.invokeLater(() -> {
			if(user.isEmpty()){
				this.addOrReplaceUser(new User(userID, creditChange, deltaX, deltaY));
			} else {
				this.addOrReplaceUser(user.get().update(creditChange, deltaX, deltaY));
			}
			centralPanel.refresh();
		});
	}
}
