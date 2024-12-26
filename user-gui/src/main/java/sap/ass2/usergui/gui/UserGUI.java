package sap.ass2.usergui.gui;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;
import javax.swing.*;
import io.vertx.core.json.JsonObject;
import sap.ass2.usergui.domain.*;
import sap.ass2.usergui.library.ApplicationAPI;
import sap.ass2.usergui.library.RideEventObserver;
import sap.ass2.usergui.library.UserEventObserver;

public class UserGUI extends JFrame implements ActionListener, UserEventObserver, RideEventObserver {
    private JButton startRideButton;                
    private JButton endRideButton;                  
    private JLabel userCreditLabel;                 
    private JTextField creditRechargeTextField;     
    private JButton creditRechargeButton;          
    private JButton loginButton;                    
    private JButton registerUserButton;             
    private JComboBox<String> userDropdown;         
    private JPanel mainPanel;                       
    private CardLayout cardLayout;                  
    private User selectedUser;                      
    private Ride launchedRide;                      
    private ApplicationAPI app;
    private List<User> availableUsers;
    private JTextField changeXTextField;
    private JTextField changeYTextField;
    private JButton moveButton;

    public UserGUI(ApplicationAPI app) {
        this.app = app;
        setupView(); 
    }

    private static User jsonObjToUser(JsonObject obj){
        return new User(obj.getString("userId"), obj.getInteger("credit"));
    }

    protected void setupView() {
        setTitle("USER GUI");        
        setSize(800, 300); 
        setResizable(false); 
        setLayout(new BorderLayout()); 
        cardLayout = new CardLayout(); 
        mainPanel = new JPanel(cardLayout); 

        JPanel userSelectionPanel = new JPanel();

        userDropdown = new JComboBox<>();
        this.app.users().getAllUsers()
            .onSuccess(users -> {
                availableUsers = users.stream().map(obj -> jsonObjToUser((JsonObject)obj)).collect(Collectors.toList()); 
                userDropdown.setModel(new DefaultComboBoxModel<String>(availableUsers.stream().map(User::id).collect(Vector<String>::new, Vector::add, Vector::addAll)));
            })
            .onFailure(ex -> {
                JOptionPane.showMessageDialog(this, ex.getMessage(), "ERROR", JOptionPane.ERROR_MESSAGE);
            });
        
        loginButton = new JButton("LOGIN");             
        registerUserButton = new JButton("NEW USER");   
        loginButton.addActionListener(this);            
        registerUserButton.addActionListener(this);     
        userSelectionPanel.add(userDropdown);           
        userSelectionPanel.add(loginButton);            
        userSelectionPanel.add(registerUserButton);     

        JPanel ridePanel = new JPanel();
        startRideButton = new JButton("Start Ride");    
        startRideButton.addActionListener(this);        
        endRideButton = new JButton("End Ride");        
        endRideButton.addActionListener(this);        
        endRideButton.setEnabled(false);                
        creditRechargeButton = new JButton("RECHARGE"); 
        creditRechargeButton.addActionListener(this);  
        moveButton = new JButton("MOVE");
        moveButton.addActionListener(this); 
        creditRechargeTextField = new JTextField();     
        creditRechargeTextField.setColumns(2);          
        userCreditLabel = new JLabel("Credit: ");    
        changeXTextField =   new JTextField();
        changeXTextField.setColumns(2);       
        changeYTextField = new JTextField();
        changeYTextField.setColumns(2);    
        ridePanel.add(startRideButton);                 
        ridePanel.add(endRideButton);                  
        ridePanel.add(userCreditLabel);                 
        ridePanel.add(creditRechargeTextField);        
        ridePanel.add(creditRechargeButton);    
        ridePanel.add(changeXTextField);
        ridePanel.add(changeYTextField);
        ridePanel.add(moveButton);      

        mainPanel.add(userSelectionPanel, "UserSelection"); 
        mainPanel.add(ridePanel, "RidePanel");              

        cardLayout.show(mainPanel, "UserSelection");         

        add(mainPanel, BorderLayout.CENTER);                

        addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent ev) {
                System.exit(-1);                            
            }
        });

        pack(); 
    }

    public void display() {
        SwingUtilities.invokeLater(() -> {
            this.setVisible(true); 
        });
    }
        
    @Override
    public void actionPerformed(ActionEvent e) {
        if (e.getSource() == registerUserButton) {
            JDialog registerDialog = new JDialog(this, "CREATE NEW USER", true);
            registerDialog.setSize(300, 150);
            registerDialog.setLayout(new GridLayout(3, 1));

            JTextField newUserField = new JTextField(); 
            JButton confirmButton = new JButton("REGISTER"); 

            confirmButton.addActionListener(ev -> {
                String newUserId = newUserField.getText(); 
                if (!newUserId.isEmpty()) {
                    
                    this.app.users().createUser(newUserId)
                        .onSuccess(user -> {
                            var newUser = jsonObjToUser(user);
                            this.availableUsers.add(newUser);
                            userDropdown.addItem(newUser.id());    
                            registerDialog.dispose();
                        })
                        .onFailure(ex -> {
                            JOptionPane.showMessageDialog(this, ex.getMessage(), "ERROR", JOptionPane.ERROR_MESSAGE);
                        });
                }
            });

            registerDialog.add(new JLabel("INSERT USER-ID:"));
            registerDialog.add(newUserField);
            registerDialog.add(confirmButton);

            registerDialog.setVisible(true); 
        } else if (e.getSource() == loginButton) {
            this.selectedUser = this.availableUsers.get(userDropdown.getSelectedIndex());
            userCreditLabel.setText("Credit: " + this.selectedUser.credit());

            this.app.users().subscribeToUserEvents(this.selectedUser.id(), this);

            cardLayout.show(mainPanel, "RidePanel"); 
            this.pack(); 
        } else if (e.getSource() == creditRechargeButton) {
            this.app.users().rechargeCredit(selectedUser.id(), Integer.parseInt(creditRechargeTextField.getText()))
                .onFailure(ex -> {
                    JOptionPane.showMessageDialog(this, ex.getMessage(), "ERROR", JOptionPane.ERROR_MESSAGE);
                });
        } else if (e.getSource() == startRideButton) {
            JDialog d;
            d = new RideDialog(this, this.selectedUser.id(), this.app);
            d.setVisible(true);
        } else if (e.getSource() == endRideButton) {
            this.app.rides().stopRide(launchedRide.rideId(), launchedRide.userId())
                .onFailure(ex -> {
                    JOptionPane.showMessageDialog(this, ex.getMessage(), "ERROR", JOptionPane.ERROR_MESSAGE);
                });
        }
        else if (e.getSource() == moveButton) {
            this.app.users().move(selectedUser.id(), Double.parseDouble(changeXTextField.getText()), Double.parseDouble(changeYTextField.getText()))
                .onFailure(ex -> {
                    JOptionPane.showMessageDialog(this, ex.getMessage(), "ERROR", JOptionPane.ERROR_MESSAGE);
                });
        }
    }

    public void setLaunchedRide(Ride newRide) {
        this.launchedRide = newRide;
        this.startRideButton.setEnabled(false); 
        this.endRideButton.setEnabled(true); 

        this.app.rides().subscribeToRideEvents(this.launchedRide.rideId(), this);
        System.out.println("Ride started.");
    }

    @Override
    public void userUpdated(String userID, int creditChange) {
        this.selectedUser = this.selectedUser.updateCredit(creditChange);
        this.userCreditLabel.setText("Credits: " + selectedUser.credit());
        this.pack();
    }

    @Override
    public void rideStep(String rideID, double x, double y, double directionX, double directionY, double speed, int batteryLevel) {
        System.out.println("Bike movement: [x -> " + x + "], [y ->" + y + "], [battery -> " + batteryLevel + "]");
    }

    @Override
    public void rideEnded(String rideID, String reason) {
        System.out.println("Ride ended.");
        new Thread(() -> JOptionPane.showMessageDialog(this, reason, "Info", JOptionPane.INFORMATION_MESSAGE)).start();
        
        this.launchedRide = null; 
                
        this.startRideButton.setEnabled(true);
        this.endRideButton.setEnabled(false);

        this.app.rides().unsubscribeFromRideEvents();
    }
}
