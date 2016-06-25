/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package frames;

import javax.swing.JOptionPane;

import core.utils;

/**
 *
 * @author furqan
 */
public class srcConnection extends javax.swing.JInternalFrame {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
     * Creates new form srcConnection
     */
    public srcConnection() {
         super("Source Connection", true,true, true, true);
        initComponents();
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        srcDB = new javax.swing.JComboBox<>();
        srcDBlable = new javax.swing.JLabel();
        srcDBUrl = new javax.swing.JTextField();
        srcDBUrlLable = new javax.swing.JLabel();
        srcDBPort = new javax.swing.JTextField();
        srcDBPortLable = new javax.swing.JLabel();
        srcDBUserName = new javax.swing.JTextField();
        srcDBUserNameLable = new javax.swing.JLabel();
        srcDBPassword = new javax.swing.JTextField();
        srcDBPasswordLable = new javax.swing.JLabel();
        srcDBSaveButton = new javax.swing.JButton();
        srcDBTestButton = new javax.swing.JButton();

        srcDB.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "MySQl Server", "SQL Server", "Postgre Server", "Oracle" }));

        srcDBlable.setText("Select DB");

        srcDBUrl.setBackground(new java.awt.Color(254, 254, 254));

        srcDBUrlLable.setText("DB URL");

        srcDBPort.setBackground(new java.awt.Color(254, 254, 254));

        srcDBPortLable.setText("Port");

        srcDBUserName.setBackground(new java.awt.Color(254, 254, 254));

        srcDBUserNameLable.setText("User Name");

        srcDBPassword.setBackground(new java.awt.Color(254, 254, 254));

        srcDBPasswordLable.setText("Password");

        srcDBSaveButton.setText("Save");
        srcDBSaveButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                srcDBSaveButtonActionPerformed(evt);
            }
        });

        srcDBTestButton.setText("Test");
        srcDBTestButton.addInputMethodListener(new java.awt.event.InputMethodListener() {
            public void inputMethodTextChanged(java.awt.event.InputMethodEvent evt) {
                srcDBTestButtonInputMethodTextChanged(evt);
            }
            public void caretPositionChanged(java.awt.event.InputMethodEvent evt) {
            }
        });
        srcDBTestButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                srcDBTestButtonActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
                    .addGroup(javax.swing.GroupLayout.Alignment.LEADING, layout.createSequentialGroup()
                        .addComponent(srcDBSaveButton, javax.swing.GroupLayout.PREFERRED_SIZE, 72, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 48, Short.MAX_VALUE)
                        .addComponent(srcDBTestButton, javax.swing.GroupLayout.PREFERRED_SIZE, 69, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addComponent(srcDBPassword)
                    .addComponent(srcDBUserName)
                    .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
                            .addComponent(srcDBUrl)
                            .addComponent(srcDB, 0, 189, Short.MAX_VALUE))
                        .addComponent(srcDBPort, javax.swing.GroupLayout.PREFERRED_SIZE, 62, javax.swing.GroupLayout.PREFERRED_SIZE)))
                .addGap(27, 27, 27)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(srcDBUserNameLable, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(srcDBPortLable, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(srcDBUrlLable, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(srcDBlable, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(srcDBPasswordLable, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                .addContainerGap(97, Short.MAX_VALUE))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(srcDB, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(srcDBlable))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(srcDBUrl, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(srcDBUrlLable))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(srcDBPort, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(srcDBPortLable))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(srcDBUserName, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(srcDBUserNameLable))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(srcDBPassword, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(srcDBPasswordLable))
                .addGap(39, 39, 39)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(srcDBSaveButton)
                    .addComponent(srcDBTestButton))
                .addContainerGap(45, Short.MAX_VALUE))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void srcDBTestButtonInputMethodTextChanged(java.awt.event.InputMethodEvent evt) {//GEN-FIRST:event_srcDBTestButtonInputMethodTextChanged
        // TODO add your handling code here:
    }//GEN-LAST:event_srcDBTestButtonInputMethodTextChanged

    private void srcDBTestButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_srcDBTestButtonActionPerformed
        boolean status = utils.testConnection(0, srcDBUrl.getText(), srcDBPort.getText(), srcDBUserName.getText(), srcDBPassword.getText());
        if( status )
        JOptionPane.showMessageDialog(this, "DB Ping Sucessfully");
        else
        JOptionPane.showMessageDialog(this, "Error in Connection");
    }//GEN-LAST:event_srcDBTestButtonActionPerformed

    private void srcDBSaveButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_srcDBSaveButtonActionPerformed
        boolean status = utils.testConnection(0, srcDBUrl.getText(), srcDBPort.getText(), srcDBUserName.getText(), srcDBPassword.getText());
        if( status ){
            JOptionPane.showMessageDialog(this, "Data Saved");
        }
        else
            JOptionPane.showMessageDialog(this, "Error in Connection- fix Params");
    }//GEN-LAST:event_srcDBSaveButtonActionPerformed


    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JComboBox<String> srcDB;
    private javax.swing.JTextField srcDBPassword;
    private javax.swing.JLabel srcDBPasswordLable;
    private javax.swing.JTextField srcDBPort;
    private javax.swing.JLabel srcDBPortLable;
    private javax.swing.JButton srcDBSaveButton;
    private javax.swing.JButton srcDBTestButton;
    private javax.swing.JTextField srcDBUrl;
    private javax.swing.JLabel srcDBUrlLable;
    private javax.swing.JTextField srcDBUserName;
    private javax.swing.JLabel srcDBUserNameLable;
    private javax.swing.JLabel srcDBlable;
    // End of variables declaration//GEN-END:variables
}