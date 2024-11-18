package nebula.stream.nessystestrunner;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

public class PluginSettingsComponent {
    private final JPanel panel;
    private final JTextField pathField;

    public PluginSettingsComponent() {

        /// initialize components
        panel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        pathField = new JTextField();
        JButton browseButton = new JButton("Browse");
        JLabel description = new JLabel("./systest folder path:");

        panel.add(description, BorderLayout.WEST);
        panel.add(pathField, BorderLayout.CENTER);
        panel.add(browseButton, BorderLayout.EAST);

        browseButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                JFileChooser fileChooser = new JFileChooser();
                fileChooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
                int result = fileChooser.showOpenDialog(panel);
                if (result == JFileChooser.APPROVE_OPTION) {
                    File selectedFile = fileChooser.getSelectedFile();
                    pathField.setText(selectedFile.getAbsolutePath());
                }
            }
        });
    }

    public JPanel getPanel() {
        return panel;
    }

    public String getPathText() {
        return pathField.getText();
    }

    public void setPathText(String text) {
        pathField.setText(text);
    }
}
