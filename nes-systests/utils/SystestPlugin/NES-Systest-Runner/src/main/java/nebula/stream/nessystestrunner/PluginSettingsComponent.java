package nebula.stream.nessystestrunner;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

public class PluginSettingsComponent {
    private final JPanel panel;
    private final JTextField pathField;
    private final JTextField dockerCommandField;
    private final JCheckBox dockerCommandCheckBox;
    private final JTextField dockerTestFilePath;

    public PluginSettingsComponent() {
        /// Main Panel
        panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

        /// Systest Path
        JLabel pathLabel = new JLabel("Systest binary path:");
        pathLabel.setToolTipText("The folder that contains the 'systest' binary, e.g.: \n " +
                "'path\\to\\nebulastream-public\\build\\nes-systests\\systest'");
        pathField = new JTextField();
        JButton browseButton = new JButton("Browse");
        JPanel pathPanel = createSettingPanel(pathLabel, pathField);
        pathPanel.add(browseButton);

        /// Docker Command
        JLabel dockerCommandLabel = new JLabel("Docker Command:");
        dockerCommandLabel.setToolTipText("The command to run the system tests in docker, e.g.: \n " +
                "'docker run -it -v /home/user/workspace/nebulastream-public/:/tmp local:latest" +
                " /tmp/nebulastream-public/cmake-build-docker/nes-systests/systest/systest'");
        dockerCommandField = new JTextField();
        JPanel dockerCommandPanel = createSettingPanel(dockerCommandLabel, dockerCommandField);
        dockerCommandPanel.add(dockerCommandLabel);
        dockerCommandPanel.add(dockerCommandField);

        /// Use Docker CheckBox
        JLabel dockerCommandCheckBoxLabel = new JLabel("Use Docker?:");
        dockerCommandCheckBoxLabel.setToolTipText("Check this box to run the system tests in the docker instead");
        dockerCommandCheckBox = new JCheckBox();
        JPanel dockerCommandCheckBoxPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 5));
        dockerCommandCheckBoxLabel.setPreferredSize(new Dimension(120, 25));
        dockerCommandCheckBoxPanel.setMaximumSize(new Dimension(800, 35));
        dockerCommandCheckBoxPanel.setPreferredSize(new Dimension(600, 35));
        dockerCommandCheckBoxPanel.add(dockerCommandCheckBoxLabel);
        dockerCommandCheckBoxPanel.add(dockerCommandCheckBox);

        /// Docker Test File Path
        JLabel dockerTestFilePathLabel = new JLabel("Docker Test File Path:");
        dockerTestFilePathLabel.setToolTipText("The docker's temp folder path that leads to the project root counterpart, e.g.: \n " +
                "'tmp' in the case for:" +
                "'docker run -it -v /home/user/workspace/nebulastream-public/:/tmp local:latest" +
                " /tmp/nebulastream-public/cmake-build-docker/nes-systests/systest/systest" +
                " -t /tmp/nebulastream-public/nes-systests/operator/filter/Filter.test:01'");
        dockerTestFilePath = new JTextField();

        /// Main Panel
        /// NOTE: these settings have become obsolete and have been hidden. However, we may need to implement settings in the future.
        //panel.add(pathPanel);
        //panel.add(dockerCommandCheckBoxPanel);
        //panel.add(dockerCommandPanel);

        browseButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                JFileChooser fileChooser = new JFileChooser();
                fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
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

    public String getDockerCommandText() {
        return dockerCommandField.getText();
    }

    public void setDockerCommandField(String text) {
        dockerCommandField.setText(text);
    }

    public boolean getDockerCommandCheckBox() {
        return dockerCommandCheckBox.isSelected();
    }

    public void setDockerCommandCheckBox(boolean selected) {
        dockerCommandCheckBox.setSelected(selected);
    }

    public String getDockerTestFilePath() {
        return dockerTestFilePath.getText();
    }

    public void setDockerTestFilePath(String text) {
        dockerTestFilePath.setText(text);
    }

    private JPanel createSettingPanel(JLabel label, JTextField textField) {

        label.setPreferredSize(new Dimension(120, 25));
        textField.setPreferredSize(new Dimension(300, 25));

        JPanel settingPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 5));
        settingPanel.add(label);
        settingPanel.add(textField);

        settingPanel.setMaximumSize(new Dimension(800, 35));
        settingPanel.setPreferredSize(new Dimension(600, 35));

        return settingPanel;
    }
}
