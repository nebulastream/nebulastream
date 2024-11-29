package nebula.stream.nessystestrunner;

import com.intellij.openapi.options.Configurable;
import org.jetbrains.annotations.Nls;
import org.jetbrains.annotations.Nullable;

import javax.swing.*;

public class PluginSettingsConfigurable implements Configurable {
    private PluginSettingsComponent component;

    @Override
    public @Nls(capitalization = Nls.Capitalization.Title) String getDisplayName() {
        return "Plugin Settings";
    }

    @Nullable
    @Override
    public JComponent createComponent() {
        component = new PluginSettingsComponent();
        return component.getPanel();
    }

    @Override
    public boolean isModified() {
        PluginSettings settings = PluginSettings.getInstance();
        boolean modified = !component.getPathText().equals(settings.getPathSetting()) ||
                !component.getDockerCommandText().equals(settings.getDockerCommand()) ||
                !component.getDockerTestFilePath().equals(settings.getDockerTestFilePath()) ||
                component.getDockerCommandCheckBox() != settings.getDockerCommandCheckBox();
        return modified;
    }

    @Override
    public void apply() {
        PluginSettings settings = PluginSettings.getInstance();
        settings.setPathSetting(component.getPathText());
        settings.setDockerCommand(component.getDockerCommandText());
        settings.setDockerTestFilePath(component.getDockerTestFilePath());
        settings.setDockerCommandCheckBox(component.getDockerCommandCheckBox());
    }

    @Override
    public void reset() {
        PluginSettings settings = PluginSettings.getInstance();
        component.setPathText(settings.getPathSetting());
        component.setDockerCommandField(settings.getDockerCommand());
        component.setDockerTestFilePath(settings.getDockerTestFilePath());
        component.setDockerCommandCheckBox(settings.getDockerCommandCheckBox());
    }

    @Override
    public void disposeUIResources() {
        component = null;
    }
}
