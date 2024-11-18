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
        return !component.getPathText().equals(settings.getPathSetting());
    }

    @Override
    public void apply() {
        PluginSettings settings = PluginSettings.getInstance();
        settings.setPathSetting(component.getPathText());
    }

    @Override
    public void reset() {
        PluginSettings settings = PluginSettings.getInstance();
        component.setPathText(settings.getPathSetting());
    }

    @Override
    public void disposeUIResources() {
        component = null;
    }
}
