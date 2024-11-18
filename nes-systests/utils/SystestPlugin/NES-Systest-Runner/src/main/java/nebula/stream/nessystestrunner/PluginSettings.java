package nebula.stream.nessystestrunner;

import com.intellij.openapi.components.PersistentStateComponent;
import com.intellij.openapi.components.ServiceManager;
import com.intellij.openapi.components.State;
import com.intellij.openapi.components.Storage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@State(
        name = "com.example.nessystestrunner.PluginSettings",
        storages = @Storage("PluginSettings.xml")
)
public class PluginSettings implements PersistentStateComponent<PluginSettings> {
    private String pathSetting = "";

    public static PluginSettings getInstance() {
        return ServiceManager.getService(PluginSettings.class);
    }

    @Nullable
    @Override
    public PluginSettings getState() {
        return this;
    }

    @Override
    public void loadState(@NotNull PluginSettings state) {
        this.pathSetting = state.pathSetting;
    }

    public String getPathSetting() {
        return pathSetting;
    }

    public void setPathSetting(String pathSetting) {
        this.pathSetting = pathSetting;
    }
}
