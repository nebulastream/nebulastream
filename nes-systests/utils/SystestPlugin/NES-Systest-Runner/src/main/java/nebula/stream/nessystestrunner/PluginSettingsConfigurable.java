/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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
