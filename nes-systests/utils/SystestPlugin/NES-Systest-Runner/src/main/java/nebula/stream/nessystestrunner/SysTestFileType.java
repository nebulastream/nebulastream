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

import com.intellij.openapi.fileTypes.FileType;
import com.intellij.openapi.fileTypes.LanguageFileType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.intellij.icons.AllIcons;
import javax.swing.*;

/// This class defines systest files as their own language to check whether SysTestLineMarkerProvider should run
/// affected extensions are: .test, .test_disabled, .test.disabled
public class SysTestFileType extends LanguageFileType {

    public static final @NotNull FileType INSTANCE = new SysTestFileType();

    SysTestFileType() {
        super(SysTestLanguage.INSTANCE);
    }

    @NotNull
    @Override
    public String getName() {
        return "System Test File";
    }

    @NotNull
    @Override
    public String getDescription() {
        return "System Test File for NES-SysTest-Runner";
    }

    @NotNull
    @Override
    public String getDefaultExtension() {
        return "test,test_disabled,test.disabled";
    }

    @Nullable
    @Override
    public Icon getIcon() {
        return AllIcons.FileTypes.Text;
    }
}
