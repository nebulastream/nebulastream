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
