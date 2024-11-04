package com.example.nessystestrunner;

import com.intellij.openapi.fileTypes.LanguageFileType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.swing.*;

public class TestFileType extends LanguageFileType {
    public static final TestFileType INSTANCE = new TestFileType();

    private TestFileType() {
        super(TestLanguage.INSTANCE);
    }

    @NotNull
    @Override
    public String getName() {
        return "Test File";
    }

    @NotNull
    @Override
    public String getDescription() {
        return "Test file for custom plugin";
    }

    @NotNull
    @Override
    public String getDefaultExtension() {
        return "test";
    }

    @Nullable
    @Override
    public Icon getIcon() {
        return null; // Set an icon if desired
    }
}