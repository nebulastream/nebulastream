package com.example.nessystestrunner;

import com.intellij.lang.Language;

public class TestLanguage extends Language {
    public static final TestLanguage INSTANCE = new TestLanguage();

    private TestLanguage() {
        super("TestLanguage");
    }
}