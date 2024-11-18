package nebula.stream.nessystestrunner;

import com.intellij.lang.Language;

public class SysTestLanguage extends Language {
    public static final SysTestLanguage INSTANCE = new SysTestLanguage();

    private SysTestLanguage() {
        super("SysTestLanguage");
    }
}
