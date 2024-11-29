package nebula.stream.nessystestrunner;

import com.intellij.execution.ExecutionException;
import com.intellij.execution.configurations.RunProfile;
import com.intellij.execution.runners.ExecutionEnvironment;
import com.intellij.execution.runners.ProgramRunner;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;

public class SysTestDebugRunner implements ProgramRunner {

    @Override
    public @NotNull @NonNls String getRunnerId() {
        return "";
    }

    @Override
    public boolean canRun(@NotNull String executorId, @NotNull RunProfile profile) {
        return false;
    }

    @Override
    public void execute(@NotNull ExecutionEnvironment environment) throws ExecutionException {

    }
}
