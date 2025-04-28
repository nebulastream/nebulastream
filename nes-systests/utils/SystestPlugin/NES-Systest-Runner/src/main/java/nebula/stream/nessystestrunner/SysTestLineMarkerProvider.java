package nebula.stream.nessystestrunner;

import com.intellij.codeInsight.daemon.LineMarkerInfo;
import com.intellij.codeInsight.daemon.LineMarkerProvider;
import com.intellij.execution.*;
import com.intellij.execution.configurations.ConfigurationFactory;
import com.intellij.execution.executors.DefaultDebugExecutor;
import com.intellij.execution.executors.DefaultRunExecutor;
import com.intellij.execution.ui.*;
import com.intellij.openapi.application.ApplicationManager;
import com.intellij.openapi.editor.Document;
import com.intellij.openapi.editor.markup.GutterIconRenderer;
import com.intellij.openapi.fileEditor.FileDocumentManager;
import com.intellij.openapi.util.TextRange;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.openapi.wm.*;
import com.intellij.psi.PsiDocumentManager;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiFile;
import com.intellij.icons.AllIcons;
import com.intellij.ui.content.ContentFactory;
import com.intellij.util.messages.MessageBusConnection;
import com.jetbrains.cidr.cpp.cmake.model.CMakeConfiguration;
import com.jetbrains.cidr.cpp.cmake.model.CMakeTarget;
import com.jetbrains.cidr.cpp.cmake.workspace.CMakeProfileInfo;
import com.jetbrains.cidr.cpp.cmake.workspace.CMakeWorkspace;
import com.jetbrains.cidr.cpp.cmake.workspace.CMakeWorkspaceListener;
import com.jetbrains.cidr.cpp.execution.*;
import com.jetbrains.cidr.cpp.toolchains.CPPEnvironment;
import com.jetbrains.cidr.cpp.toolchains.CPPToolSet;
import com.jetbrains.cidr.cpp.toolchains.CPPToolchains;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import com.intellij.openapi.project.Project;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.intellij.execution.impl.ConsoleViewImpl;
import com.intellij.openapi.wm.ToolWindow;
import com.intellij.openapi.wm.ToolWindowManager;
import com.intellij.execution.ui.ConsoleView;
import com.intellij.execution.RunManager;
import com.intellij.execution.RunnerAndConfigurationSettings;
import com.intellij.execution.ExecutionTarget;
import com.jetbrains.cidr.cpp.execution.CMakeAppRunConfiguration;

public class SysTestLineMarkerProvider implements LineMarkerProvider  {

    @Nullable
    @Override
    public LineMarkerInfo<?> getLineMarkerInfo(@NotNull PsiElement element) {

        return null;
    }

    /// The Plugin affects all files of type '.test' and '.test.disabled'
    /// This searches the currently open file for queries and adds an interactable Gutter Icon
    /// that can run/debug the corresponding system test
    @Override
    public void collectSlowLineMarkers(@NotNull List<? extends PsiElement> elements,
                                       @NotNull Collection<? super LineMarkerInfo<?>> result) {

        if (elements.isEmpty()) return;

        PsiFile file = elements.get(0).getContainingFile();

        if (file == null || !file.getName().endsWith(".test") && !file.getName().endsWith(".test_disabled") && !file.getName().endsWith(".test.disabled")) {
            return;
        }

        /// Pattern match all occurrences of a test using "----", which indicates the end of the system test query
        String fileText = file.getText();
        Pattern pattern = Pattern.compile("----");
        Matcher matcher = pattern.matcher(fileText);

        int systestIndex = 1;

        /// General Gutter Icon that runs all test in current file
        PsiElement firstElement = file.getFirstChild();
        if (firstElement != null) {
            /// Get the range of the first line
            int firstLineEndOffset = fileText.indexOf('\n');
            if (firstLineEndOffset == -1) {
                firstLineEndOffset = fileText.length();
            }

            TextRange firstLineRange = new TextRange(0, firstLineEndOffset);

            LineMarkerInfo<PsiElement> firstLineMarkerInfo = new LineMarkerInfo<>(
                    firstElement,
                    firstLineRange,
                    AllIcons.Actions.RunAll,
                    psiElement -> "Run All System Tests in " + file.getVirtualFile().getName(),
                    (e, elt) -> { startRunSysTest(elt.getProject(), false, file, 0); },
                    GutterIconRenderer.Alignment.CENTER
            );
            result.add(firstLineMarkerInfo);
        }

        while (matcher.find()) {

            int startOffset = matcher.start();

            /// Calculate line start and end offsets manually
            int lineStartOffset = fileText.lastIndexOf('\n', startOffset - 1) + 1;
            int lineEndOffset = fileText.indexOf('\n', startOffset);
            if (lineEndOffset == -1) {
                lineEndOffset = fileText.length();
            }

            /// Find the element that corresponds to the start of the line
            PsiElement lineElement = file.findElementAt(lineStartOffset);
            if (lineElement == null) continue;

            TextRange lineTextRange = new TextRange(lineStartOffset, lineEndOffset);
            int currentSystestIndex = systestIndex;

            /// Create the LineMarkerInfo for the gutter icon
            LineMarkerInfo<PsiElement> lineMarkerInfo = new LineMarkerInfo<>(
                    lineElement,
                    lineTextRange,
                    AllIcons.Actions.Execute,
                    psiElement -> "Run System Test " + currentSystestIndex,
                    (e, elt) -> { startRunSysTest(elt.getProject(), false, file, currentSystestIndex); },
                    GutterIconRenderer.Alignment.CENTER
            );
            result.add(lineMarkerInfo);

            /// Create the LineMarkerInfo for the gutter icon, debugging variant
            LineMarkerInfo<PsiElement> lineMarkerInfoDebug = new LineMarkerInfo<>(
                    lineElement,
                    lineTextRange,
                    AllIcons.Actions.StartDebugger,
                    psiElement -> "Debug System Test " + currentSystestIndex,
                    (e, elt) -> { startRunSysTest(elt.getProject(), true, file, currentSystestIndex); },
                    GutterIconRenderer.Alignment.LEFT
            );
            result.add(lineMarkerInfoDebug);

            systestIndex++;
        }
    }

    /// This function is called when the systest Gutter Icon is clicked.
    /// It finds the "systest" configuration, makes a "systest_plugin" copy and modifies the program arguments
    /// Then run/debug the plugin configuration
    public static void startRunSysTest(Project project, boolean runDebugger, PsiFile file, int testIndex) {

        /// Save File to ensure recent changes apply to System Test run
        saveFile(file);

        /// Create Nes-Systest-Runner window or reuse existing one to show console output / potential errors
        ConsoleView consoleView = new ConsoleViewImpl(project, false);
        ToolWindow toolWindow = ToolWindowManager.getInstance(project).getToolWindow("NES-Systest-Runner");
        if (toolWindow == null) {
            toolWindow = ToolWindowManager.getInstance(project).registerToolWindow(
                    "NES-Systest-Runner",
                    true,
                    ToolWindowAnchor.BOTTOM);
        }
        var contentManager = toolWindow.getContentManager();
        contentManager.removeAllContents(true);

        /// Add console to the tool window content
        var contentFactory = ContentFactory.getInstance();
        var content = contentFactory.createContent(consoleView.getComponent(), "Command Output", false);
        toolWindow.getContentManager().addContent(content);

        try {
            /// Find the "systest" Run/Debug configuration
            RunManager runManager = RunManager.getInstance(project);
            RunnerAndConfigurationSettings runnerAndConfigurationSettings = runManager
                    .getAllSettings()
                    .stream()
                    .filter(confsettings -> "systest".equals(confsettings.getName()))
                    .findFirst()
                    .orElse(null);

            if(runnerAndConfigurationSettings == null){
                toolWindow.activate(null);
                consoleView.print("Could not find the 'systest' Run/Debug configuration. Reloading CMake... \n", ConsoleViewContentType.ERROR_OUTPUT);
                toolWindow.show(null);

                CMakeWorkspace cMakeWorkspace = CMakeWorkspace.getInstance(project);

                MessageBusConnection connection = project.getMessageBus().connect();

                connection.subscribe(CMakeWorkspaceListener.TOPIC, new CMakeWorkspaceListener() {
                    @Override
                    public void reloadingFinished(boolean canceled) {
                        if (canceled) {
                            consoleView.print("CMake Reload canceled. \n", ConsoleViewContentType.ERROR_OUTPUT);
                        }
                        RunnerAndConfigurationSettings runnerAndConfigurationSettings = runManager
                                .getAllSettings()
                                .stream()
                                .filter(confsettings -> "systest".equals(confsettings.getName()))
                                .findFirst()
                                .orElse(null);
                        if(runnerAndConfigurationSettings == null){
                            consoleView.print("Could not find 'systest' configuration after CMake reload. \n " +
                                    "Please ensure that CMake can create the Run/Debug Configuration from the 'systest' target " +
                                    "or create the configuration manually if it is missing.", ConsoleViewContentType.ERROR_OUTPUT);
                            return;
                        }

                        runSysTest(project, runDebugger, runnerAndConfigurationSettings, file, testIndex);
                        connection.disconnect();
                    }
                });

                /// trigger cmake reload
                CMakeWorkspace.getInstance(project).scheduleReload(true);
            }
            else{
                runSysTest(project, runDebugger, runnerAndConfigurationSettings, file, testIndex);
            }
        }
        catch(Exception e) {
            toolWindow.activate(null);
            consoleView.print("Error: " + e.getMessage() + "\n", ConsoleViewContentType.ERROR_OUTPUT);
            for (StackTraceElement element : e.getStackTrace()) {
                consoleView.print(element.toString() + "\n", ConsoleViewContentType.ERROR_OUTPUT);
            }
            toolWindow.show(null);
        }
    }
    public static void runSysTest(
            Project project,
            boolean runDebugger,
            RunnerAndConfigurationSettings runnerAndConfigurationSettings,
            PsiFile file,
            int testIndex
            ) {
        RunManager runManager = RunManager.getInstance(project);
        CMakeAppRunConfiguration cMakeAppRunConfigurationExisting = (CMakeAppRunConfiguration) runnerAndConfigurationSettings.getConfiguration();

        /// Check if 'systest_plugin' configuration already exists and reuse
        RunnerAndConfigurationSettings pluginConfigSettings = runManager
                .getAllSettings()
                .stream()
                .filter(confsettings -> "systest_plugin".equals(confsettings.getName()))
                .findFirst()
                .orElse(null);

        if(pluginConfigSettings == null){
            /// Create systest_plugin configuration
            CMakeAppRunConfigurationType configurationType = CMakeAppRunConfigurationType.getInstance();
            pluginConfigSettings = runManager.createConfiguration(
                    "systest_plugin", configurationType.getFactory()
            );
            runManager.addConfiguration(pluginConfigSettings);
        }

        /// Get the currently selected CMake profile and environment
        CMakeWorkspace cMakeWorkspace = CMakeWorkspace.getInstance(project);
        ExecutionTarget executionTarget = ExecutionTargetManager.getInstance(project).findTarget(cMakeAppRunConfigurationExisting);
        CMakeConfiguration cMakeConfiguration = cMakeAppRunConfigurationExisting.getBuildAndRunConfigurations(executionTarget).getRunConfiguration();
        CMakeProfileInfo activeProfile = null;
        try {
            activeProfile = cMakeWorkspace.getProfileInfoFor(cMakeConfiguration);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        CPPEnvironment cppEnvironment = activeProfile.getEnvironment();

        /// If index is specified, only run that single test
        String testPath = file.getVirtualFile().getPath();
        String testIndexSuffix = "";
        if(testIndex > 0){
            testIndexSuffix = ":" + String.format("%02d", testIndex);
        }

        /// Get the correct test path from the environment
        String executablePath = cppEnvironment.toEnvPath(testPath);
        String Parameters = "-t " + executablePath + testIndexSuffix;

        /// Filter out old test path in parameters, if it exists; plugin path takes priority
        String existingParameters = cMakeAppRunConfigurationExisting.getProgramParameters();
        String cleanedOldParameters = "";
        if(existingParameters != null){
            String tPattern = "-t\\s+\\S+";
            cleanedOldParameters = existingParameters.replaceAll(tPattern, "").trim();
            tPattern = "--testLocation\\s+\\S+";
            cleanedOldParameters = cleanedOldParameters.replaceAll(tPattern, "").trim();
        }

        /// Change program parameters of plugin configuration
        CMakeAppRunConfiguration cMakeAppRunConfigurationPlugin = (CMakeAppRunConfiguration)  pluginConfigSettings.getConfiguration();
        cMakeAppRunConfigurationPlugin.setTargetAndConfigurationData(cMakeAppRunConfigurationExisting.getTargetAndConfigurationData());
        cMakeAppRunConfigurationPlugin.setExecutableData(cMakeAppRunConfigurationExisting.getExecutableData());
        cMakeAppRunConfigurationPlugin.setProgramParameters(Parameters + " " + cleanedOldParameters);
        pluginConfigSettings.setTemporary(false);
        cMakeAppRunConfigurationPlugin.setExplicitBuildTargetName(cMakeAppRunConfigurationExisting.getExplicitBuildTargetName());
        runManager.setSelectedConfiguration(pluginConfigSettings);

        /// Run/Debug the plugin configuration
        if(runDebugger){
            ProgramRunnerUtil.executeConfiguration(
                    pluginConfigSettings,
                    DefaultDebugExecutor.getDebugExecutorInstance()
            );
        }
        else{
            ProgramRunnerUtil.executeConfiguration(
                    pluginConfigSettings,
                    DefaultRunExecutor.getRunExecutorInstance()
            );
        }
    }

    public static void saveFile(PsiFile psiFile) {
        VirtualFile virtualFile = psiFile.getVirtualFile();
        Document document = FileDocumentManager.getInstance().getDocument(virtualFile);
        if (document != null) {
            PsiDocumentManager psiDocumentManager = PsiDocumentManager.getInstance(psiFile.getProject());
            psiDocumentManager.commitDocument(document);
            FileDocumentManager.getInstance().saveDocument(document);
        }
    }
}
