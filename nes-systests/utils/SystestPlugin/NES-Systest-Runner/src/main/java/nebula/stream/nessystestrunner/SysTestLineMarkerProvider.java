package nebula.stream.nessystestrunner;

import com.intellij.codeInsight.daemon.LineMarkerInfo;
import com.intellij.codeInsight.daemon.LineMarkerProvider;
import com.intellij.execution.*;
import com.intellij.execution.executors.DefaultDebugExecutor;
import com.intellij.execution.executors.DefaultRunExecutor;
import com.intellij.execution.ui.*;
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
import com.jetbrains.cidr.cpp.cmake.model.CMakeConfiguration;
import com.jetbrains.cidr.cpp.cmake.workspace.CMakeProfileInfo;
import com.jetbrains.cidr.cpp.cmake.workspace.CMakeWorkspace;
import com.jetbrains.cidr.cpp.execution.*;
import com.jetbrains.cidr.cpp.toolchains.CPPEnvironment;
import com.jetbrains.cidr.cpp.toolchains.CPPToolSet;
import com.jetbrains.cidr.cpp.toolchains.CPPToolchains;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import com.intellij.openapi.project.Project;
import java.util.Collection;
import java.util.List;
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

        /// pattern match all occurrences of a test using "----", which indicates the end of the system test query
        String fileText = file.getText();
        Pattern pattern = Pattern.compile("----");
        Matcher matcher = pattern.matcher(fileText);

        int systestIndex = 1;

        /// general Gutter Icon that runs all test in current file
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
                    (e, elt) -> { runSysTest(elt.getProject(), false, file, 0); },
                    GutterIconRenderer.Alignment.CENTER
            );
            result.add(firstLineMarkerInfo);
        }

        while (matcher.find()) {

            int startOffset = matcher.start();

            /// calculate line start and end offsets manually
            int lineStartOffset = fileText.lastIndexOf('\n', startOffset - 1) + 1;
            int lineEndOffset = fileText.indexOf('\n', startOffset);
            if (lineEndOffset == -1) {
                lineEndOffset = fileText.length();
            }

            /// find the element that corresponds to the start of the line
            PsiElement lineElement = file.findElementAt(lineStartOffset);
            if (lineElement == null) continue;

            TextRange lineTextRange = new TextRange(lineStartOffset, lineEndOffset);
            int currentSystestIndex = systestIndex;

            /// create the LineMarkerInfo for the gutter icon
            LineMarkerInfo<PsiElement> lineMarkerInfo = new LineMarkerInfo<>(
                    lineElement,
                    lineTextRange,
                    AllIcons.Actions.Execute,
                    psiElement -> "Run System Test " + currentSystestIndex,
                    (e, elt) -> { runSysTest(elt.getProject(), false, file, currentSystestIndex); },
                    GutterIconRenderer.Alignment.CENTER
            );
            result.add(lineMarkerInfo);

            /// create the LineMarkerInfo for the gutter icon, debugging variant
            LineMarkerInfo<PsiElement> lineMarkerInfoDebug = new LineMarkerInfo<>(
                    lineElement,
                    lineTextRange,
                    AllIcons.Actions.StartDebugger,
                    psiElement -> "Debug System Test " + currentSystestIndex,
                    (e, elt) -> { runSysTest(elt.getProject(), true, file, currentSystestIndex); },
                    GutterIconRenderer.Alignment.LEFT
            );
            result.add(lineMarkerInfoDebug);

            systestIndex++;
        }
    }

    /// This function is called when the systest Gutter Icon is clicked.
    /// It finds the "systest" configuration, makes a temporary copy and modifies the program arguments
    /// Then run/debug the temporary configuration
    public static void runSysTest(Project project, boolean runDebugger, PsiFile file, int testIndex) {

        /// Save File to ensure recent changes apply to System Test run
        saveFile(file);

        /// create Nes-Systest-Runner window or reuse existing one to show console output / potential errors
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

        /// add console to the tool window content
        var contentFactory = ContentFactory.getInstance();
        var content = contentFactory.createContent(consoleView.getComponent(), "Command Output", false);
        toolWindow.getContentManager().addContent(content);

        /// if index is specified, only run that single test
        String testPath = file.getVirtualFile().getPath();
        String testIndexSuffix = "";
        if(testIndex > 0){
            testIndexSuffix = ":" + String.format("%02d", testIndex);
        }

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
                consoleView.print("Could not find the 'systest' Run/Debug configuration.", ConsoleViewContentType.ERROR_OUTPUT);
                toolWindow.show(null);
                return;
            }
            CMakeAppRunConfiguration cMakeAppRunConfigurationExisting = (CMakeAppRunConfiguration) runnerAndConfigurationSettings.getConfiguration();

            /// create a temporary configuration
            CMakeAppRunConfigurationType configurationType = CMakeAppRunConfigurationType.getInstance();
            RunnerAndConfigurationSettings temporaryConfigSettings = runManager.createConfiguration(
                    "systest_temp", configurationType.getFactory()
            );

            /// Get the currently selected CMake profile
            CMakeWorkspace cMakeWorkspace = CMakeWorkspace.getInstance(project);
            ExecutionTarget executionTarget = ExecutionTargetManager.getInstance(project).findTarget(cMakeAppRunConfigurationExisting);
            CMakeConfiguration cMakeConfiguration = cMakeAppRunConfigurationExisting.getBuildAndRunConfigurations(executionTarget).getRunConfiguration();
            CMakeProfileInfo activeProfile = cMakeWorkspace.getProfileInfoFor(cMakeConfiguration);
            CPPEnvironment cppEnvironment = activeProfile.getEnvironment();

            /// Get the correct path from the target/executable for Remote Host
            String executablePath = cppEnvironment.toEnvPath(testPath);
            String Parameters = "-t " + executablePath + testIndexSuffix;

            /// change program parameters of temp configuration
            CMakeAppRunConfiguration cMakeAppRunConfigurationTemp = (CMakeAppRunConfiguration)  temporaryConfigSettings.getConfiguration();
            cMakeAppRunConfigurationTemp.setTargetAndConfigurationData(cMakeAppRunConfigurationExisting.getTargetAndConfigurationData());
            cMakeAppRunConfigurationTemp.setExecutableData(cMakeAppRunConfigurationExisting.getExecutableData());
            cMakeAppRunConfigurationTemp.setProgramParameters(Parameters);
            temporaryConfigSettings.setTemporary(true);
            cMakeAppRunConfigurationTemp.setExplicitBuildTargetName(cMakeAppRunConfigurationExisting.getExplicitBuildTargetName());

            /// Run/Debug the temporary configuration
            if(runDebugger){
                ProgramRunnerUtil.executeConfiguration(
                        temporaryConfigSettings,
                        DefaultDebugExecutor.getDebugExecutorInstance()
                );
            }
            else{
                ProgramRunnerUtil.executeConfiguration(
                        temporaryConfigSettings,
                        DefaultRunExecutor.getRunExecutorInstance()
                );
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
