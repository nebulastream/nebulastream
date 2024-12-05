package nebula.stream.nessystestrunner;
import com.intellij.codeInsight.daemon.LineMarkerInfo;
import com.intellij.codeInsight.daemon.LineMarkerProvider;
import com.intellij.execution.process.*;
import com.intellij.execution.ui.*;
import com.intellij.openapi.editor.Document;
import com.intellij.openapi.editor.markup.GutterIconRenderer;
import com.intellij.openapi.fileEditor.FileDocumentManager;
import com.intellij.openapi.util.Key;
import com.intellij.openapi.util.TextRange;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.openapi.wm.*;
import com.intellij.psi.PsiDocumentManager;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiFile;
import com.intellij.icons.AllIcons;
import com.intellij.ui.content.ContentFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import com.intellij.openapi.project.Project;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.intellij.execution.configurations.GeneralCommandLine;
import com.intellij.execution.impl.ConsoleViewImpl;
import com.intellij.openapi.wm.ToolWindow;
import com.intellij.openapi.wm.ToolWindowManager;
import com.intellij.execution.ui.ConsoleView;

public class SysTestLineMarkerProvider implements LineMarkerProvider  {

    @Nullable
    @Override
    public LineMarkerInfo<?> getLineMarkerInfo(@NotNull PsiElement element) {
        return null;
    }

    /// The Plugin affects all files of type '.test' and '.test.disabled'
    /// This searches the currently open file for queries and adds an interactable Gutter Icon
    /// that can run the corresponding system test
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
                    (e, elt) -> { runSysTest(elt.getProject(), null, file, 0); },
                    GutterIconRenderer.Alignment.CENTER
            );
            result.add(firstLineMarkerInfo);

            /// Debug variant
            LineMarkerInfo<PsiElement> firstLineMarkerInfoDebug = new LineMarkerInfo<>(
                    firstElement,
                    firstLineRange,
                    AllIcons.Actions.StartDebugger,
                    psiElement -> "Debug All System Tests in " + file.getVirtualFile().getName(),
                    (e, elt) -> { runSysTest(elt.getProject(),  "--debug", file, 0); },
                    GutterIconRenderer.Alignment.LEFT
            );
            result.add(firstLineMarkerInfoDebug);
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
                    (e, elt) -> { runSysTest(elt.getProject(), null, file, currentSystestIndex); },
                    GutterIconRenderer.Alignment.CENTER
            );
            result.add(lineMarkerInfo);

            /// create the LineMarkerInfo for the gutter icon, debugging variant
            LineMarkerInfo<PsiElement> lineMarkerInfoDebug = new LineMarkerInfo<>(
                    lineElement,
                    lineTextRange,
                    AllIcons.Actions.StartDebugger,
                    psiElement -> "Debug System Test " + currentSystestIndex,
                    (e, elt) -> { runSysTest(elt.getProject(), "--debug", file, currentSystestIndex); },
                    GutterIconRenderer.Alignment.LEFT
            );
            result.add(lineMarkerInfoDebug);

            systestIndex++;
        }
    }

    /// This function is called when the systest Gutter Icon is clicked.
    /// It opens a new console "Nes-Systest-Runner" and adds it to the toolWindow,
    /// runs the systest command and prints the output
    public static void runSysTest(Project project, String parameters, PsiFile file, int testIndex) {

        /// Save File to ensure recent changes apply to System Test run
        saveFile(file);

        /// Access plugin settings for path to systest binary
        PluginSettings settings = PluginSettings.getInstance();
        String pathSetting = settings.getPathSetting();
        String DockerCommand = settings.getDockerCommand();
        //String dockerTestFilePath = settings.getDockerTestFilePath();

        File workingDir = new File(pathSetting);
        if (!workingDir.isAbsolute()) {
            workingDir = new File(System.getProperty("user.dir"), pathSetting);
        }

        ConsoleView consoleView = new ConsoleViewImpl(project, false);

        /// create Nes-Systest-Runner window or reuse existing one to show console output
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
        toolWindow.activate(null);

        /// validate path
        if (!workingDir.exists() || !workingDir.isDirectory()) {
            IllegalArgumentException e = new IllegalArgumentException("Invalid working directory, could not find systest binary in: " + pathSetting);
            consoleView.print("Error: " + e.getMessage() + "\n", ConsoleViewContentType.ERROR_OUTPUT);
            toolWindow.show(null);
            return;
        }



        /// if index is specified, only run that single test
        String testPath = file.getVirtualFile().getPath();
        String testIndexSuffix = "";
        if(testIndex > 0){
            testIndexSuffix = ":" + String.format("%02d", testIndex);
        }

        /// Check whether to run with docker
        GeneralCommandLine commandLine;
        if (settings.getDockerCommandCheckBox()){
            /// change the filepath to begin at the user-defined docker test file path, leading to the project root counterpart
            int mainFolderIndex = testPath.toString().indexOf("nebulastream-public");

            if (mainFolderIndex == -1) {
                IllegalArgumentException e = new IllegalArgumentException(
                        "could not find '" + project.getName() + "' in Systest filepath \n" +
                        "Systest file path: '" + testPath + "'");
                consoleView.print("Error: " + e.getMessage() + "\n", ConsoleViewContentType.ERROR_OUTPUT);
                for (StackTraceElement element : e.getStackTrace()) {
                    consoleView.print(element.toString() + "\n", ConsoleViewContentType.ERROR_OUTPUT);
                }
                toolWindow.show(null);
                return;
            }

            /// Construct the new path
            String extractedTargetDockerPath = extractTargetDockerPath(DockerCommand);
            if(extractedTargetDockerPath.isEmpty()){
                IllegalArgumentException e = new IllegalArgumentException(
                        "could not extract target docker path");
                consoleView.print("Error: " + e.getMessage() + "\n", ConsoleViewContentType.ERROR_OUTPUT);
                for (StackTraceElement element : e.getStackTrace()) {
                    consoleView.print(element.toString() + "\n", ConsoleViewContentType.ERROR_OUTPUT);
                }
                toolWindow.show(null);
                return;
            }

            String relativePathFromMainFolder = testPath.substring(mainFolderIndex);
            String newPath = Paths.get(extractedTargetDockerPath, relativePathFromMainFolder) + testIndexSuffix;

            commandLine = new GeneralCommandLine(DockerCommand.split(" "));
            commandLine.addParameter("-t");
            commandLine.addParameter(newPath);
            commandLine.setWorkDirectory(workingDir);
        }
        else{
            commandLine = new GeneralCommandLine("./systest");
            commandLine.addParameter("-t");
            commandLine.addParameter(testPath + testIndexSuffix);
            commandLine.setWorkDirectory(workingDir);
        }

        if(parameters != null){
            commandLine.addParameter(parameters);
        }

        consoleView.print("Running: '" + commandLine.getCommandLineString() + "'\n", ConsoleViewContentType.NORMAL_OUTPUT);

        try {
            /// run process
            OSProcessHandler processHandler = new OSProcessHandler(commandLine);

            /// forward process output to console
            processHandler.addProcessListener(new ProcessAdapter() {
                @Override
                public void onTextAvailable(ProcessEvent event, Key outputType) {
                    consoleView.print(event.getText(), ConsoleViewContentType.NORMAL_OUTPUT);
                }

                @Override
                public void processTerminated(ProcessEvent event) {
                    consoleView.print("Process finished with exit code " + event.getExitCode() + "\n", ConsoleViewContentType.SYSTEM_OUTPUT);
                }
            });
            processHandler.startNotify();

        }
        catch(Exception e) {
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

    public static String extractTargetDockerPath(String command) {
        /// Define the regex pattern to capture the target directory
        String regex = "-v\\s+[^:]+:(/[^\\s]+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(command);

        /// Find the match and return the captured group
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return "";
        }
    }
}
