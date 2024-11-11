package com.example.nessystestrunner;
import com.intellij.codeInsight.daemon.LineMarkerInfo;
import com.intellij.codeInsight.daemon.LineMarkerProvider;
import com.intellij.execution.process.*;
import com.intellij.execution.ui.*;
import com.intellij.openapi.editor.markup.GutterIconRenderer;
import com.intellij.openapi.util.Key;
import com.intellij.openapi.util.TextRange;
import com.intellij.openapi.wm.*;
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
import com.intellij.execution.ExecutionException;
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


    /// The Plugin affects all files of type '.test', as defined in the plugin.xml
    /// This searches the currently open file for queries and adds an interactable Gutter Icon
    /// that can run the corresponding system test
    @Override
    public void collectSlowLineMarkers(@NotNull List<? extends PsiElement> elements,
                                       @NotNull Collection<? super LineMarkerInfo<?>> result) {

        if (elements.isEmpty()) return;

        PsiFile file = elements.get(0).getContainingFile();
        if (file == null || !file.getName().endsWith(".test")) {
            return;
        }

        /// pattern match all occurrences of a test using "----", which indicates the end of the system test query
        String fileText = file.getText();
        Pattern pattern = Pattern.compile("----");
        Matcher matcher = pattern.matcher(fileText);

        int systestIndex = 1;

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

            /// create the LineMarkerInfo for the gutter icon
            int currentSystestIndex = systestIndex;
            String testPath = file.getVirtualFile().getPath() + ":" + String.format("%02d", currentSystestIndex);
            LineMarkerInfo<PsiElement> lineMarkerInfo = new LineMarkerInfo<>(
                    lineElement,
                    lineTextRange,
                    AllIcons.Actions.Execute,
                    psiElement -> "Run System Test " + currentSystestIndex,
                    (e, elt) -> { runSysTest(elt.getProject(), testPath); },
                    GutterIconRenderer.Alignment.CENTER
            );
            result.add(lineMarkerInfo);
            systestIndex++;
        }
    }

    /// This function is called when the systest Gutter Icon is clicked.
    /// It opens a new console "Nes-Systest-Runner" and adds it to the toolWindow,
    /// runs the systest command and prints the output
    public static void runSysTest(Project project, String testPath) {

        /// TODO: make the path to the build folder configurable using the project settings tab in CLion

        File workingDir = Paths.get(System.getProperty("user.home"),
                "nebulastream-public", "build", "nes-systests", "systest").toFile();

        GeneralCommandLine commandLine = new GeneralCommandLine("./systest")
                .withParameters("-t", testPath)
                .withWorkDirectory(workingDir);

        ConsoleView consoleView = new ConsoleViewImpl(project, false);

        /// create Nes-Systest-Runner window or reuse existing one to show console output
        ToolWindow toolWindow = ToolWindowManager.getInstance(project).getToolWindow("NES-Systest-Runner");
        if (toolWindow == null) {
            toolWindow = ToolWindowManager.getInstance(project).registerToolWindow(
                    "NES-Systest-Runner",
                    true,
                    ToolWindowAnchor.BOTTOM);
        }

        /// add console to the tool window content
        var contentFactory = ContentFactory.getInstance();
        var content = contentFactory.createContent(consoleView.getComponent(), "Command Output", false);
        toolWindow.getContentManager().addContent(content);
        toolWindow.activate(null);

        try{
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
        catch(ExecutionException | RuntimeException e) {
            consoleView.print("Error: " + e.getMessage() + "\n", ConsoleViewContentType.ERROR_OUTPUT);
            for (StackTraceElement element : e.getStackTrace()) {
                consoleView.print(element.toString() + "\n", ConsoleViewContentType.ERROR_OUTPUT);
            }
            toolWindow.show(null);
        }
    }
}
