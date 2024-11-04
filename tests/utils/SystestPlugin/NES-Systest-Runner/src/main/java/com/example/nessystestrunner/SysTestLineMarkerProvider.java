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
        // no-op to satisfy interface requirement
        return null;
    }

    @Override
    public void collectSlowLineMarkers(@NotNull List<? extends PsiElement> elements,
                                       @NotNull Collection<? super LineMarkerInfo<?>> result) {
        if (elements.isEmpty()) return;

        PsiFile file = elements.get(0).getContainingFile();
        if (file == null || !file.getName().endsWith(".test")) {
            return;
        }

        // pattern match all occurrences of a test using "----", which indicates the end of the system test query
        String fileText = file.getText();
        Pattern pattern = Pattern.compile("----");
        Matcher matcher = pattern.matcher(fileText);

        // index to determine test count, later inserted into console command
        int systestIndex = 1;

        // loop through each match of the pattern
        while (matcher.find()) {

            int startOffset = matcher.start();

            // calculate line start and end offsets manually
            int lineStartOffset = fileText.lastIndexOf('\n', startOffset - 1) + 1;
            int lineEndOffset = fileText.indexOf('\n', startOffset);
            if (lineEndOffset == -1) { // Handle last line without newline
                lineEndOffset = fileText.length();
            }

            // find the element that corresponds to the start of the line
            PsiElement lineElement = file.findElementAt(lineStartOffset);
            if (lineElement == null) continue; // Ensure the element is not null

            // create the text range for the entire line
            TextRange lineTextRange = new TextRange(lineStartOffset, lineEndOffset);

            // add interactable gutter icon
            if (lineElement != null) {
                // create the LineMarkerInfo for the gutter icon
                int currentSystestIndex = systestIndex;
                LineMarkerInfo<PsiElement> lineMarkerInfo = new LineMarkerInfo<>(
                        lineElement,                                        // target line element
                        lineTextRange,                                      // icon position
                        AllIcons.Actions.Execute,                           // icon sprite
                        psiElement -> "Run SysTest " + currentSystestIndex, // tooltip
                        (e, elt) -> {                                       // clickable function
                            try {
                                executeCustomCommand(elt.getProject(), file.getName() + ":" + currentSystestIndex);
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        },
                        GutterIconRenderer.Alignment.CENTER                 // icon alignment
                );
                result.add(lineMarkerInfo);
                systestIndex++;
            }
        }
    }

    public static void executeCustomCommand(Project project, String fileName) throws ExecutionException, InterruptedException {

        String cmd_systest = "systest -t " + fileName;


        String cmd = "ping";

        // synthesize command string and create
        GeneralCommandLine commandLine = new GeneralCommandLine(cmd);

        // TODO: determine location of systest and from where it should be called called, i.e. the build folder
        // commandLine.setExePath("/path/to/systest");
        // commandLine.setWorkDirectory("/path/to/working/directory");

        // create process
        OSProcessHandler processHandler = new OSProcessHandler(commandLine);

        // create console view
        ConsoleView consoleView = new ConsoleViewImpl(project, false);

        // create Nes-Systest-Runner window or reuse existing one to show console output
        ToolWindow toolWindow = ToolWindowManager.getInstance(project)
                .getToolWindow("NES-Systest-Runner");
        if (toolWindow == null) {
            toolWindow = ToolWindowManager.getInstance(project)
                    .registerToolWindow("NES-Systest-Runner", true, ToolWindowAnchor.BOTTOM);
        }

        // add console to the tool window content
        var contentFactory = ContentFactory.getInstance();
        var content = contentFactory.createContent(consoleView.getComponent(), "Command Output", false);
        toolWindow.getContentManager().addContent(content);
        toolWindow.activate(null);

        // attach process to console
        processHandler.addProcessListener(new ProcessAdapter() {
            @Override
            public void onTextAvailable(ProcessEvent event, Key outputType) {
                // forward process output to console
                ConsoleViewContentType contentType = outputType == ProcessOutputTypes.STDOUT
                        ? ConsoleViewContentType.NORMAL_OUTPUT
                        : ConsoleViewContentType.ERROR_OUTPUT;
                consoleView.print(event.getText(), contentType);
            }

            @Override
            public void processTerminated(ProcessEvent event) {
                consoleView.print("Process finished with exit code " + event.getExitCode() + "\n", ConsoleViewContentType.SYSTEM_OUTPUT);
                // TODO: remove once finding systest is implemented
                consoleView.print("This is a WIP and would normally run '" + cmd_systest + "' instead.\n", ConsoleViewContentType.SYSTEM_OUTPUT);
            }
        });

        // run and output process
        processHandler.startNotify();
    }
}
