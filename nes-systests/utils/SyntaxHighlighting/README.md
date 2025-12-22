# Syntax Highlighting

We provide syntax highlighting settings for Clion system level test files.

## CLion

To import the syntax highlighting settings in CLion, follow these steps:

1. Open CLion and go to Help | Find Action....
2. In the search bar, type Import Settings....
3. A file selection dialog will appear. Navigate to the syntax highlighting file located at `nes-systests/utils/SyntaxHighlighting/Clion.zip`.
4. Select the Clion.zip file and click Open.
5. In the Import Settings dialog, make sure all components are selected.
6. Click OK to proceed.
7. Restart CLion to apply the new syntax highlighting settings.

To update the syntax highlighting settings the `filetypes/System-level test.xml` file within the zip must be edited by
adding or subtracting keywords from the asorted lists. Each list uses a different color within the editor (see below)  

After modifying the grammar the settings can be updated automatically by running the `updateHighlight.py` script, providing the `.g4` grammar file and the previous `Clion.zip` settings  


![Syntax Highlighting in Clion](syntaxHighlighting.png)
