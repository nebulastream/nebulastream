# NES-Systest-Runner

Adds interactable gutter icons for each System-Level Test .test file, that will run a single specific test within that file.

### How to setup in CLion
- go to "Project Settings"
- select "Plugins"
- click on "Manage repositories, Configure proxy or install plugins from disk"
- select "install from disk..."
- navigate to tests/utils/SystestPlugin/NES-Systest-Runner-1.0-SNAPSHOT.zip and select it
- install the plugin and restart IDE (confirm if necessary)

### Run systests
- navigate to any .test file
- within the code gutter, a green, clickable arrow icon will appear beside each line 
that contains the end of a test query "----"
- hovering above it will display a tooltip like this: "Run Systest 'TestNumber'" 
- click the icon to run "systest -t 'FileName':'TestNumber'", which runs the system level test,
and display the output in a new console