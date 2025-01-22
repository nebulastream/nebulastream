# NES-Systest-Runner

Adds interactable gutter icons for each System-Level Test .test file, 
that will run/debug a single specific test within that file or all tests within that file

### How to setup in CLion
- go to "IDE and Project Settings" and select "Plugins..." from the drop-down menu
- go to "Plugins" (Host) if using remote development
- uninstall the NES-Systest-Runner plugin if an old version exists (requires restart)
- click on the settings icon and select "Install Plugin from Disk..." from the drop-down menu
- navigate to "nes-systests/utils/SystestPlugin/NES-Systest-Runner-1.0-SNAPSHOT.zip" and select it
- install the plugin (does not require restart)

### How the plugin works
- Adds gutter icons to lines containing "----", which marks an assert, and at the beginning of the file for "run all"
- The plugin tries to find the "systest" configuration and creates a temporary copy of it
- the plugin checks whether to modify the program arguments in regards to the docker, by checking if the docker is used
in the CMake profile of the "systest" configuration and changes the path to "/tmp/nebulastream-public/..."

### Docker
- To run the system tests with docker, simply select a CMake Profile for the "systest" configuration that uses a docker 
toolchain

### Run systests
- navigate to any .test file
- within the code gutter, a green, clickable arrow icon will appear beside each line 
that contains the end of a test query "----"
- hovering above it will display a tooltip like this: "Run Systest 'TestNumber'" 
- click the icon which runs/debugs the system level test
