# NES-Systest-Runner

Adds interactable gutter icons for each System-Level Test .test file, 
that will run/debug a single specific test within that file or all tests within that file

### How to setup in CLion
- Go to "IDE and Project Settings" and select "Plugins..." from the drop-down menu
- Go to "Plugins" (Host) if using remote development
- Uninstall the NES-Systest-Runner plugin if an old version exists (requires restart)
- Click on the settings icon and select "Install Plugin from Disk..." from the drop-down menu
- Navigate to "nes-systests/utils/SystestPlugin/NES-Systest-Runner-1.2-SNAPSHOT.zip" and select it
- Install the plugin (does not require restart)

### How the plugin works
- Adds gutter icons to lines containing "----", which marks an assert, and at the beginning of the file for "run all"
- The plugin tries to find the "systest" configuration and creates a copy named "systest_plugin"
- Program arguments are taken over from "systest" to "systest_plugin", with the testLocation path being overridden
- The plugin accesses the CMake profile used for the "systest" configuration and construct the correct test path from the cppEnvironment

### Docker
- To run the system tests with docker, simply select a CMake Profile for the "systest" configuration that uses a docker 
toolchain

### Run systests
- Navigate to any .test file
- Within the code gutter, clickable run and debug icons will appear beside each line 
that contains the end of a test query "----"
- Hovering above them will display a tooltip: "Run Systest 'TestNumber'" 
- Click the icon to run/debug the system level test
- Press Shift+F10 / Shift+F9 to rerun / debug the last test
- To configure the program arguments, simply edit them in the "systest" configuration
- NOTE: changes in "systest_plugin" are temporary and will be overwritten

### Troubleshooting
- If there is no 'systest' configuration in your run/debug configurations, even after the CMake project has been reloaded, 
you might need to resolve any potential conflicts that lead to CLion not generating the Run/Debug configurations from the target. 
If that does not work, you may need to create the configuration manually, especially if the configuration was deleted.
