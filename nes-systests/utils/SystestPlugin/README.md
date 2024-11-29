# NES-Systest-Runner

Adds interactable gutter icons for each System-Level Test .test file, that will run a single specific test within that file.

### How to setup in CLion
- go to "Project Settings"
- select "Plugins"
- click on "Manage repositories, Configure proxy or install plugins from disk"
- select "install from disk..."
- navigate to tests/utils/SystestPlugin/NES-Systest-Runner-1.0-SNAPSHOT.zip and select it
- install the plugin (does not necessarily require restart)

### Configuration:
- To run the system tests, the plugin requires the path of the folder of the 'systest' binary (not the binary itself).
- Go to Project Settings -> Tools -> Nes-SysTest-Runner
- Then, for "./systest folder path:" click on the file selector and navigate to the directory (which contains the binary 'systest')
  The folder is located somewhere at: path\to\nebulastream-public\build\nes-systests\systest

### Docker
- To run the system tests within the docker, you will need to enable running with docker in the plugin settings and configure the used docker command
- Go to Project Settings -> Tools -> Nes-SysTest-Runner
- Then, enable the checkbox "Use Docker?" to run using your predefined docker command instead
- e.g.: 'docker run -it -v /home/user/workspace/nebulastream-public/:/tmp local:latest /tmp/nebulastream-public/cmake-build-docker/nes-systests/systest/systest'
- NOTE: the plugin will extract the mounted target path (/tmp/ in this case) to construct the system test's file path 
- NOTE: the plugin will assume the nebulastream-public directory to exist for the path to the system tests

### Run systests
- navigate to any .test file
- within the code gutter, a green, clickable arrow icon will appear beside each line 
that contains the end of a test query "----"
- hovering above it will display a tooltip like this: "Run Systest 'TestNumber'" 
- click the icon to run "./systest -t 'PathToFile'/'FileName':'TestNumber'", which runs the system level test,
and display the output in a new console
