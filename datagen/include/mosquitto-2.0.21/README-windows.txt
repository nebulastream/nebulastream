Mosquitto for Windows
=====================

Mosquitto for Windows comes in 64-bit and 32-bit flavours. All dependencies are
provided in the installer.

Installing
----------

Running the installer will present the normal type of graphical installer. If
you want to install without starting the graphical part of the installer, you
can do so by running it from a cmd prompt with the `/S` switch:

```
mosquitto-2.0.12-install-windows-x64.exe /S
```

You can override the installation directory with the `/D` switch:

```
mosquitto-2.0.12-install-windows-x64.exe /S /D=:\mosquitto
```


Capabilities
------------

Some versions of Windows have limitations on the number of concurrent
connections due to the Windows API being used. In modern versions of Windows,
e.g. Windows 10 or Windows Server 2019, this is approximately 8192 connections.
In earlier versions of Windows, t his limit is 2048 connections.


Websockets
----------

The broker executables provided in the installers have Websockets support
through a statically compiled version of libwebsockets and is being distributed
under the Static Linking Exception (Section 2) of the License. As a result, the
content is not subject to the LGPL 2.1.


Library Thread Support
----------------------

libmosquitto on Windows is currently compiled without thread support, so
neither of mosquitto_loop_start() nor "mosquitto_pub -l" are available.

A better solution that the old pthreads-win32 is being looked into, so support
will return in the future. If you need thread support, the code still supports
it just fine. Support has been dropped to simplify installation.


Windows Service
---------------

If you wish, mosquitto can be installed as a Windows service so you can
start/stop it from the control panel as well as running it as a normal
executable.

When running as a service, the configuration file used is mosquitto.conf in the
directory that you installed to.

If you want to install/uninstall mosquitto as a Windows service run from the
command line as follows:

C:\Program Files\mosquitto\mosquitto install
C:\Program Files\mosquitto\mosquitto uninstall

Logging
-------

If you use `log_dest file ...` in your configuration, the log file will be
created with security permissions for the current user only. If running as a
service, this means the SYSTEM user. You will only be able to view the log file
if you add permissions for yourself or whatever user you wish to view the logs.
