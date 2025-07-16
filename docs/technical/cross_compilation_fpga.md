# Cross Compilation

This document explains the process of compiling Nebulastream on an arm server for the Zynq Ultrascale ZCU106.

## Required Packages

On the arm server, install the following packages

```shell
sudo apt update
sudo apt install schroot kpartx
```

## Preparing the Root Directory

You need to create a root directory for the FPGA board

```shell
sudo mkdir zcu106_chroot
```

## Mounting the Image

Download the Ubuntu image (https://ubuntu.com/download/amd) and create loop device mappings for the partitions in the
image file (replace `<image>` with the name of the image)

```shell
sudo kpartx -av <image>.img
```

This will output the device mappings it creates, which is something like

```
/dev/mapper/loop0p1
/dev/mapper/loop0p2
```

You now need to mount the partition that contains the root filesystem of the Ubuntu image. Typically, this would be the
larger partition with an ext4 filesystem. For the ZCU106 image it is the second partition

```shell
sudo mount /dev/mapper/loop0p2 /mnt
```

## Copying Filesystem

Copy the whole partition

```shell
sudo cp -a /mnt zcu106_chroot
```

## Unmounting and Removing Device Mappings

Once you are done, unmount the partitions and remove the device mappings

```shell
sudo umount /mnt
sudo kpartx -d <image>.img
```

## Configuring schroot

Configure schroot to manage your chroot environment. Edit the configuration file

```shell
sudo nano /etc/schroot/chroot.d/zcu106
```

Add the following configuration and replace `<username>` with your username on the arm server

```
[zcu106]
description=ZCU106 Ubuntu Image
directory=zcu106_chroot
root-users=<username>
type=directory
personality=linux
preserve-environment=true
```

## Entering the chroot Environment

Enter the chroot environment

```shell
schroot -c zcu106
```

## Compiling

Install all required packages and clone the Nebulastream repository, as well as vcpkg. Compile as usual. Copy the
produced executables to root of the filesystem (home directory is not accessible from outside the chroot environment).
Exit the chroot environment and copy the executables to the FPGA board

## Troubleshooting

### Creating missing directories manually

If certain directories are missing when setting up schroot, you can manually create them within the chroot environment

```shell
sudo mkdir -p zcu106_chroot/run/systemd/resolve
```

### Checking Directory Permissions

Ensure that the permissions on your `zcu106_chroot` directory are correct

```shell
sudo chown -R <username>:<username> zcu106_chroot
```

### Fixing Ownership and Permissions

If you cannot use sudo within chroot after checking directory permissions, exit the chroot and fix sudo permissions

```shell
sudo chown root:root zcu106_chroot/etc/sudo.conf
sudo chown root:root zcu106_chroot/usr/bin/sudo
sudo chmod 4755 zcu106_chroot/usr/bin/sudo
sudo chown root:root zcu106_chroot/usr/libexec/sudo/sudoers.so
sudo chown root:root zcu106_chroot/etc/sudoers
sudo chown root:root zcu106_chroot/usr/libexec/sudo/sudoers.so
sudo chmod 755 zcu106_chroot/usr/libexec/sudo/sudoers.so
sudo chown root:root zcu106_chroot/etc/sudoers.d
sudo chown root:root zcu106_chroot/etc/sudoers.d/README
```

### Removing Problematic Overrides

You may need to remove override entries for some users/groups that don't exist

```shell
sudo dpkg-statoverride --remove geoclue
sudo dpkg-statoverride --remove ssl-cert
```

If the above commands don't work directly, you might need to manually edit the statoverride file

```shell
sudo nano /var/lib/dpkg/statoverride
```

Look for any lines that mention the problematic users/groups and remove them

### Mounting Necessary Filesystems (should not be necessary)

Before entering the chroot, you might need to mount some necessary virtual filesystems

```shell
sudo mount -t proc /proc zcu106_chroot/proc
sudo mount --bind /dev zcu106_chroot/dev
sudo mount --bind /sys zcu106_chroot/sys
```

Unmount them after exiting chroot

```shell
sudo umount zcu106_chroot/proc
sudo umount zcu106_chroot/dev
sudo umount zcu106_chroot/sys
```

### Configuring Network (should not be necessary)

If the build process requires network access, ensure that the network is configured correctly within the chroot
environment

```shell
sudo cp /etc/resolv.conf zcu106_chroot/etc/resolv.conf
```
