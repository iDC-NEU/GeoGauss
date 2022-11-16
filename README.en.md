# GeoGauss

**GeoGauss: Strongly Consistent and Light-Coordinated OLTP forGeo-Replicated SQL Database**

GeoGauss proposes a strongly consistent OLTP database with full replica multi-master architecture. To efficiently merge the updates from different master nodes, we propose a multi-master OCC that unifies data replication and concurrent transaction processing. By leveraging an epoch based delta state merge rule and the optimistic asynchronous execution, GeoGauss ensures strong consistency with light-coordinated protocol and allows more concurrency with weak isolation (RC, RR, Snapshot Isolation(SI).





# Deploy GeoGauss

1. build environment

   Refer to the installation and deployment process of openGauss-2.0.0 https://github.com/opengauss-mirror/openGauss-server/blob/2.0.0/README.en.md to configure the system environment
   In addition to the environment required by openGauss-2.0.0, additional installation of zeromq, cppzmq, protobuf is required

2. Copy the binarylibs in the folder to the corresponding opengauss third-party dependency library.

3. Compile message.proto that is in GeoGauss/src/gausskernel/storage/mot/fdw_adapter/src using protobuf in the opengauss third-party dependency library, and put the generated file in GeoGauss/src/gausskernel/storage/mot/fdw_adapter/src.

4. Compile and install GeoGauss using the compile and install commands referring to opengauss-2.0.0.

5. Modify ServerInfo.xml, configure cluster information, and place ServerInfo.xml in /tmp directory. If you want to change the storage directory, you need to change the storage directory in src/gausskernel/process/postmaster/postmaster.cpp Line 11493.

6. Start the cluster, the cluster enters the stage of waiting for the start command.

7. Send start command: Others/time1 executable file refer to time1.cpp.

8. The next minute after the system receives the start command, the cluster starts to provide services.

# Test

You need to create roles on each server and assign corresponding database permissions firstly,

Log in to the database system through the role, and then create the relevant in-memory table.

Later, you need to inject table data into the cluster through one machine, and the data will be synchronized to other machines.



GeoGauss provides a standard jdbc interface. For specific test methods, refer to openGauss-MOT.























# openGauss

English | [简体中文](./README.md)



- [What Is openGauss](#what-is-opengauss)
- [Installation](#installation)
  - [Creating a Configuration File](#creating-a-configuration-file)
  - [Initializing the Installation Environment](#initializing-the-installation-environment)
- [Executing Installation](#executing-installation)
- [Uninstalling the openGauss](#uninstalling-the-openGauss)
- [Compilation](#compilation)
  - [Overview](#overview)
  - [OS and Software Dependency Requirements](#os-and-software-dependency-requirements)
  - [Downloading openGauss](#downloading-openGauss)
  - [Compiling Third-Party Software](#compiling-third-party-software)
  - [Compiling by build.sh](#compiling-by-build.sh)
  - [Compiling by Command](#compiling-by-command)
  - [Compiling the Installation Package](#compiling-the-installation-package)
- [Quick Start](#quick-start)
- [Docs](#docs)
- [Community](#community)
  - [Governance](#governance)
  - [Communication](#communication)
- [Contribution](#contribution)
- [Release Notes](#release-notes)
- [License](#license)

## What Is openGauss

openGauss is an open source relational database management system. It has multi-core high-performance, full link security, intelligent operation and maintenance for enterprise features. openGauss, which is early originated from PostgreSQL, integrates Huawei's core experience in database field for many years. It optimizes the architecture, transaction, storage engine, optimizer and ARM architecture. At the meantime, openGauss as a global database open source community, aims to further advance the development and enrichment of the database software/hardware application ecosystem.

<img src="../../Dropbox/GeoGauss/GeoGauss/doc/openGauss-architecture.en.png" alt="openGauss Architecture" width="600"/>

**High Performance**

openGauss breaks through the bottleneck of multi-core CPU, 2-way Kunpeng 128 core 1.5 million TPMC on disk-based row store and 3.5 million TPMC on MOT (Memory-Optimized Tables) Engine.

**Partitions**

Divide key data structure shared by internal threads into different partitions to reduce lock access conflicts. For example, CLOG uses partition optimization to solve the bottleneck of ClogControlLock.

**NUMA Structure**

Malloc key data structures help reduce cross CPU access. The global PGPROC array is divided into several parts according to the number of NUMA nodes, solving the bottleneck of ProcArrayLock. 

**Binding Cores**

Bind NIC interrupts to different cores and bind cores to different background threads to avoid performance instability due to thread migration between cores.

**ARM Optimization**

Optimize atomic operations based on ARM platform LSE instructions, implementing efficient operation of critical sections.

**SQL Bypass**

Optimize SQL execution process through SQL bypass, reducing CPU execution overhead.

**High Reliability**

Under normal service loads, the RTO is less than 10 seconds, reducing the service interruption time caused by node failure.

**Parallel Recovery**

When the Xlog is transferred to the standby node, the standby node flushs the Xlog to storage medium. At the meantime, the Xlog is sent to the redo recovery dispatch thread. The dispatch thread sends the Xlog to multiple parallel recovery threads to replay. Ensure that the redo speed of the standby node keeps up with the generation speed of the primary host. The standby node is ready in real time, which can be promoted to primary instantly. 

**MOT Engine (beta release)**

The Memory-Optimized Tables (MOT) storage engine is a transactional rowstore optimized for many-core and large memory and delivering extreme OLTP performance and high resources utilization. With data and indexes stored totally in-memory, a NUMA-aware design, algorithms that eliminate lock and latch contention and query native compilation (JIT), MOT provides low latency data access and more efficient transaction execution. See [MOT Engine documentation](https://opengauss.org/en/docs/2.0.0/docs/Developerguide/mot.html).

**Security**

openGauss supports account management, account authentication, account locking, password complexity check, privilege management and verification, transmission encryption, and operation audit, protecting service data security.

**Easy Operation and Maintenance**

openGauss integrates AI algorithms into databases, reducing the burden of database maintenance.

- **SQL Prediction**

openGauss supports SQL execution time prediction based on collected historical performance data.

- **SQL Diagnoser**

openGauss supports the diagnoser for SQL execution statements, finding out slow queries in advance..

- **Automatical Parameter Adjustment**

openGauss supports automatically adjusting database parameters, reducing the cost and time of parameter adjustment.

## Installation

### Creating a Configuration File

Before installing the openGauss, you need to create the clusterconfig.xml file. The configuration file in the XML format contains the information about the server where the openGauss is deployed, installation path, IP address, and port number. This file is used to guide how to deploy the openGauss. You need to configure the configuration file according to the actual deployment requirements.

The following describes how to create an XML configuration file based on the deployment solution of one primary node and one standby node.
The information of value is only an example. You can replace it as required. Each line of information is commented out.

```
<?xml version="1.0" encoding="utf-8"?>
<ROOT>
<!-- Overall information -->
  <CLUSTER>
  <!-- Database name -->
    <PARAM name="clusterName" value="dbCluster" />
  <!-- Database node name (hostname) -->
    <PARAM name="nodeNames" value="node1_hostname,node2_hostname"/>
  <!-- Node IP addresses corresponding to the node names, respectively -->
    <PARAM name="backIp1s" value="192.168.0.1,192.168.0.2"/>
  <!-- Database installation path -->
    <PARAM name="gaussdbAppPath" value="/opt/huawei/install/app" />
  <!-- Log directory -->
    <PARAM name="gaussdbLogPath" value="/var/log/omm" />
  <!-- Temporary file directory -->
    <PARAM name="tmpMppdbPath" value="/opt/huawei/tmp"/>
  <!-- Database tool directory -->
    <PARAM name="gaussdbToolPath" value="/opt/huawei/install/om" />
  <!--Directory of the core file of the database -->
    <PARAM name="corePath" value="/opt/huawei/corefile"/>
  <!-- openGauss deployment type. A single-instance cluster is used as an example here, that is, one primary node and multiple standby nodes are deployed. -->
    <PARAM name="clusterType" value="single-inst"/>
  </CLUSTER>
<!-- Information about node deployment on each server -->
  <DEVICELIST>
  <!-- Information about the node deployment on node1 -->
    <DEVICE sn="node1_hostname">
  <!-- Host name of node1 -->
      <PARAM name="name" value="node1_hostname"/>
  <!-- AZ where node1 is located and AZ priority -->
      <PARAM name="azName" value="AZ1"/>
      <PARAM name="azPriority" value="1"/>
  <!-- IP address of node1. If only one NIC is available for the server, set backIP1 and sshIP1 to the same IP address. -->
      <PARAM name="backIp1" value="192.168.0.1"/>
      <PARAM name="sshIp1" value="192.168.0.1"/>
      
  <!--DBnode-->
      <PARAM name="dataNum" value="1"/>
  <!-- Database node port number -->
      <PARAM name="dataPortBase" value="15400"/>
  <!-- Data directory on the primary database node and data directories of standby nodes -->
      <PARAM name="dataNode1" value="/opt/huawei/install/data/dn,node2_hostname,/opt/huawei/install/data/dn"/>
  <!-- Number of nodes for which the synchronization mode is set on the database node -->
 	  <PARAM name="dataNode1_syncNum" value="0"/>
    </DEVICE>
    
  <!-- Information about the node deployment on node2 -->
    <DEVICE sn="node2_hostname">
  <!-- Host name of node2 -->
      <PARAM name="name" value="node2_hostname"/>
  <!-- AZ where node2 is located and AZ priority -->
      <PARAM name="azName" value="AZ1"/>
      <PARAM name="azPriority" value="1"/>
  <!-- IP address of node2. If only one NIC is available for the server, set backIP1 and sshIP1 to the same IP address. -->
      <PARAM name="backIp1" value="192.168.0.2"/>
      <PARAM name="sshIp1" value="192.168.0.2"/>
    </DEVICE>
  </DEVICELIST>
</ROOT>
```

### Initializing the Installation Environment

After the openGauss configuration file is created, you need to run the gs_preinstall script to prepare the account and environment so that you can perform openGauss installation and management operations with the minimum permission, ensuring system security.

Executing the **gs_preinstall** script enables the system to automatically complete the following installation preparations:

- Sets kernel parameters for the SUSE Linux OS to improve server load performance. The kernel parameters directly affect database running status. Reconfigure them only when necessary. 
- Automatically copies the clusteropenGauss configuration files and installation packages to the same directory on each clusteropenGauss host.
- If the installation user and user group of the clusteropenGauss do not exist, the system automatically creates them.
- Reads the directory information in the clusteropenGauss configuration file, creates the directory, and grants the directory permission to the installation user.

**Precautions**

- You must check the upper-layer directory permissions to ensure that the user has the read, write, and execution permissions on the installation package and configuration file directory.
- The mapping between each host name and IP address in the XML configuration file must be correct.
- Only user root is authorized to run the gs_preinstall command.

**Procedure**

1. Log in to any host where the openGauss is to be installed as user root and create a directory for storing the installation package as planned.

   ```
   mkdir -p /opt/software/openGauss
   chmod 755 -R /opt/software
   ```

   > **NOTE:** 
   >
   > - Do not create the directory in the home directory or subdirectory of any openGauss user because you may lack permissions for such directories.
   > - The openGauss user must have the read and write permissions on the /opt/software/openGauss directory.

2. Upload the installation package openGauss-x.x.x-openEULER-64bit.tar.gz and the configuration file clusterconfig.xml to the directory created in the previous step.

3. Go to the directory for storing the uploaded software package and decompress the package openGauss-x.x.x-openEULER-64bit.tar.gz.After the installation package is decompressed, the script subdirectory is automatically generated in /opt/software/openGauss. OM tool scripts such as gs_preinstall are generated in the script subdirectory.

   ```
   cd /opt/software/openGauss
   tar -zxvf openGauss-x.x.x-openEULER-64bit.tar.gz
   ```

4. Go to the directory for storing tool scripts.

   ```
   cd /opt/software/openGauss/script
   ```

5. If the openEuler operating system is used, run the following command to open the **performance.sh** file, comment out **sysctl -w vm.min_free_kbytes=112640 &> /dev/null** using the number sign (#), press **Esc** to enter the command mode, and run the **:wq** command to save the modification and exit.

   ```
   vi /etc/profile.d/performance.sh
   ```

6. To ensure that the OpenSSL version is correct, load the lib library in the installation package before preinstallation. Run the following command. {packagePath} indicates the path where the installation package is stored. In this example, the path is /opt/software/openGauss.

   ```
   export LD_LIBRARY_PATH={packagePath}/script/gspylib/clib:$LD_LIBRARY_PATH
   ```

7. To ensure successful installation, check whether the values of hostname and /etc/hostname are the same. During preinstallation, the host name is checked.

8. Execute gs_preinstall to configure the installation environment. If the shared environment is used, add the --sep-env-file=ENVFILE parameter to separate environment variables to avoid mutual impact with other users. The environment variable separation file path is specified by users.
   Execute gs_preinstall in interactive mode. During the execution, the mutual trust between users root and between clusteropenGauss users is automatically established.	

   ```
   ./gs_preinstall -U omm -G dbgrp -X /opt/software/openGauss/clusterconfig.xml
   ```

   omm is the database administrator (also the OS user running the openGauss), dbgrp is the group name of the OS user running the openGauss, and /opt/software/ openGauss/clusterconfig.xml is the path of the openGauss configuration file. During the execution, you need to determine whether to establish mutual trust as prompted and enter the password of user root or the openGauss user.

### Executing Installation

After the openGauss installation environment is prepared by executing the pre-installation script, deploy openGauss based on the installation process.

**Prerequisites**

- You have successfully executed the gs_preinstall script. 
- All the server OSs and networks are functioning properly.
- You have checked that the locale parameter for each server is set to the same value. 

**Procedure**

1. (Optional) Check whether the installation package and openGauss configuration file exist in the planned directories. If no such package or file exists, perform the preinstallation again..

2. Log in to any host of the openGauss and switch to the omm user.

   ```
   su - omm
   ```

   > **NOTE:** 
   >
   > - omm indicates the user specified by the -U parameter in the gs_preinstall script.
   > - You need to execute the gs_install script as user omm specified in the gs_preinstall script. Otherwise, an execution error will be reported.

3. Use gs_install to install the openGauss. If the openGauss is installed in environment variable separation mode, run the source command to obtain the environment variable separation file ENVFILE.

   ```
   gs_install -X /opt/software/openGauss/clusterconfig.xml
   
   ```

   /opt/software/openGauss/script/clusterconfig.xml is the path of the openGauss configuration file. During the execution, you need to enter the database password as prompted. The password must meet complexity requirements. To ensure that you can use the database properly, remember the entered database password.

   The password must meet the following complexity requirements:

   - Contain at least eight characters.	
   - Cannot be the same as the username, the current password (ALTER), or the current password in an inverted sequence.
   - Contain at least three of the following: uppercase characters (A to Z), lowercase characters (a to z), digits (0 to 9), and other characters (limited to ~!@#$%^&*()-_=+\|[{}];:,<.>/?).

4. After the installation is successful, manually delete the trust between users root on the host, that is, delete the mutual trust file on each openGauss database node.

   ```
   rm -rf ~/.ssh
   
   ```

### Uninstalling the openGauss

The process of uninstalling the openGauss includes uninstalling the openGauss and clearing the environment of the openGauss server.

#### **Executing Uninstallation**

The openGauss provides an uninstallation script to help users uninstall the openGauss.

**Procedure**

1. Log in as the OS user omm to the host where the CN is located.

2. Execute the gs_uninstall script to uninstall the database cluster.

   ```
   gs_uninstall --delete-data
   
   ```

   Alternatively, execute uninstallation on each openGauss node.

   ```
   gs_uninstall --delete-data -L
   
   ```

#### **Deleting openGauss Configurations**

After the openGauss is uninstalled, execute the gs_postuninstall script to delete configurations from all servers in the openGauss if you do not need to re-deploy the openGauss using these configurations. These configurations are made by the gs_preinstall script.
**Prerequisites**

- The openGauss uninstallation task has been successfully executed.
- User root is trustworthy and available.
- Only user root is authorized to run the gs_postuninstall command.

**Procedure**

1. Log in to the openGauss server as user root.

2. Run the ssh Host name command to check whether mutual trust has been successfully established. Then, enter exit.

   ```
   plat1:~ # ssh plat2 
   Last login: Tue Jan  5 10:28:18 2016 from plat1 
   plat2:~ # exit 
   logout 
   Connection to plat2 closed. 
   plat1:~ #
   
   ```

3. Go to the following path:

   ```
   cd /opt/software/openGauss/script
   
   ```

4. Run the gs_postuninstall command to clear the environment. If the openGauss is installed in environment variable separation mode, run the source command to obtain the environment variable separation file ENVFILE.

   ```
   ./gs_postuninstall -U omm -X /opt/software/openGauss/clusterconfig.xml --delete-user --delete-group
   
   ```

   Alternatively, locally use the gs_postuninstall tool to clear each openGauss node.

   ```
   ./gs_postuninstall -U omm -X /opt/software/openGauss/clusterconfig.xml --delete-user --delete-group -L
   
   ```

   omm is the name of the OS user who runs the openGauss, and the path of the openGauss configuration file is /opt/software/openGauss/clusterconfig.xml.
   If the cluster is installed in environment variable separation mode, delete the environment variable separation parameter ENV obtained by running the source command.

   ```
   unset MPPDB_ENV_SEPARATE_PATH
   
   ```

5. Delete the mutual trust between the users root on each openGauss database node. 


## Compilation

### Overview

To compile openGauss, you need two components: openGauss-server and binarylibs.

- openGauss-server: main code of openGauss. You can obtain it from the open source community.

- binarylibs: third party open source software that openGauss depends on. You can obtain it by compiling the openGauss-third_party code or downloading from the open source community on which we have compiled a copy and uploaded it . The first method will be introduced in the following chapter.

Before you compile openGauss, please check the OS and software dependency requirements.

You can compile openGauss by build.sh, a one-click shell tool, which we will introduce later, or compile by command. Also, an installation package is produced by build.sh.

### OS and Software Dependency Requirements

The following OSs are supported:

- CentOS 7.6 (x86 architecture)

- openEuler-20.03-LTS (aarch64 architecture)


The following table lists the software requirements for compiling the openGauss.

You are advised to use the default installation packages of the following dependent software in the listed OS installation CD-ROMs or sources. If the following software does not exist, refer to the recommended versions of the software.

Software dependency requirements are as follows:

| Software      | Recommended Version |
| ------------- | ------------------- |
| libaio-devel  | 0.3.109-13          |
| flex          | 2.5.31 or later     |
| bison         | 2.7-4               |
| ncurses-devel | 5.9-13.20130511     |
| glibc-devel   | 2.17-111            |
| patch         | 2.7.1-10            |
| lsb_release   | 4.1                 |

### Downloading openGauss

You can download openGauss-server and openGauss-third_party from open source community.

https://opengauss.org/zh/

From the following website, you can obtain the binarylibs we have compiled. Please unzip it and rename to **binarylibs** after you download.

https://opengauss.obs.cn-south-1.myhuaweicloud.com/2.0.0/openGauss-third_party_binarylibs.tar.gz


Now we have completed openGauss code. For example, we store it in following directories.

- /sda/openGauss-server
- /sda/binarylibs
- /sda/openGauss-third_party

### Compiling Third-Party Software

Before compiling the openGauss, compile and build the open-source and third-party software on which the openGauss depends. These open-source and third-party software is stored in the openGauss-third_party code repository and usually needs to be built only once. If the open-source software is updated, rebuild the software.

You can also directly obtain the output file of the open-source software compilation and build from the **binarylibs** repository.

If you want to compile third-party by yourself, please go to openGauss-third_party repository to see details. 

After the preceding script is executed, the final compilation and build result is stored in the **binarylibs** directory at the same level as **openGauss-third_party**. These files will be used during the compilation of **openGauss-server**.

### Compiling code

##### Compiling by build.sh

build.sh in openGauss-server is an important script tool during compilation. It integrates software installation and compilation and product installation package compilation functions to quickly compile and package code.

The following table describes the parameters.

| Option | Default Value                | Parameter                              | Description                                                  |
| :----- | :--------------------------- | :------------------------------------- | :----------------------------------------------------------- |
| -h     | Do not use this option.      | -                                      | Help menu.                                                   |
| -m     | release                      | [debug &#124; release &#124; memcheck] | Selects the target version.                                  |
| -3rd   | ${Code directory}/binarylibs | [binarylibs path]                      | Specifies the path of binarylibs. The path must be an absolute path. |
| -pkg   | Do not use this option.      | -                                      | Compresses the code compilation result into an installation package. |
| -nopt  | Do not use this option.      | -                                      | On kunpeng platform, like 1616 version, without LSE optimized. |

> **NOTICE:** 
>
> - **-m [debug | release | memcheck]** indicates that three target versions can be selected:
>   - **release**: indicates that the binary program of the release version is generated. During compilation of this version, the GCC high-level optimization option is configured to remove the kernel debugging code. This option is usually used in the generation environment or performance test environment.
>   - **debug**: indicates that a binary program of the debug version is generated. During compilation of this version, the kernel code debugging function is added, which is usually used in the development self-test environment.
>   - **memcheck**: indicates that a binary program of the memcheck version is generated. During compilation of this version, the ASAN function is added based on the debug version to locate memory problems.
> - **-3rd [binarylibs path]** is the path of **binarylibs**. By default, **binarylibs** exists in the current code folder. If **binarylibs** is moved to **openGauss-server** or a soft link to **binarylibs** is created in **openGauss-server**, you do not need to specify the parameter. However, if you do so, please note that the file is easy to be deleted by the **git clean** command.
> - Each option in this script has a default value. The number of options is small and the dependency is simple. Therefore, this script is easy to use. If the required value is different from the default value, set this parameter based on the actual requirements.

Now you know the usage of build.sh, so you can compile the openGauss-server by one command with build.sh.

```
[user@linux openGauss-server]$ sh build.sh -m [debug | release | memcheck] -3rd [binarylibs path]

```

For example: 

```
[user@linux openGauss-server]$ sh build.sh       # Compile openGauss of the release version. The binarylibs or its soft link must exist in the code directory. Otherwise, the operation fails.
[user@linux openGauss-server]$ sh build.sh -m debug -3rd /sda/binarylibs    # Compilate openGauss of the debug version using binarylibs we put on /sda/

```

The software installation path after compilation is **/sda/openGauss-server/dest**.

The compiled binary files are stored in **/sda/openGauss-server/dest/bin**.

Compilation log: **make_compile.log**

##### Compiling by Command

1. Run the following script to obtain the system version:

   ```
   [user@linux openGauss-server]$ sh src/get_PlatForm_str.sh
   
   ```

   > **NOTICE:** 
   >
   > - The command output indicates the OSs supported by the openGauss. The OSs supported by the openGauss are centos7.6_x86_64 and openeuler_aarch64.
   > - If **Failed** or another version is displayed, the openGauss does not support the current operating system.

2. Configure environment variables, add **____** based on the code download location, and replace *** with the result obtained in the previous step.

   ```
   export CODE_BASE=________     # Path of the openGauss-server file
   export BINARYLIBS=________    # Path of the binarylibs file
   export GAUSSHOME=$CODE_BASE/dest/
   export GCC_PATH=$BINARYLIBS/buildtools/***/gcc8.2/
   export CC=$GCC_PATH/gcc/bin/gcc
   export CXX=$GCC_PATH/gcc/bin/g++
   export LD_LIBRARY_PATH=$GAUSSHOME/lib:$GCC_PATH/gcc/lib64:$GCC_PATH/isl/lib:$GCC_PATH/mpc/lib/:$GCC_PATH/mpfr/lib/:$GCC_PATH/gmp/lib/:$LD_LIBRARY_PATH
   export PATH=$GAUSSHOME/bin:$GCC_PATH/gcc/bin:$PATH
   
   
   ```

   For example, on CENTOS X86-64 platform, binarylibs directory is placed as the sibling directory of openGauss-server directory.
   The following command can be executed under openGauss-server directory.

   ```
   export CODE_BASE=`pwd`
   export BINARYLIBS=`pwd`/../binarylibs
   export GAUSSHOME=$CODE_BASE/dest/
   export GCC_PATH=$BINARYLIBS/buildtools/centos7.6_x86_64/gcc8.2/
   export CC=$GCC_PATH/gcc/bin/gcc
   export CXX=$GCC_PATH/gcc/bin/g++
   export LD_LIBRARY_PATH=$GAUSSHOME/lib:$GCC_PATH/gcc/lib64:$GCC_PATH/isl/lib:$GCC_PATH/mpc/lib/:$GCC_PATH/mpfr/lib/:$GCC_PATH/gmp/lib/:$LD_LIBRARY_PATH
   export PATH=$GAUSSHOME/bin:$GCC_PATH/gcc/bin:$PATH
   
   
   ```

3. Select a version and configure it.

   **debug** version:

   ```
   ./configure --gcc-version=8.2.0 CC=g++ CFLAGS='-O0' --prefix=$GAUSSHOME --3rd=$BINARYLIBS --enable-debug --enable-cassert --enable-thread-safety --without-readline --without-zlib
   
   ```

   **release** version:

   ```
   ./configure --gcc-version=8.2.0 CC=g++ CFLAGS="-O2 -g3" --prefix=$GAUSSHOME --3rd=$BINARYLIBS --enable-thread-safety --without-readline --without-zlib
   
   ```

   **memcheck** version:

   ```
   ./configure --gcc-version=8.2.0 CC=g++ CFLAGS='-O0' --prefix=$GAUSSHOME --3rd=$BINARYLIBS --enable-debug --enable-cassert --enable-thread-safety --without-readline --without-zlib --enable-memory-check
   
   ```

   > **NOTICE:** 
   >
   > - *[debug | release | memcheck]* indicates that three target versions are available. 
   > - On the ARM-based platform, **-D__USE_NUMA** needs to be added to **CFLAGS**.
   > - On the **ARMv8.1** platform or a later version (for example, Kunpeng 920), **-D__ARM_LSE** needs to be added to **CFLAGS**.
   > - If **binarylibs** is moved to **openGauss-server** or a soft link to **binarylibs** is created in **openGauss-server**, you do not need to specify the **--3rd** parameter. However, if you do so, please note that the file is easy to be deleted by the `git clean` command.

4. Run the following commands to compile openGauss:

   ```
   [user@linux openGauss-server]$ make -sj
   [user@linux openGauss-server]$ make install -sj
   
   ```

5. If the following information is displayed, the compilation and installation are successful:

   ```
   openGauss installation complete.
   
   ```

   The software installation path after compilation is **$GAUSSHOME**.

   The compiled binary files are stored in **$GAUSSHOME/bin**.



### Compiling the Installation Package 

Please read the chapter **Compiling by build.sh** first to understand the usage of build.sh and how to compile openGauss by using the script.

Now you can compile the installation package with just adding an option `-pkg`.

```
[user@linux openGauss-server]$ sh build.sh -m [debug | release | memcheck] -3rd [binarylibs path] -pkg

```

For example:

```
[user@linux openGauss-server]$ sh build.sh -pkg      # Compile openGauss installation package of the release version. The binarylibs or its soft link must exist in the code directory. Otherwise, the operation fails.
[user@linux openGauss-server]$ sh build.sh -m debug -3rd /sda/binarylibs -pkg   # Compile openGauss installation package of the debug version using binarylibs we put on /sda/

```

The generated installation package is stored in the **./package** directory.

Compilation log: **make_compile.log**

Installation package packaging log: **./package/make_package.log**

## Quick Start

See the [Quick Start](https://opengauss.org/en/docs/2.0.0/docs/Quickstart/Quickstart.html).

## Docs

For more details about the installation guide, tutorials, and APIs, please see the [User Documentation](https://gitee.com/opengauss/docs).

## Community

### Governance

Check out how openGauss implements open governance [works](https://gitee.com/opengauss/community/blob/master/governance.md).

### Communication

- WeLink- Communication platform for developers.
- IRC channel at `#opengauss-meeting` (only for meeting minutes logging purpose)
- Mailing-list: https://opengauss.org/en/community/onlineCommunication.html

## Contribution

Welcome contributions. See our [Contributor](https://opengauss.org/en/contribution.html) for more details.

## Release Notes

For the release notes, see our [RELEASE](https://opengauss.org/en/docs/2.0.0/docs/Releasenotes/Releasenotes.html).

## License

[MulanPSL-2.0](http://license.coscl.org.cn/MulanPSL2/)