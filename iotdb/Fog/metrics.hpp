#ifndef _METRICS_H
#define _METRICS_H

#include <sys/sysinfo.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <ifaddrs.h>
#include <linux/if_link.h>
#include <netdb.h>
#include <net/if.h>

#include <fstream>
#include "json.hpp"

using JSON = nlohmann::json;
typedef unsigned long uint64_t;

#define PROC_STAT_CPU_COLUMNS 11

// memory metrics
// struct sinfo {
//   long uptime;             /* Seconds since boot */
//   unsigned long loads[3];  /* 1, 5, and 15 minute load averages */
//   unsigned long totalram;  /* Total usable main memory size */
//   unsigned long freeram;   /* Available memory size */
//   unsigned long sharedram; /* Amount of shared memory */
//   unsigned long bufferram; /* Memory used by buffers */
//   unsigned long totalswap; /* Total swap space size */
//   unsigned long freeswap;  /* Swap space still available */
//   unsigned short procs;    /* Number of current processes */
//   unsigned long totalhigh; /* Total high memory size */
//   unsigned long freehigh;  /* Available high memory size */
//   unsigned int mem_unit;   /* Memory unit size in bytes */
//   char _f[20-2*sizeof(long)-sizeof(int)];  /* Padding to 64 bytes */
// };
// file system metrics
// struct statvfs {
//   unsigned long  f_bsize;    /* file system block size */
//   unsigned long  f_frsize;   /* fragment size */
//   fsblkcnt_t     f_blocks;   /* size of fs in f_frsize units */
//   fsblkcnt_t     f_bfree;    /* # free blocks */
//   fsblkcnt_t     f_bavail;   /* # free blocks for unprivileged users */
//   fsfilcnt_t     f_files;    /* # inodes */
//   fsfilcnt_t     f_ffree;    /* # free inodes */
//   fsfilcnt_t     f_favail;   /* # free inodes for unprivileged users */
//   unsigned long  f_fsid;     /* file system ID */
//   unsigned long  f_flag;     /* mount flags */
//   unsigned long  f_namemax;  /* maximum filename length */
// };
// cpu metrics
// read from /proc/stat
// TODO
// network metrics


// struct cpuinfo {
//   unsigned long user;
//   unsigned long nice;
//   unsigned long system;
//   unsigned long idle;
//   unsigned long iowait;
//   unsigned long irq;
//   unsigned long softirq;
//   unsigned long steal;
//   unsigned long guest;
//   unsigned long guest_nice;
//   char name[8];

//   JSON load(std::string cpuInfoStr) {
//     JSON cpu = JSON::parse(cpuInfoStr);
//     return cpu;
//   }
//   JSON load() {
//     JSON cpu;
//     cpu["name"] = name;
//     cpu["user"] = user;
//     cpu["nice"] = nice;
//     cpu["system"] = system;
//     cpu["idle"] = idle;
//     cpu["iowait"] = iowait;
//     cpu["irq"] = irq;
//     cpu["softirq"] = softirq;
//     cpu["steal"] = steal;
//     cpu["guest"] = guest;
//     cpu["guest_nice"] = guest_nice;
//     return cpu;
//   }
//   std::string dump() {
//     JSON cpu;
//     cpu["name"] = name;
//     cpu["user"] = user;
//     cpu["nice"] = nice;
//     cpu["system"] = system;
//     cpu["idle"] = idle;
//     cpu["iowait"] = iowait;
//     cpu["irq"] = irq;
//     cpu["softirq"] = softirq;
//     cpu["steal"] = steal;
//     cpu["guest"] = guest;
//     cpu["guest_nice"] = guest_nice;
//     return cpu.dump();
//   }
// };

// struct netinfo {
//   char name[NI_MAXHOST];
// };

class Metrics {
public:
  Metrics() {
    // this->readCpuStats();
    // this->readNetworkStats();
    // this->readMemStats();
    // this->readFsStats();
  }

  ~Metrics() {
    if (this->sinfo)
      free(this->sinfo);
    if (this->svfs)
      free(this->svfs);
    if (this->ifaddr)
      freeifaddrs(this->ifaddr);
  }

  void print();
  std::string dump(int setw=-1);
  JSON load(const char *metricsBuffer);
  JSON load();
public:
  void readCpuStats();           // read cpu inforamtion
  void readMemStats();           // read memory information
  void readFsStats();            // read file system information
  void readNetworkStats();       // read network information

private:
  /*
   * memory information
   * /proc/meminfo
   * /proc/loadavg
   */
  struct sysinfo *sinfo;
  /*
   * file system information
   * /proc/diskstats
   */
  struct statvfs *svfs;
  /*
   * network information
   * /proc/net/dev
   *
   */
  struct ifaddrs *ifaddr;

  long nbrProcessors;

  JSON _metrics;
  JSON _mem;
  JSON _fs;
  JSON _cpus;
  JSON _nets;
};
#endif
