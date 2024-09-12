# Reading cgroup stuff 


```

reading at start of invoke: Ok(
  CGROUPReading { 
    usr: 14376000000, 
    sys: 2064000000, 
    pcpu_usr: [0, 0, 236000000, 0, 0, 0, 0, 332000000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11904000000, 1256000000, 0, 0, 0, 620000000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 28000000, 0, 0, 0, 0], 
    pcpu_sys: [0, 0, 60000000, 0, 0, 0, 0, 68000000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1672000000, 128000000, 0, 0, 0, 112000000, 0, 0, 0, 0, 0, 0, 0, 0, 4000000, 0, 0, 20000000, 0, 0, 0, 0], 
    threads: [2893364, 2893462], 
    procs: [2893364, 2893462], 
    cpustats: {"nr_bursts": 0, "nr_periods": 0, "nr_throttled": 0, "throttled_time": 0, "burst_time": 0}, 

    v2: 
      CGROUPReadingV2 { 
        threads: [2893364, 2893462], 
        procs: [2893364, 2893462], 
        cpustats: {"user_usec": 14361899, "system_usec": 2061975, "usage_usec": 16423875}, 
        cpupsi: CGROUPV2Psi { 
          some: CGROUPV2PsiVal { avg10: 0.0, avg60: 0.0, avg300: 0.0, total: 7447 }, 
          full: CGROUPV2PsiVal { avg10: 0.0, avg60: 0.0, avg300: 0.0, total: 7439 } 
        }, 
        mempsi: CGROUPV2Psi { 
          some: CGROUPV2PsiVal { avg10: 0.0, avg60: 0.0, avg300: 0.0, total: 0 }, 
          full: CGROUPV2PsiVal { avg10: 0.0, avg60: 0.0, avg300: 0.0, total: 0 } 
        }, 
        iopsi: CGROUPV2Psi { 
          some: CGROUPV2PsiVal { avg10: 0.3, avg60: 0.19, avg300: 0.05, total: 268096 }, 
          full: CGROUPV2PsiVal { avg10: 0.3, avg60: 0.19, avg300: 0.05, total: 268096 } 
        } 
      } 
  }
)

reading at end of invoke: Ok(
  CGROUPReading { 
    usr: 14560000000, 
    sys: 2088000000, 
    pcpu_usr: [0, 0, 236000000, 0, 0, 0, 0, 332000000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11904000000, 1440000000, 0, 0, 0, 620000000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 28000000, 0, 0, 0, 0], 
    pcpu_sys: [0, 0, 60000000, 0, 0, 0, 0, 68000000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1672000000, 152000000, 0, 0, 0, 112000000, 0, 0, 0, 0, 0, 0, 0, 0, 4000000, 0, 0, 20000000, 0, 0, 0, 0], 
    threads: [2893364, 2893462], 
    procs: [2893364, 2893462], 
    cpustats: {"nr_bursts": 0, "burst_time": 0, "nr_throttled": 0, "throttled_time": 0, "nr_periods": 0}, 
    v2: CGROUPReadingV2 { 
      threads: [2893364, 2893462], 
      procs: [2893364, 2893462], 
      cpustats: {"user_usec": 14546077, "usage_usec": 16632080, "system_usec": 2086003}, 
      cpupsi: CGROUPV2Psi { 
        some: CGROUPV2PsiVal { avg10: 0.0, avg60: 0.0, avg300: 0.0, total: 7503 }, 
        full: CGROUPV2PsiVal { avg10: 0.0, avg60: 0.0, avg300: 0.0, total: 7494 } 
      }, 
      mempsi: CGROUPV2Psi { 
        some: CGROUPV2PsiVal { avg10: 0.0, avg60: 0.0, avg300: 0.0, total: 0 }, 
        full: CGROUPV2PsiVal { avg10: 0.0, avg60: 0.0, avg300: 0.0, total: 0 } 
      }, 
      iopsi: CGROUPV2Psi { 
        some: CGROUPV2PsiVal { avg10: 0.3, avg60: 0.19, avg300: 0.05, total: 268096 }, 
        full: CGROUPV2PsiVal { avg10: 0.3, avg60: 0.19, avg300: 0.05, total: 268096 } 
      } 
    } 
  }
)
```

