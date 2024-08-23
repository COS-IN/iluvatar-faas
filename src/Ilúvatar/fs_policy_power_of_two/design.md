# Power of Two 


Each time a new cgroup is created it is assigned a specific Q for it's lifetime.

All tasks spawned in that cgroup are pushed to the assigned Q. 

It helps to preserve locality. 

Each Q is inturn associated with only 2 cores. 

Whichever core is idle is used to execute the next task in the Q for a default timeslice. 

Qs operate in fifo manner with tasks whose timeslice expires are pushed back of the Q.   



