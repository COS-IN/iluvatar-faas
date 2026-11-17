
CPU Scheduler using SCX 

Main source directories: 

iluvatar-faas/src/Ilúvatar/iluvatar_finesched/src/



* Queue-peek 
** 1 lookahead
- Look at the next invocation in queue, and adjust the scheduling of the current function accordingly. 
- For Power of 2 choices: see if there is an overlap in the chosen domains.
  - Question: for the random Po2, how does this work? How do we know the future random domain? 
  - For hash-based deterministic power of two, it makes sense. 
  - So if the future invok is larger and there is overlap in domains, then the current invok can be selfless and sacrifice its lower loaded domain and leave that for the next invok. 
  
  
** K lookahead 
- For larger lookahead windows, this will need a different approach. 
- Can define anticipated load (aLoad) as the future load over a window of K reqests. 
- The aLoad is calculated before every placement decision. 
- Once the domains likely assigned to a function are known (say d1 and d2 for po2-deterministic), then the invocations which also map to the same domains anticipated load is computed (based on their expected execution time, mostly warm), and then this load is added to the real load,
- For 2 potential domains, it is "spread around" and each domain gets half of this. 
- Question: Using pelt-like utilization seems to work well as load indicator. But adding warm-time latency to this will not work (different units). 
  - Will need to map the function exec latency to utilization increase. We store this in some map? Can use with some caution. 
- Additional filtering can be done: only compute anticip load of items larger than some threshold (> 2*current function's warm time). Since they impact the domain load much more. And fewer 'guessed' anticipated load items added. 
- 

* Burst mitigation ideas 
** Power of 2 
- Local Burst Trigger (min(load(d1), load(d2)) >= capacity_threshold):
The moment both deterministic home domains are saturated (meaning function $f$ itself—or its two hash neighbors—is experiencing a genuine burst), switch that invocation to Power of Two Random Choices across the remaining $D - 2$ domains (or pick the minimum of d1, d2, plus 1–2 random probes).
	- The current burst/high load heuristics can also be used (they detect *future* load increases which are more useful and novel). 
- 
