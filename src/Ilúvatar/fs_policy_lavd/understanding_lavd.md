
<!-- vim-markdown-toc Marked -->

* [Understanding the LAVD Policy](#understanding-the-lavd-policy)
  * [What is the context? (read)](#what-is-the-context?-(read))
  * [What is the summary of what I just read? (read)](#what-is-the-summary-of-what-i-just-read?-(read))
    * [Summary of Functions and One-Liner description](#summary-of-functions-and-one-liner-description)
      * [Helper Funcs](#helper-funcs)
        * [boost_lat](#boost_lat)
        * [calc_virtual_deadline_delta](#calc_virtual_deadline_delta)
        * [calc_eligible_delta](#calc_eligible_delta)
        * [find_victim_cpu](#find_victim_cpu)
        * [try_find_and_kick_victim_cpu](#try_find_and_kick_victim_cpu)
        * [put_global_rq](#put_global_rq)
        * [scx_bpf_dispatch_vtime](#scx_bpf_dispatch_vtime)
        * [update_stat_for_runnable](#update_stat_for_runnable)
        * [calc_task_load_actual](#calc_task_load_actual)
        * [try_proc_introspec_cmd](#try_proc_introspec_cmd)
        * [calc_greedy_factor](#calc_greedy_factor)
        * [calc_slice_share](#calc_slice_share)
        * [calc_time_slice](#calc_time_slice)
        * [have_scheduled](#have_scheduled)
        * [calc_avg_freq](#calc_avg_freq)
        * [update_stat_for_running](#update_stat_for_running)
        * [adjust_slice_boost](#adjust_slice_boost)
        * [slice_fully_consumed](#slice_fully_consumed)
        * [update_stat_for_running](#update_stat_for_running)
        * [update_stat_for_stopping](#update_stat_for_stopping)
        * [cpu_ctx_init_online](#cpu_ctx_init_online)
        * [cpu_ctx_init_offline](#cpu_ctx_init_offline)
      * [Callbacks](#callbacks)
          * [lavd_select_cpu](#lavd_select_cpu)
          * [lavd_enqueue](#lavd_enqueue)
          * [lavd_dispatch](#lavd_dispatch)
          * [lavd_runnable](#lavd_runnable)
          * [lavd_running](#lavd_running)
          * [lavd_stopping](#lavd_stopping)
          * [lavd_quiescent](#lavd_quiescent)
          * [lavd_cpu_online](#lavd_cpu_online)
          * [lavd_cpu_offline](#lavd_cpu_offline)
          * [lavd_update_idle](#lavd_update_idle)
          * [lavd_init_task](#lavd_init_task)
          * [lavd_init](#lavd_init)
          * [lavd_exit](#lavd_exit)
  * [What are the fundamentals? (reason)](#what-are-the-fundamentals?-(reason))
  * [Why?](#why?)
  * [Why?](#why?)
  * [Why?](#why?)
  * [What level of understanding do I have?](#what-level-of-understanding-do-i-have?)
  * [Questions](#questions)
  * [Important Variables](#important-variables)

<!-- vim-markdown-toc -->

# Understanding the LAVD Policy 

## What is the context? (read)

  Understand the fundamental idea and implementation of this 
  policy, so that I can implement my own idea. 

## What is the summary of what I just read? (read)

  Latency-Criticality Aware Virtual Deadline Scheduler 

  Aimed at minimizing latency spikes.  
    While maintaining good throughput and fair use of CPU time 

  Basics 

    Runnable task -> 
      time slice 
      virtual deadline 
    
    Scheduler picks the closest virtual deadline 
      allows it to execute for given time slice 
  
  Determining Latency-Criticality 

    What is it? 
      If delaying scheduling of a task doesn't affect overall perforamce then it's 
      not latency critical. 
    
    One way is to, ask the developer to annotate the code with nice values. 
      (not possible)
  
    In a dependency graph of A -> B -> C  
      task B is latency critical, because it affects the latency of the 
      execution graph the most. 

  Virtual Deadline 

    more latency critical - tighter virtual deadline 
    so that the scheduler picks up such a task more frequently 

  Time Slice 
    
    targeted latency 
      a pre-configured window of time which is divided equally among the tasks 
      while taking nice level priority into consideration 
        lower nice value takes longer time slice 

    behavioral characteristics 
      if a task consumes all it's time slice - it's next time slice is boosted 
      a fresh task only get's half of the time slice 

  Fairness 

    over-scheduled tasks if they are not latency critical, 
      their virtual deadline 
        is deferred by some amount to help reduce their execution frequency 
      
      time slice is reduced 


### Summary of Functions and One-Liner description

#### Helper Funcs 

##### boost_lat
 
  * first calculate factors for runtime, wait and wakeup 
  * it is assumed that shorter running tasks, higher frequency waiting and waking is associated with 
    * latency critical tasks 
  * runtime_ft = max - task runtime - capped b/w 0 and max 
  * wait_freq_ft = just wait frequency capped to max 
  * wake_freq_ft = just wake frequency capped to max 
  * latency criticality = wake_freq^2 + wait_freq + runtime_ft ^ 3 
    * high frequency tasks and shorter running tasks are considered latency critical 
  * since it's an exponential distribution 
    * take log2 of it 
  * convert latency criticality to relative priority  
    * min lc to avg lc -- boost_rang/2 to 0 
    * min lc to avg lc -- boost_rang/2 to 0 
  * save the sum of static priority and lat boost priority 
  * return the boost priority 

##### calc_virtual_deadline_delta
  
  * get load factor from sys cpu util 
  * calc_latency_weight
    * boost_lat
    * return lat_priority to weight based off a table 
  * calculate vdeadline delta = weight * slice / 1000 
  * when the system is overload increase vdeadline delta 

##### calc_eligible_delta

  * calculate greedy ratio that is  
    * ratio of (taskc->load_actual / taskc->ideal) / ( sys->actual / sys->ideal )
    * representing how greedy task was as compared to it's ideal allocation 
  * check if it's eligible 
    * cap the latency priority b/w 0 and nicewidth 
    * eligible if the greedy ratio is less then threshold in the table 
    * if eligible set delta to zero and go out 
  * otherwise 
    * lat factor = inverted priority * 2
    * delta = 1 / run frequency  * greedy_ratio / lat_factor 
    * delta = run time * greedy ratio / lat_factor 
    * high latency priority (0) would get assigned shorter delta since lat_factor would be smaller 
    * lower latency priority would get assigned higher eligibility delta - thus can be scheduled later 
  * delta_ns 
  

##### find_victim_cpu

  * 

##### try_find_and_kick_victim_cpu

  * logic to find a victim cpu - it's a load balancing problem - 

##### put_global_rq

  * calculate when to run 
  * vdeadline = eligible_delta + vdeadline_delta + now 
  * try to find a cpu that is running a less urgent task and kick it 
  * dispatch to the global queue with vdeadline 

##### scx_bpf_dispatch_vtime

  * assign slice and vtime to the p->scx.* 
  * and dispatch to the queue 

##### update_stat_for_runnable

  * taskc 
    * update estimated load - runtime * frequency 
    * acc runtime = 0 
  * cpuc 
    * add load based on nice priority to load_ideal 
    * add load actual from taskc 
    * add capped runtime to load runtime 

##### calc_task_load_actual

  * one second / 100 ms -> 10  
  * runtime * frequency -> total runtime / 10 - to scale according to the interval 
  * return load ( time consumed by the task in 100 ms )


##### try_proc_introspec_cmd

  * no idea what it does! 

##### calc_greedy_factor
  
  * given task context 
  * cap the latency priority b/w 0 and max 
  * fetch the greedy_threshold ratio from a table using latency priority 
  * scale the greedy ratio from the task context using this threshold 
  * scale the scaled ratio by 3 
  * return the greedy ratio 

  * essentially it's the ratio based on nice priority 

##### calc_slice_share

  * get weight based on nice priority level  
  * increase the share based on slice boost updated during running stats capture 
  * return the updated weight share 

##### calc_time_slice

  * get the weight share for task p using it's context  
  * calculate slice time = 
    * weight * number of online cpus * targeted latency (15 ms) / load ideal 
  * get the greedy factor based on the nice priority 
  * scale the slice based on it 
    * greater factor get's shorter time slice 
  * cap the slice between min and max 
  * if the task was never scheduled - half the slice 
  * save in the context and return as well 



##### have_scheduled
  
  * if the slice_ns of a task is not zero 
    * then it has been scheduled by this scheduler 

##### calc_avg_freq

  * frequency = number of nanon seconds in one second / interval in nano seconds 
  * exp moving average run frequency = (old, new)

##### update_stat_for_running
  
  * if the task has been scheduled 
    * wait period = current clock - last time it was slep 
    * interval = runtime of the task + wait period 
    * run frequency of the task = 1 / interval 
  * update per cpu latency criticality information 
    * capture max, min and sum of all the tasks lat criticality 
  * increment count of tasks scheduled on this cpu 
  * capture current time for when the task started running 

##### adjust_slice_boost
  
  * if all consumed 
    * increase boost priority 
  * otherwise 
    * decrease boost priority 

##### slice_fully_consumed

  * compare stop - start time with assigned time slice 
    * if >= then all consumed 

  
##### update_stat_for_running



##### update_stat_for_stopping

  * calculate the exponential moving average of the task runtime 
    * using now - old time pattern 
  * add it to the accumalated run time 
  * reduce the cpu load runtime 

##### cpu_ctx_init_online

  * reset the context 
  * mark as online in context 

##### cpu_ctx_init_offline

  * reset the context 
  * mark as offline in context 

#### Callbacks 

```
SCX_OPS_DEFINE(lavd_ops,
	       .select_cpu		= (void *)lavd_select_cpu,
	       .enqueue			= (void *)lavd_enqueue,
	       .dispatch		= (void *)lavd_dispatch,
	       .runnable		= (void *)lavd_runnable,
	       .running			= (void *)lavd_running,
	       .stopping		= (void *)lavd_stopping,
	       .quiescent		= (void *)lavd_quiescent,
	       .cpu_online		= (void *)lavd_cpu_online,
	       .cpu_offline		= (void *)lavd_cpu_offline,
	       .update_idle		= (void *)lavd_update_idle,
	       .init_task		= (void *)lavd_init_task,
	       .init			= (void *)lavd_init,
	       .exit			= (void *)lavd_exit,
	       .flags			= SCX_OPS_KEEP_BUILTIN_IDLE,
	       .timeout_ms		= 30000U,
	       .name			= "lavd");
```

###### lavd_select_cpu


###### lavd_enqueue
  
  * put a task on global rq 


###### lavd_dispatch
  
  * get a task from the global queue 

###### lavd_runnable
  
  * callback when a task becomes runnable 
  * update stats for a runnable task 
  * get the current task which is assumed to be the waker task 
  * calculate the interval since last runnable 
  * calculate wake frequency and update in waker tasks context 


###### lavd_running
  
  * callback for when the task starts running 
  
  * update stats for running 
    * run_frequency 
    * cpu latency criticality 
    * start time 

  * update information for preemption 
    * latency priority 
    * estimated stopping time 
      * now + runtime of the task 
  
  * calculate the time slice of the task p using task context we have and put it in scx.slice 

###### lavd_stopping

  * callback for when the task stops running 

  * update stats for the task 
    * runtime
    * cpu load runtime 

###### lavd_quiescent

  * callback when task is going into sleep 
  * remove the task load from cpucontext 
  * based on the time since it last went to sleep 
    * calculate frequency, calculate it's moving average
    * that would be the new wait frequency (wait_freq)

###### lavd_cpu_online

  * callback for cpu becoming online  
  * trigger recalculation of cpu load after incrementing online cpu count 

###### lavd_cpu_offline

  * callback for cpu becoming offline 
  * trigger recalculation of cpu load after decrementing online cpu count 

###### lavd_update_idle
  
  * callback when entering or leaving the idle state 
  * get the cpu context 
  * add duration since going into the idle to the context if not already updated in timer callback 
  * accumlates the idle duration 

###### lavd_init_task

  * lookup task context, create if necessary  
    * initialize following clks to now  
      * runnable, running, stopping, quiescent  
    * greedy ratio to 1000 
    * time slice to 15 ms 
    * run freq to 1 
  
  * if the task is forked 
    * reflect changes to system utilization immediately 

###### lavd_init
  
  * create a central global queue 
  * initialize per-cpu context 
  * initialize system wide util tracking __sys_cpu_util using timer 
  * create a timer callback with interval of 100 ms for updating cpu utilization 

###### lavd_exit
  
  Capture exit code. 


## What are the fundamentals? (reason)
  
  The goal is to 
    decide which task to select next based on virtual deadline. 
    decide how much time slice to assign it. 
  
  Based on 
    Overscheduled charactersitics and underscheduled characteristics of the 
    task. 

## Why? 

## Why? 

## Why? 

## What level of understanding do I have?


## Questions 

  Why is 1000 multiplied everywhere? 

  What is load_factor? 
    sum of all the load runtimes on all the cpus 
      normalized with number of online cpus 

  How is over-scheduled task determined? 
  
  How is load-utilization determined? 

## Important Variables 
  
  * task 
    * slice_boost_prio
    * acc_run_time_ns
    * lat_cri
    * lat_prio
    * greedy_ratio
    
  * cpu 
    * load_run_time_ns
    * sum_lat_cri
    
  * cutil_cur->load_ideal
    * accumulated weight of all the tasks that ran on the cpuc 




