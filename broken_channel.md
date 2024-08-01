# Investigating Broken Channel after code changes 

```
            ┌─────────────────────────────┐
            │                             ▼
   Capture Observations              Ask Questions
            ▲                             │
            └─────────────────────────────┘
                Until Root Cause is Found
```

## Process 
### Observation 0 

  Adding prints to pid sender  
    there are way too many pids being sent, even those that don't make sense. 

  Even when sending characteristics they are not being printed out by the scheduler. 

  Scheduler is not even printing the user stats. 
    Removing the stdio pipes and getting output on the console
    shows that the user stats are being printed and characteristics are being 
    sent 

### Questions 0 

  Why doesn't this stuff print out to files?   ✓

  Why are so many pids being sent? 
    because pid 0 was being stored and lookedup 

  Why are pids which don't make sense being sent? 
    because the pid 0 was being stored and lookedup

### Conclusion 0 
  
  * need to fix writing to file issue  ✓
  * investigate futher questions 

### Observation 1 

  Now the scheduler writes to the file.
  It was waiting on the stderr.read() forever. 

### Questions 1 
### Conclusion 1 


### Observation 2 
### Questions 2 

## What is the root cause of the problem? 
  
  careless coding!!! 

## How to fix it? 

  Carefully write every bit of code!!! 



