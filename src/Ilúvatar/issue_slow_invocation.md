# Investigation: Slow invocation after Q switch    

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
  
  * for the first invocation - it's pretty fast 
  * after Q switch 
    * the invocation becomes really slow 
  * enqueue is called regularly 
  * select_cpu is called very rarely

### Questions 0 
  
  * Why is select_cpu not called and enqueue is called? 


### Observation 1 

  * enque flags 
```
>>> bin(0x20000000000)
'0b 0010 00000000 00000000 00000000 00000000 00000000'
    43   39       31       23       15       7

    in this case bit 41 is set 
```
    * the set bit says that this task was the only task available on the local Q 
      * due to the last enqueue flag this enqueue has been triggered again 
      * if you dispatch to local dsq again 
        * it will keep on executing 
      * otherwise 
        * you are responsible to trigger the scheduling event again 

### Questions 1 
### Observation 2 
### Questions 2 
## What is the root cause of the problem? 


  * after the enqueue the scheduling cycle was not being triggered 

## How to fix it? 

  * kick the target cpu! 


