# multi_yielder

a easy-to-use Python multi-thread/process parallel helper

## support:

- multi thread, process or gevent
- easy to use
- easy to stop 

## how to use:

```
def xprint(x): 
    time.sleep(1)  # mock a long time task 
    yield x*x   
i=0
for item in multi_yield(xprint, process_mode,3,xrange(100)):
    i+=1
    print(item)
    if i>10:
        break
        
```