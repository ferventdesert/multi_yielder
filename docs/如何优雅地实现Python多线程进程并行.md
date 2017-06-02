# 如何优雅地实现Python通用多线程/进程并行模块

标签（空格分隔）： 技术文档

---

当单线程性能不足时，我们通常会使用多线程/多进程去加速运行。而这些代码往往多得令人绝望，需要考虑：

- 如何创建线程执行的函数？
- 如何收集结果？若希望结果从子线程返回主线程，则还要使用队列
- 如何取消执行？ 直接kill掉所有线程？信号如何传递？
- 是否需要线程池？ 否则反复创建线程的成本过高了

不仅如此，若改为多进程或协程，代码还要继续修改。若多处使用并行，则这些代码还会重复很多遍，非常痛苦。

于是，我们考虑将并行的所有逻辑封装到一个模块之内，向外部提供像串行执行一样的编程体验，还能彻底解决上面所述的疑难问题。所有代码不足180行。

GitHub地址： 

> https://github.com/ferventdesert/multi_yielder

使用时非常简洁：
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
上面的代码会使用三个进程，并行地打印1-10。当打印完10之后，进程自动回收释放。就像串行程序一样简单。


## 1. 先实现串行任务

我们通常会将任务分割为很多个子块，从而方便并行。因此可以将任务抽象为生成器。类似下面的操作，每个seed都是任务的种子。

```
def get_generator():
    for seed in 100:
        yield seed
```

任务本身的定义，则可以通过一个接受种子的函数来实现：
```
def worker(seed):
    # some long time task
    return seed*seed # just example
```

那么实现串行任务就像这样：
```
for seed in get_generator(n):
    print worker(seed)
```
进一步地，可以将其抽象为下面的函数：
```
def serial_yield(genenator,worker):
    for seed in generator():
        yield worker(seed)
```
该函数通过传入生成器函数（generator）和任务的定义(worker函数)，即可再返回一个生成器。消费时：
```
for result in serial_yield(your_genenator, your_worker):
    print(result)
```

我们看到，通过定义高阶函数，serial_yield就像map函数，对seed进行加工后输出。

## 2. 定义并行任务

考虑如下场景： boss负责分发任务到任务队列，多个worker从任务队列捞数据，处理完之后，再写入结果队列。主线程从结果队列中取结果即可。

我们定义如下几种执行模式：

- async: 异步/多协程
- thread: 多线程
- process: 多进程

使用Python创建worker的代码如下,func是任务的定义（是个函数）
```
    def factory(func, args=None, name='task'):
        if args is None:
            args = ()
        if mode == process_mode:
            return multiprocessing.Process(name=name, target=func, args=args)
        if mode == thread_mode:
            import threading
            t = threading.Thread(name=name, target=func, args=args)
            t.daemon = True
            return t
        if mode == async_mode:
            import gevent
            return gevent.spawn(func, *args)
```
创建队列的代码如下，注意seeds可能是无穷流，因此需要限定队列的长度，当入队列发现队列已满时，则任务需要阻塞。
```
  def queue_factory(size):
        if mode == process_mode:
            return multiprocessing.Queue(size)
        elif mode == thread_mode:
            return Queue(size)
        elif mode == async_mode:
            from gevent import queue
            return queue.Queue(size)
```

什么时候任务可以终止？ 我们罗列如下几种情况：

- 所有的seed都已经被消费完了
- 外部传入了结束请求

对第一种情况，我们让boss在seed消费完之后，在队列里放入多个Empty标志，worker收到Empty之后，就会自动退出，下面是boss的实现逻辑：

```
    def _boss(task_generator, task_queue, worker_count):
        for task in task_generator:
            task_queue.put(task)
        for i in range(worker_count):
            task_queue.put(Empty)
        print('worker boss finished')
```

再定义worker的逻辑：
```
    def _worker(task_queue, result_queue, gene_func):
        import time
        try:
            while not stop_wrapper.is_stop():
                if task_queue.empty():
                    time.sleep(0.01)
                    continue
                task = task.get()
                if task == Empty:
                    result_queue.put(Empty)
                    break
                if task == Stop:
                    break
                for item in gene_func(task):
                    result_queue.put(item)
            print ('worker worker is stop')
        except Exception as e:
            logging.exception(e)
            print ('worker exception, quit')
```
简单吧？但是这样会有问题，这个后面再说，我们把剩余的代码写完。

再定义multi_yield的主要代码。 代码非常好理解，创建任务和结果队列，再创建boss和worker线程(或进程/协程)并启动，之后不停地从结果队列里取数据就可以了。

```
 def multi_yield(customer_func, mode=thread_mode, worker_count=1, generator=None, queue_size=10):
        workers = []
        result_queue = queue_factory(queue_size)
        task_queue = queue_factory(queue_size)

        main = factory(_boss, args=(generator, task_queue, worker_count), name='_boss')
        for process_id in range(0, worker_count):
            name = 'worker_%s' % (process_id)
            p = factory(_worker, args=(task_queue, result_queue, customer_func), name=name)
            workers.append(p)
        main.start()

        for r in workers:
            r.start()
        count = 0
        while not should_stop():
            data = result_queue.get()
            if data is Empty:
                count += 1
                if count == worker_count:
                    break
                continue
            if data is Stop:
                break
            else:
                yield data
```

这样从外部消费时，即可：
```
def xprint(x):
    time.sleep(1)
    yield x

i=0
for item in multi_yield(xprint, process_mode,3,xrange(100)):
    i+=1
    print(item)
    if i>10:
        break
```
这样我们就实现了一个与`serial_yield`功能类似的`multi_yield`。可以定义多个worker，从队列中领任务，而不需重复地创建和销毁，更不需要线程池。当然，代码不完全，运行时可能出问题。但以上代码已经说明了核心的功能。完整的代码可以在文末找到。

但是你也会发现很严重的问题：

- 当从外部break时，内部的线程并不会自动停止
- 我们无法判断队列的长度，若队列满，那么put操作会永远卡死在那里，任务都不会结束。

## 3. 改进任务停止逻辑

最开始想到的，是通过在`multi_yield`函数参数中添加一个返回bool的函数，这样当外部break时，同时将该函数的返回值置为True，内部检测到该标志位后强制退出。伪代码如下:

```
_stop=False
def can_stop():
    return _stop

for item in multi_yield(xprint, process_mode,3,xrange(100),can_stop):
    i+=1
    print(item)
    if i>10:
        _stop=True
        break

```
但这样并不优雅，引入了更多的函数作为参数，还必须手工控制变量值，非常繁琐。在多进程模式下，stop标志位还如何解决？

我们希望外部在循环时执行了break后，会自动通知内部的生成器。实现方法似乎就是with语句，即contextmanager.

我们实现以下的包装类：

```
class Yielder(object):
    def __init__(self, dispose):
        self.dispose = dispose

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()
```
它实现了with的原语，参数是dispose函数，作用是退出with代码块后的回收逻辑。

由于值类型的标志位无法在多进程环境中传递，我们再创建StopWrapper类，用于管理停止标志和回收资源:

```
   class Stop_Wrapper():
        def __init__(self):
            self.stop_flag = False
            self.workers=[]

        def is_stop(self):
            return self.stop_flag

        def stop(self):
            self.stop_flag = True
            for process in self.workers:
                if isinstance(process,multiprocessing.Process):
                    process.terminate()
                    
```

最后的问题是，如何解决队列满或空时，put/get的无限等待问题呢？考虑包装一下put/get：包装在`while True`之中，每隔两秒get/put，这样即使阻塞时，也能保证可以检查退出标志位。所有线程在主线程结束后，最迟也能在2s内自动退出。

```
def safe_queue_get(queue, is_stop_func=None, timeout=2):
    while True:
        if is_stop_func is not None and is_stop_func():
            return Stop
        try:
            data = queue.get(timeout=timeout)
            return data
        except:
            continue


def safe_queue_put(queue, item, is_stop_func=None, timeout=2):
    while True:
        if is_stop_func is not None and is_stop_func():
            return Stop
        try:
            queue.put(item, timeout=timeout)
            return item
        except:
            continue
            
```

如何使用呢？我们只需在multi_yield的yield语句之外加上一行就可以了：

```
    with Yielder(stop_wrapper.stop):
        # create queue,boss,worker, then start all
        # ignore repeat code
        while not should_stop():
            data = safe_queue_get(result_queue, should_stop)
            if data is Empty:
                count += 1
                if count == worker_count:
                    break
                continue
            if data is Stop:
                break
            else:
                yield data
```

**仔细阅读上面的代码**， 外部循环时退出循环，则会自动触发stop_wrapper的stop操作，回收全部资源，而不需通过外部的标志位传递！这样调用方在心智完全不需有额外的负担。

实现生成器和上下文管理器的编程语言，都可以通过上述方式实现自动协程资源回收。笔者也实现了一个C#版本的，有兴趣欢迎交流。

这样，我们就能像文章开头那样，实现并行的迭代器操作了。


## 4. 结语

完整代码在：

一些实现的细节很有趣，我们借助在函数中定义函数，可以不用复杂的类去承担职责，而仅仅只需函数。而类似的思想，在函数式编程中非常常见。

该工具已经被笔者的流式语言etlpy所集成。但是依然有较多改进的空间，欢迎留言交流。












