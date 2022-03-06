from multiprocessing import Process
from multiprocessing import BoundedSemaphore, Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array

NPROD = 4
NUMS = 25
SIZE = 5

def add_data(buffer, index, data, mutex):
    mutex.acquire()
    buffer[index.value] = data
    index.value = (index.value + 1) % SIZE
    mutex.release()
    
def get_data(buffer, index, mutex, remove=False):
    mutex.acquire()
    data = buffer[index.value]
    if (remove):
        index.value = (index.value + 1) % SIZE
    mutex.release()
    return data

def get_minimum(buffers, indices, mutexes):
    min_index = 0
    data = get_data(buffers[0], indices[0], mutexes[0])
    while min_index < NPROD and data == -1:
        min_index = min_index + 1
        data = get_data(buffers[min_index],
                        indices[min_index],
                        mutexes[min_index])
    for i in range(min_index + 1, NPROD):
        candidate = get_data(buffers[i], indices[i], mutexes[i])
        if candidate != -1 and candidate < data:
            data = candidate
            min_index = i
    return min_index
        

def produce(tid, buffer, index, mutex, empty, full):
    num = tid
    for i in range(NUMS):
        empty.acquire()
        add_data(buffer, index, num, mutex)
        num += NPROD
        full.release()
    num = -1
    empty.acquire()
    add_data(buffer, index, num, mutex)
    full.release()
        
def consume(buffers, read, mutexes, emptys, fulls):
    result = []
    
    while len(result) < NPROD*NUMS:
            
        for f in fulls:
            f.acquire()
    
        min_index = get_minimum(buffers, read, mutexes)
        if min_index == NPROD:
            return
        
        result.append(get_data(buffers[min_index],
                               read[min_index],
                               mutexes[min_index],
                               remove=True))
        
        for f in range(NPROD):
            if f != min_index:
                fulls[f].release()
            else:
                emptys[f].release()
        
        printArray(result)

def main():
    buffers = [Array('i', SIZE) for p in range(NPROD)]
    mutexes = [Lock() for p in range(NPROD)]
    write = [Value('i', 0) for p in range(NPROD)]
    read = [Value('i', 0) for p in range(NPROD)]
    emptys = [BoundedSemaphore(SIZE) for p in range(NPROD)]
    fulls = [Semaphore(0) for p in range(NPROD)]
    
    producers = [Process(target=produce,
                         name=f'prod_{i}',
                         args=(i, buffers[i], write[i], mutexes[i], emptys[i], fulls[i]))
        for i in range(NPROD)]
    
    consumer = Process(target=consume,
                       name='merge',
                       args=(buffers, read, mutexes, emptys, fulls))
    
    consumer.start()
    for pr in producers:
        pr.start()
    
    consumer.join()
    for pr in producers:
        pr.join()
        
def printArray(array, extra=""):
    print(extra)
    print([f'{elem},' for elem in array])

if __name__ == '__main__':
    main()