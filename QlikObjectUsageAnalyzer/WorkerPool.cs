using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QlikObjectUsageAnalyzer
{
    public class WorkerPool<T>
    {
        private readonly int _workerCount;
        private int _freeWorkers;

        private readonly List<Task> _workers = new List<Task>();
        private readonly Queue<Func<Task<T>>> _queue = new Queue<Func<Task<T>>>();
        private readonly BlockingCollection<Task<T>> _completedTasks = new BlockingCollection<Task<T>>();

        public WorkerPool(int workerCount)
        {
            _workerCount = workerCount;
            _freeWorkers = workerCount;
        }

        public void AddWork(Func<Task<T>> job)
        {
            lock (_workers)
            {
                if (_freeWorkers == 0)
                {
                    _queue.Enqueue(job);
                    return;
                }

                lock (_workers)
                {
                    _workers.Add(job().ContinueWith(OnTaskCompleted));
                    _freeWorkers--;
                }
            }
        }

        private void OnTaskCompleted(Task<T> t)
        {
            lock (_workers)
            {
                _workers.Remove(t);
                _freeWorkers++;
                if (_queue.Any())
                {
                    var job = _queue.Dequeue();
                    AddWork(job);
                }
            }
            _completedTasks.Add(t);
        }

        public bool IsFull => _freeWorkers == 0;
        public bool HasWork => _freeWorkers != _workerCount || _completedTasks.Any();
        
        public Task<T> GetResult()
        {
            if (!HasWork) throw new Exception("No tasks running.");

            return _completedTasks.Take();
        }
    }
}