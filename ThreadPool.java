package il.co.ilrd.threadpoolilrd;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import il.co.ilrd.waitablepq.WaitablePQ;

public class ThreadPoolILRD implements Executor {
	private int nThread;
	private Semaphore semPause = new Semaphore(0);
	private Semaphore semWaitTermination = new Semaphore(0);
	private WaitablePQ<Task<?>> pq = new WaitablePQ<Task<?>>();
	private Map<Long, Thread> workers;
	private boolean isShutdown = false;
	private static final int MAX_PRIORITY = Priority.HIGH.getValue() + 1;
	private static final int MIN_PRIORITY = Priority.LOW.getValue() - 1;

	public ThreadPoolILRD(int nThreads)  {
		if(nThreads < 1) {
			throw new IllegalArgumentException("min thread in more then 0!");
		}
		this.nThread = nThreads;
		workers = new HashMap<Long, Thread>(nThreads);
		insertToMapAndRunThreads(nThreads);
	}

	private void insertToMapAndRunThreads(int nThreads) {
		for (int i = 0 ; i < nThreads ; ++i) {
			Thread thread = new WorkerThread();
			thread.start();
			workers.put(thread.getId(), thread);
		}
	}

	@Override
	public void execute(Runnable command) {
		try {
			submit(command, Priority.MEDIUM);
		} catch (RejectedExecutionException e) {
			e.printStackTrace();
		}
	}

	private <T> Future<T> submit(Callable<T> callable, int priority) throws RejectedExecutionException {
		if (isShutdown) {
			throw new RejectedExecutionException("Can't submit new tasks while pool is shutdown");
		}
		Task<T> task = new Task<>(callable, priority);
		pq.enqueue(task);
		return task.getFuture();
	}

	public <T> Future<T> submit(Callable<T> callable, Priority priority) throws RejectedExecutionException {
		return submit(callable, priority.getValue());
	}

	public <T> Future<T> submit(Callable<T> callable) throws Exception{
		return submit(callable, Priority.MEDIUM);
	}

	public <T> Future<T> submit(Runnable command, Priority priority, T result) throws RejectedExecutionException{
		return submit(Executors.callable(command, result), priority);
	}

	public Future<Void> submit(Runnable command, Priority priority) throws RejectedExecutionException{
		return submit(Executors.callable(command, null), priority);
	}

	public void pause() throws Exception {
		for (int i = 0; i < nThread; i++) {
			submit(new Callable<Void>() {

				@Override
				public Void call() throws InterruptedException {
					semPause.acquire();
					return null;
				}

			}, MAX_PRIORITY);			
		}		
	} 

	public void resume() {
		semPause.release(nThread);
	} 

	public void setNumberOfThreads(int nThreads) throws Exception {
		int threadToAdd = nThreads - this.nThread;
		this.nThread = nThreads; 
		if (threadToAdd >= 0) {
			insertToMapAndRunThreads(threadToAdd);
		} else {
			int threadToRemove = -threadToAdd;
			addBadAppleTask(threadToRemove, MAX_PRIORITY);			
		}
	}	
	
	public void shutDown() throws Exception {
		isShutdown = true;
		addBadAppleTask(nThread, MIN_PRIORITY);	
	}

	public boolean waitTermination() { 
		boolean result = true;
		try {
			semWaitTermination.acquire(nThread);
		} catch (InterruptedException e) {
			e.printStackTrace();
			result = false;
		}
		return result;
	}

	public boolean waitTermination(long timeout, TimeUnit unit) { 
		boolean result = true;
		try {
			if (!semWaitTermination.tryAcquire(nThread, timeout, unit)){
				result = false;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			result = false;
		}
		return result;
	}
	
	private void addBadAppleTask(int numOfThread, int priority) throws Exception {
		for (int i = 0; i < numOfThread; i++) {
			submit(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					WorkerThread curThread = (WorkerThread) workers.remove(Thread.currentThread().getId());
					curThread.isRunning = false;
					if (isShutdown) {
						semWaitTermination.release();                    
					}
					return null;
				}                
			}, priority);            
		}
	}

	public enum Priority {
		LOW(1), 
		MEDIUM(2), 
		HIGH(3);

		private final int value;

		private Priority(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}
	}
	
	private class Task<T> implements Comparable<Task<T>>{
		private Callable<T> callable;
		private int priority;
		private T result;
		private boolean isDone = false;
		private boolean isCancel = false;
		private Semaphore sem = new Semaphore(0);
		private FutureTask futureTask;
		private Exception runException = null;

		private Task(Callable<T> callable, int priority) {
			this.callable = callable;
			this.priority = priority;	
			futureTask = new FutureTask();
		}

		@Override
		public int compareTo(Task<T> arg0) {
			return (priority - arg0.priority);
		}

		private FutureTask getFuture() {
			return futureTask;			
		}

		private void run()  {	
			try {
				result = callable.call();
			} catch (Exception e) {
				runException = e;
			}finally {					
				isDone = true;
				sem.release();
			}
		}

		private class FutureTask implements Future<T>{

			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				isCancel = pq.remove(Task.this);
				return isCancel;
			}

			@Override
			public T get() throws InterruptedException, ExecutionException {
				if (isCancelled()) {
					throw new CancellationException();
				}
				if(!isDone()) {
					sem.acquire();
				}
				if (runException != null) {
					throw new ExecutionException(runException);
				}
				return result;
			}
			//				return get(10000000, TimeUnit.DAYS);


			@Override
			public T get(long timeout, TimeUnit unit) 
					throws InterruptedException, ExecutionException, TimeoutException {
				if (isCancelled()) {
					throw new CancellationException();
				}
				if(isDone() || sem.tryAcquire(timeout, unit)) {
					return result;
				}else {
					throw new TimeoutException();
				}
			}

			@Override
			public boolean isCancelled() {
				return isCancel;
			}

			@Override
			public boolean isDone() {
				return isDone || isCancelled();
			}
		}
	}

	private class WorkerThread extends Thread{
		private boolean isRunning = true;

		@Override
		public void run() {
			while(isRunning) { 
				try {
					Task<?> task = pq.dequeue();
					task.run();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}

