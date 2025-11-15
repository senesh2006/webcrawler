"""
Pipeline Stage - Abstract base class for all pipeline stages.

This module defines the PipelineStage abstract base class that all concrete
stages must inherit from. It provides common functionality for:
- Multi-threaded worker management
- Input/output queue handling
- Error handling and retry logic
- Statistics tracking
- Lifecycle management (start/stop/pause/resume)
"""

import threading
import logging
import time
from typing import Optional, Any
from queue import Queue, Empty
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime


class PipelineStage(ABC):
    """
    Abstract base class for all pipeline stages.
    
    Each stage:
    - Reads data from input_queue
    - Processes it (implemented by subclass)
    - Writes result to output_queue
    - Runs multiple worker threads for concurrency
    
    Lifecycle:
    ---------
    1. Create stage instance
    2. Call start() to begin processing
    3. Stage processes items from input queue
    4. Call stop() to gracefully shut down
    """
    
    def __init__(self, name: str, input_queue: Queue, output_queue: Optional[Queue],
                 num_workers: int = 1):
        """
        Initialize pipeline stage.
        
        Args:
            name: Stage name (for logging/monitoring)
            input_queue: Queue to read input data from
            output_queue: Queue to write output data to (None for final stage)
            num_workers: Number of worker threads to spawn
        """
        self.name = name
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.num_workers = num_workers
        
        # Worker threads
        self.workers = []
        self.is_running = False
        self.is_paused = False
        
        # Statistics
        self.processed_count = 0
        self.error_count = 0
        self.start_time = None
        self.total_processing_time = 0.0
        
        # Thread safety
        self.stats_lock = threading.Lock()
        self.control_lock = threading.Lock()
        
        # Logging
        self.logger = logging.getLogger(f"{self.__class__.__name__}:{self.name}")
        
        self.logger.info(f"Initialized stage '{self.name}' with {num_workers} workers")
    
    @abstractmethod
    def process(self, data: Any) -> Optional[Any]:
        """
        Process a single data item.
        
        This method MUST be implemented by all concrete stages.
        
        Args:
            data: Input data item (typically PipelineData)
            
        Returns:
            Processed data to pass to next stage, or None to drop item
            
        Raises:
            Exception: Any exception will be caught and logged
        """
        pass
    
    def start(self):
        """Start the stage by spawning worker threads."""
        with self.control_lock:
            if self.is_running:
                self.logger.warning(f"Stage '{self.name}' already running")
                return
            
            self.is_running = True
            self.start_time = time.time()
            
            # Spawn worker threads
            for i in range(self.num_workers):
                worker = threading.Thread(
                    target=self._worker_loop,
                    name=f"{self.name}-Worker-{i+1}",
                    daemon=True
                )
                worker.start()
                self.workers.append(worker)
            
            self.logger.info(f"Started stage '{self.name}' with {self.num_workers} workers")
    
    def stop(self, timeout: float = 30.0):
        """
        Stop the stage gracefully.
        
        Args:
            timeout: Maximum seconds to wait for workers to finish
        """
        with self.control_lock:
            if not self.is_running:
                self.logger.warning(f"Stage '{self.name}' not running")
                return
            
            self.logger.info(f"Stopping stage '{self.name}'...")
            self.is_running = False
        
        # Wait for workers to finish
        start_wait = time.time()
        for worker in self.workers:
            remaining_time = timeout - (time.time() - start_wait)
            if remaining_time > 0:
                worker.join(timeout=remaining_time)
            
            if worker.is_alive():
                self.logger.warning(f"Worker {worker.name} did not stop in time")
        
        self.workers.clear()
        self.logger.info(f"Stopped stage '{self.name}'")
    
    def pause(self):
        """Pause processing (workers will wait)."""
        with self.control_lock:
            if not self.is_running:
                self.logger.warning(f"Cannot pause - stage '{self.name}' not running")
                return
            
            self.is_paused = True
            self.logger.info(f"Paused stage '{self.name}'")
    
    def resume(self):
        """Resume processing after pause."""
        with self.control_lock:
            if not self.is_running:
                self.logger.warning(f"Cannot resume - stage '{self.name}' not running")
                return
            
            self.is_paused = False
            self.logger.info(f"Resumed stage '{self.name}'")
    
    def _worker_loop(self):
        """
        Main worker loop - runs in each worker thread.
        Continuously reads from input queue, processes, and writes to output queue.
        """
        worker_name = threading.current_thread().name
        self.logger.debug(f"{worker_name} started")
        
        while self.is_running:
            try:
                # Wait while paused
                while self.is_paused and self.is_running:
                    time.sleep(0.1)
                
                if not self.is_running:
                    break
                
                # Get item from input queue (with timeout)
                try:
                    data = self.input_queue.get(timeout=1.0)
                except Empty:
                    continue
                
                # Process item
                try:
                    start_time = time.time()
                    result = self.process(data)
                    processing_time = time.time() - start_time
                    
                    # Update statistics
                    with self.stats_lock:
                        self.processed_count += 1
                        self.total_processing_time += processing_time
                    
                    # Pass result to next stage (if not None and output queue exists)
                    if result is not None and self.output_queue is not None:
                        self.output_queue.put(result)
                    
                    self.logger.debug(
                        f"{worker_name} processed item in {processing_time:.3f}s"
                    )
                
                except Exception as e:
                    # Log error but continue processing
                    with self.stats_lock:
                        self.error_count += 1
                    
                    self.logger.error(
                        f"{worker_name} error processing item: {e}",
                        exc_info=True
                    )
                
                finally:
                    # Mark task as done
                    self.input_queue.task_done()
            
            except Exception as e:
                self.logger.error(f"{worker_name} unexpected error: {e}", exc_info=True)
        
        self.logger.debug(f"{worker_name} stopped")
    
    def get_stats(self) -> dict:
        """
        Get stage statistics.
        
        Returns:
            dict with statistics
        """
        with self.stats_lock:
            runtime = time.time() - self.start_time if self.start_time else 0
            
            stats = {
                'name': self.name,
                'is_running': self.is_running,
                'is_paused': self.is_paused,
                'workers': self.num_workers,
                'processed': self.processed_count,
                'errors': self.error_count,
                'runtime_seconds': round(runtime, 2),
                'input_queue_size': self.input_queue.qsize(),
                'output_queue_size': self.output_queue.qsize() if self.output_queue else 0,
            }
            
            # Calculate throughput
            if runtime > 0:
                stats['throughput_items_per_second'] = round(
                    self.processed_count / runtime, 2
                )
            else:
                stats['throughput_items_per_second'] = 0.0
            
            # Calculate average processing time
            if self.processed_count > 0:
                stats['avg_processing_time_seconds'] = round(
                    self.total_processing_time / self.processed_count, 3
                )
            else:
                stats['avg_processing_time_seconds'] = 0.0
            
            return stats
    
    def reset_stats(self):
        """Reset statistics counters."""
        with self.stats_lock:
            self.processed_count = 0
            self.error_count = 0
            self.total_processing_time = 0.0
            self.start_time = time.time() if self.is_running else None
        
        self.logger.info(f"Reset statistics for stage '{self.name}'")
    
    def __repr__(self) -> str:
        return (f"<{self.__class__.__name__} name='{self.name}' "
                f"workers={self.num_workers} running={self.is_running}>")


# Example concrete stage implementation for testing
class ExampleStage(PipelineStage):
    """
    Example concrete stage implementation.
    Demonstrates how to create a custom stage.
    """
    
    def process(self, data: Any) -> Optional[Any]:
        """
        Example processing: convert string to uppercase.
        
        Args:
            data: Input data (string)
            
        Returns:
            Processed data (uppercase string)
        """
        if isinstance(data, str):
            return data.upper()
        return data


# Testing
if __name__ == "__main__":
    import logging
    import time
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Testing PipelineStage Base Class")
    print("=" * 60)
    
    # Create queues
    input_q = Queue()
    output_q = Queue()
    
    # Create example stage
    stage = ExampleStage("TestStage", input_q, output_q, num_workers=2)
    
    # Start stage
    print("\n1. Starting stage...")
    stage.start()
    print(f"   Stage started: {stage.is_running}")
    
    # Add test data
    print("\n2. Adding test data...")
    test_items = ["hello", "world", "pipeline", "stage"]
    for item in test_items:
        input_q.put(item)
        print(f"   Added: {item}")
    
    # Wait for processing
    print("\n3. Processing...")
    time.sleep(2)
    
    # Get results
    print("\n4. Results:")
    results = []
    while not output_q.empty():
        result = output_q.get()
        results.append(result)
        print(f"   Output: {result}")
    
    # Get statistics
    print("\n5. Statistics:")
    stats = stage.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    # Test pause/resume
    print("\n6. Testing pause/resume...")
    stage.pause()
    print(f"   Paused: {stage.is_paused}")
    time.sleep(1)
    stage.resume()
    print(f"   Resumed: {stage.is_paused}")
    
    # Stop stage
    print("\n7. Stopping stage...")
    stage.stop()
    print(f"   Stage stopped: {not stage.is_running}")
    
    print("\n" + "=" * 60)
    print("Test completed!")
    print(f"Processed {len(results)}/{len(test_items)} items successfully")