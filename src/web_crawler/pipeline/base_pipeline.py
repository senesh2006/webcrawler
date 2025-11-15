"""
Pipeline Manager - Core pipeline infrastructure and orchestration
File: src/web_crawler/pipeline/base_pipeline.py
"""
from typing import List, Dict, Any, Optional
from queue import Queue
import logging
import time
from threading import Thread, Event
from .stage import PipelineStage
from .pipeline_data import PipelineData


class PipelineManager:
    """
    Manages the entire crawling pipeline.
    Coordinates all stages, handles startup/shutdown, and monitors health.
    """
    
    def __init__(self, queue_size: int = 1000):
        """
        Initialize pipeline manager
        
        Args:
            queue_size: Maximum size for inter-stage queues
        """
        self.queue_size = queue_size
        self.stages: List[PipelineStage] = []
        self.queues: List[Queue] = []
        self.is_running = False
        self.start_time: Optional[float] = None
        
        # Monitoring
        self.monitor_thread: Optional[Thread] = None
        self.monitor_stop_event = Event()
        self.monitor_interval = 5.0  # seconds
        
        # Logging
        self.logger = logging.getLogger("pipeline.manager")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def add_stage(self, stage: PipelineStage) -> None:
        """
        Add a stage to the pipeline
        
        Args:
            stage: Pipeline stage to add
        """
        self.stages.append(stage)
        self.logger.info(f"Added stage: {stage.name}")
    
    def build_pipeline(self, stages: List[PipelineStage]) -> None:
        """
        Build complete pipeline from list of stages
        
        Args:
            stages: List of pipeline stages in order
        """
        self.stages = stages
        self.logger.info(f"Pipeline built with {len(stages)} stages")
    
    def inject_seed_urls(self, urls: List[str], depth: int = 0) -> None:
        """
        Inject seed URLs into the first stage
        
        Args:
            urls: List of starting URLs
            depth: Starting depth (default: 0)
        """
        if not self.stages:
            raise RuntimeError("No stages in pipeline")
        
        first_queue = self.stages[0].input_queue
        
        for url in urls:
            data = PipelineData(url=url, depth=depth)
            first_queue.put(data)
            self.logger.info(f"Injected seed URL: {url}")
        
        self.logger.info(f"Injected {len(urls)} seed URLs")
    
    def start(self, seed_urls: Optional[List[str]] = None) -> None:
        """
        Start all pipeline stages
        
        Args:
            seed_urls: Optional list of seed URLs to inject
        """
        if self.is_running:
            self.logger.warning("Pipeline already running")
            return
        
        self.logger.info("Starting pipeline...")
        self.start_time = time.time()
        
        # Start all stages
        for stage in self.stages:
            stage.start()
        
        # Inject seed URLs if provided
        if seed_urls:
            self.inject_seed_urls(seed_urls)
        
        # Start monitoring
        self._start_monitoring()
        
        self.is_running = True
        self.logger.info("Pipeline started successfully")
    
    def stop(self, timeout: float = 30.0) -> None:
        """
        Stop all pipeline stages gracefully
        
        Args:
            timeout: Maximum time to wait for stages to finish
        """
        if not self.is_running:
            self.logger.warning("Pipeline not running")
            return
        
        self.logger.info("Stopping pipeline...")
        
        # Stop monitoring first
        self._stop_monitoring()
        
        # Stop stages in reverse order
        for stage in reversed(self.stages):
            stage.stop(timeout=timeout)
        
        self.is_running = False
        runtime = time.time() - self.start_time if self.start_time else 0
        self.logger.info(f"Pipeline stopped (runtime: {runtime:.2f}s)")
    
    def pause(self) -> None:
        """Pause all stages"""
        self.logger.info("Pausing pipeline...")
        for stage in self.stages:
            stage.pause()
    
    def resume(self) -> None:
        """Resume all stages"""
        self.logger.info("Resuming pipeline...")
        for stage in self.stages:
            stage.resume()
    
    def wait_until_done(self, check_interval: float = 1.0, max_idle_time: float = 10.0) -> None:
        """
        Wait until pipeline finishes processing all items
        
        Args:
            check_interval: How often to check queue status (seconds)
            max_idle_time: Consider done if idle for this long (seconds)
        """
        self.logger.info("Waiting for pipeline to complete...")
        last_activity_time = time.time()
        last_total_processed = 0
        
        while self.is_running:
            time.sleep(check_interval)
            
            # Check if all queues are empty and no processing happening
            all_empty = all(q.empty() for q in self.queues if q)
            all_idle = all(not stage.is_running() for stage in self.stages)
            
            # Check if processing is happening
            current_total = sum(stage.processed_count for stage in self.stages)
            if current_total > last_total_processed:
                last_activity_time = time.time()
                last_total_processed = current_total
            
            # Check idle timeout
            idle_time = time.time() - last_activity_time
            
            if all_empty and (all_idle or idle_time > max_idle_time):
                self.logger.info("Pipeline completed - all queues empty and idle")
                break
        
        self.stop()
    
    def get_overall_stats(self) -> Dict[str, Any]:
        """Get aggregated statistics from all stages"""
        total_processed = sum(s.processed_count for s in self.stages)
        total_errors = sum(s.error_count for s in self.stages)
        total_dropped = sum(s.dropped_count for s in self.stages)
        runtime = time.time() - self.start_time if self.start_time else 0
        
        return {
            'pipeline': {
                'running': self.is_running,
                'runtime_seconds': runtime,
                'stages': len(self.stages)
            },
            'totals': {
                'processed': total_processed,
                'errors': total_errors,
                'dropped': total_dropped,
                'success_rate': (total_processed - total_errors) / total_processed if total_processed > 0 else 0
            },
            'performance': {
                'overall_throughput': total_processed / runtime if runtime > 0 else 0,
            },
            'queues': [
                {'index': i, 'size': q.qsize()} 
                for i, q in enumerate(self.queues) if q
            ]
        }
    
    def get_stage_stats(self) -> List[Dict[str, Any]]:
        """Get detailed statistics for each stage"""
        return [stage.get_stats() for stage in self.stages]
    
    def get_bottleneck_analysis(self) -> Dict[str, Any]:
        """Analyze pipeline to identify bottlenecks"""
        stage_stats = self.get_stage_stats()
        
        # Find slowest stage by average processing time
        slowest_stage = max(
            stage_stats, 
            key=lambda s: s['performance']['avg_processing_time']
        ) if stage_stats else None
        
        # Find stages with large queues (potential bottleneck)
        congested_stages = [
            s for s in stage_stats 
            if s['queue_size'] > self.queue_size * 0.8
        ]
        
        # Find stages with high error rates
        error_prone_stages = [
            s for s in stage_stats
            if s['processed'] > 0 and s['errors'] / s['processed'] > 0.1
        ]
        
        return {
            'slowest_stage': slowest_stage,
            'congested_stages': congested_stages,
            'error_prone_stages': error_prone_stages
        }
    
    def _start_monitoring(self) -> None:
        """Start background monitoring thread"""
        if self.monitor_thread and self.monitor_thread.is_alive():
            return
        
        self.monitor_stop_event.clear()
        self.monitor_thread = Thread(
            target=self._monitor_loop,
            name="pipeline-monitor",
            daemon=True
        )
        self.monitor_thread.start()
        self.logger.info("Monitoring started")
    
    def _stop_monitoring(self) -> None:
        """Stop monitoring thread"""
        if not self.monitor_thread:
            return
        
        self.monitor_stop_event.set()
        self.monitor_thread.join(timeout=5.0)
        self.logger.info("Monitoring stopped")
    
    def _monitor_loop(self) -> None:
        """Background monitoring loop"""
        while not self.monitor_stop_event.is_set():
            try:
                stats = self.get_overall_stats()
                stage_stats = self.get_stage_stats()
                
                # Log summary
                self.logger.info(
                    f"Pipeline Status - "
                    f"Processed: {stats['totals']['processed']}, "
                    f"Errors: {stats['totals']['errors']}, "
                    f"Throughput: {stats['performance']['overall_throughput']:.2f}/s"
                )
                
                # Log per-stage stats
                for s in stage_stats:
                    self.logger.debug(
                        f"  {s['name']}: {s['processed']} processed, "
                        f"queue: {s['queue_size']}, "
                        f"throughput: {s['performance']['throughput_per_sec']:.2f}/s"
                    )
                
                # Check for issues
                bottlenecks = self.get_bottleneck_analysis()
                if bottlenecks['congested_stages']:
                    self.logger.warning(
                        f"Congested stages detected: "
                        f"{[s['name'] for s in bottlenecks['congested_stages']]}"
                    )
                
            except Exception as e:
                self.logger.error(f"Error in monitoring: {str(e)}")
            
            # Wait for next check
            self.monitor_stop_event.wait(self.monitor_interval)
    
    def print_summary(self) -> None:
        """Print a formatted summary of pipeline statistics"""
        stats = self.get_overall_stats()
        stage_stats = self.get_stage_stats()
        
        print("\n" + "="*60)
        print("PIPELINE SUMMARY")
        print("="*60)
        print(f"Runtime: {stats['pipeline']['runtime_seconds']:.2f}s")
        print(f"Total Processed: {stats['totals']['processed']}")
        print(f"Total Errors: {stats['totals']['errors']}")
        print(f"Total Dropped: {stats['totals']['dropped']}")
        print(f"Success Rate: {stats['totals']['success_rate']:.1%}")
        print(f"Overall Throughput: {stats['performance']['overall_throughput']:.2f} items/s")
        print("\n" + "-"*60)
        print("STAGE DETAILS")
        print("-"*60)
        
        for s in stage_stats:
            print(f"\n{s['name']}:")
            print(f"  Processed: {s['processed']}")
            print(f"  Errors: {s['errors']}")
            print(f"  Dropped: {s['dropped']}")
            print(f"  Queue Size: {s['queue_size']}")
            print(f"  Throughput: {s['performance']['throughput_per_sec']:.2f}/s")
            print(f"  Avg Time: {s['performance']['avg_processing_time']:.4f}s")
        
        print("\n" + "="*60 + "\n")
    
    def __enter__(self):
        """Context manager support"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure pipeline stops on context exit"""
        self.stop()
        return False