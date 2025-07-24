import ray
import time
import json
import sys
import argparse
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse
import multiprocessing
import os

# Import the actors and queue
from distributed_crawler import CrawlerWorker, VisitedUrlManager
from lang_analyzer_fast import ParallelLangDetectionProcessor
from sitemap_processor import SitemapProcessor
from ray.util.queue import Queue

def get_domain_from_url(url: str) -> str:
    parsed_url = urlparse(url)
    return parsed_url.netloc

def save_summary_to_file(project_name: str, site_summary: Dict[str, Any]):
    """
    Saves the site-wide language analysis summary to a JSON file.
    """
    summary_output_file = f"{project_name.lower().replace(' ', '_')}_summary.json"
    try:
        with open(summary_output_file, 'w', encoding='utf-8') as f:
            json.dump(site_summary, f, indent=4)
        print(f"\nSite-wide language summary saved to: {summary_output_file}")
    except Exception as e:
        print(f"Error saving site summary to {summary_output_file}: {e}", flush=True)


def main(project_name: str, target_url: str, max_pages: Optional[int], num_crawlers: int, num_analysis_workers: int): # num_analysis_workers added here
    TOTAL_CPUS_AVAILABLE = multiprocessing.cpu_count()
    print(f"Detected {TOTAL_CPUS_AVAILABLE} CPU cores.")

    print(f"--- Starting Distributed Site Analysis for {get_domain_from_url(target_url)} ---")
    start_time = time.time()
    print(f"Start Time: {time.strftime('%H:%M:%S')}")
    print(f"Target URL: {target_url}")
    print(f"Max Pages to Crawl: {max_pages if max_pages is not None else 'No Limit'}")
    print(f"Number of Crawler Workers: {num_crawlers}")
    print(f"Number of Language Analysis Workers: {num_analysis_workers}") # Print the new argument value
    print("--------------------------------------------------")

    crawl_queue = Queue()
    analysis_queue = Queue()

    visited_manager = VisitedUrlManager.remote()
    ray.get(visited_manager.set_max_pages.remote(max_pages))

    # --- START OF KEY CORRECTION: LAUNCHING MULTIPLE ANALYSIS ACTORS ---
    analysis_actors = []
    for i in range(num_analysis_workers):
        # Each analysis actor will take 1 CPU, and they will run in parallel.
        # This allows for concurrent fetching and analysis of URLs.
        actor = ParallelLangDetectionProcessor.options(num_cpus=1).remote(project_name)
        analysis_actors.append(actor)
        actor.process_data_from_queue.remote(analysis_queue) # Start analysis actor consuming from queue

    print(f"Language Analysis Actors launched: {len(analysis_actors)} actors, each using 1 CPU.")
    # --- END OF KEY CORRECTION ---

    sitemap_found_and_processed = False
    sitemap_urls_count = 0

    # Phase 2: Sitemap Processing Attempt
    print("\nAttempting to find and process sitemaps...")
    # Sitemap processor still takes 1 CPU, as its task is sequential and I/O bound
    sitemap_processor = SitemapProcessor.options(num_cpus=1).remote(analysis_queue, visited_manager)
    try:
        sitemap_urls_count = ray.get(sitemap_processor.find_and_process_sitemaps.remote(target_url), timeout=120)
        if sitemap_urls_count > 0:
            sitemap_found_and_processed = True
            print(f"Successfully extracted {sitemap_urls_count} unique URLs from sitemaps.")
            print("These URLs will now be processed for language analysis. No crawling will occur unless max_pages is not met and additional crawling is desired.")
        else:
            print("No sitemaps found or extracted URLs. Falling back to traditional crawling.")
    except ray.exceptions.RayActorError as e:
        print(f"Sitemap processor actor failed: {e}. Falling back to traditional crawling.")
    except Exception as e:
        print(f"Error during sitemap processing: {e}. Falling back to traditional crawling.")
    finally:
        try:
            ray.kill(sitemap_processor)
        except ray.exceptions.RayActorError:
            pass

    # Phase 3: Conditional Crawling (Fallback if no sitemap or if you want more pages than sitemap provides)
    if not sitemap_found_and_processed:
        print(f"\nLaunching {num_crawlers} Crawler Workers (each using 1 CPU)...")
        crawler_workers = [
            CrawlerWorker.remote(crawl_queue, analysis_queue, visited_manager, get_domain_from_url(target_url))
            for _ in range(num_crawlers)
        ]
        for worker in crawler_workers:
            worker.run.remote()

        initial_url_normalized = target_url.split('#')[0].rstrip('/')
        added_initial = ray.get(visited_manager.add_url.remote(initial_url_normalized))
        if added_initial:
            crawl_queue.put(initial_url_normalized)
        else:
            print(f"Warning: Initial URL '{target_url}' was already considered visited or max pages is 0 for crawling.")
            pass

        print(f"Crawler Workers (each 1 CPU) running: {num_crawlers}")
        print(f"Ray will schedule up to {TOTAL_CPUS_AVAILABLE} total CPUs.")


        idle_rounds = 0
        max_idle_rounds = 5
        status_update_interval = 2

        try:
            while True:
                crawled_count = ray.get(visited_manager.get_crawled_count.remote())
                current_max_pages = ray.get(visited_manager.get_max_pages.remote())
                crawl_queue_size = crawl_queue.size()
                current_analysis_queue_size = analysis_queue.size()

                pages_processed_for_rate, elapsed_time_for_rate = ray.get(visited_manager.get_crawl_rate.remote())
                crawl_rate = (pages_processed_for_rate / elapsed_time_for_rate) if elapsed_time_for_rate > 0 else 0

                urls_left_to_process = "N/A"
                if current_max_pages is not None:
                    urls_left_to_process = max(0, current_max_pages - crawled_count)

                sys.stdout.write(
                    f"\rStatus: Crawled {crawled_count} pages (Max: {current_max_pages if current_max_pages is not None else 'No Limit'}), "
                    f"Crawl Queue: {crawl_queue_size}, Analysis Queue: {current_analysis_queue_size}, "
                    f"URLs Left: {urls_left_to_process}, "
                    f"Rate: {crawl_rate:.2f} pages/sec"
                )
                sys.stdout.flush()

                if current_max_pages is not None and crawled_count >= current_max_pages:
                    print(f"\nMax pages ({current_max_pages}) reached. Initiating shutdown of crawlers.")
                    break

                if crawl_queue_size == 0 and current_analysis_queue_size == 0:
                    idle_rounds += 1
                    if idle_rounds >= max_idle_rounds:
                        print("\nCrawl queue and analysis queue consistently empty. Initiating shutdown of crawlers.")
                        break
                else:
                    idle_rounds = 0

                time.sleep(status_update_interval)

        except KeyboardInterrupt:
            print("\nCrawling interrupted by user.")
        finally:
            print("\nSending STOP_SIGNAL to all crawler workers...")
            for _ in range(num_crawlers):
                crawl_queue.put("STOP_SIGNAL")

            print("Waiting for crawler workers to terminate and free resources...")
            time.sleep(5)
            for worker in crawler_workers:
                try:
                    ray.kill(worker)
                except ray.exceptions.RayActorError:
                    pass

            print("Crawler workers terminated. Language Analysis now has full CPU resources.")
    else: # If sitemaps were processed, we still need to wait for analysis queue to clear
        print(f"\n{sitemap_urls_count} URLs from sitemap(s) added to analysis queue. Waiting for analysis to complete.")


    # --- START OF KEY CORRECTION: SENDING STOP SIGNAL TO ALL ACTORS ---
    print("Sending STOP_SIGNAL to all language analysis actors...")
    for _ in range(num_analysis_workers): # Send stop signal for each analysis actor
        analysis_queue.put("STOP_SIGNAL")

    print("Waiting for language analysis to complete...")
    for actor in analysis_actors: # Wait for each analysis actor to complete
        try:
            ray.get(actor.wait_for_completion.remote(), timeout=600)
        except ray.exceptions.RayActorError:
            print("One language analysis actor terminated unexpectedly or timed out during completion wait.")
        except Exception as e:
            print(f"Error waiting for an analysis actor: {e}")
    # --- END OF KEY CORRECTION ---

    # Retrieve all visited URLs
    all_crawled_urls = ray.get(visited_manager.get_all_visited_urls.remote())
    crawled_urls_output_file = f"{project_name.lower().replace(' ', '_')}_crawled_urls.json"
    with open(crawled_urls_output_file, 'w', encoding='utf-8') as f:
        json.dump(all_crawled_urls, f, indent=4)
    print(f"Saved all crawled URLs to {crawled_urls_output_file}")

    # Retrieve and display site-wide language summary from the first analysis actor
    # IMPORTANT: Ensure that the summary collection logic correctly aggregates if
    # ParallelLangDetectionProcessor's site_total_word_counts is per-actor.
    # For now, we fetch from the first actor, assuming its state represents the full site.
    print("\n--- Site-Wide Language Summary ---")
    site_summary = ray.get(analysis_actors[0].get_site_summary.remote()) # Get summary from one of the analysis actors

    save_summary_to_file(project_name, site_summary)

    print(f"Total Pages Processed for Analysis: {site_summary.get('total_pages_processed', 0)}")
    print(f"Total Words Counted Across Site: {site_summary.get('site_total_words', 0)}")
    print("Word Count Per Language (Site-wide):")
    for lang, count in sorted(site_summary.get('site_total_word_counts', {}).items()):
        print(f"   - {lang}: {count} words")
    print("Percentage Per Language (Site-wide):")
    for lang, percent in sorted(site_summary.get('site_percentage_per_language', {}).items(), key=lambda item: item[1], reverse=True):
        print(f"   - {lang}: {percent}%")
    print("----------------------------------")

    end_time = time.time()
    total_unique_urls = ray.get(visited_manager.get_size.remote())
    print("--------------------------------------------------")
    print("--- Analysis Complete ---")
    print(f"Total Unique URLs Crawled: {total_unique_urls}")
    print(f"End Time: {time.strftime('%H:%M:%S')}")
    print(f"Total Elapsed Time: {end_time - start_time:.2f} seconds")
    print("Shutting down Ray...")
    ray.shutdown()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed web crawler and language analyzer with Ray.")
    parser.add_argument("project_name", type=str, help="Name of the project (e.g., 'MyWebsiteAnalysis')")
    parser.add_argument("target_url", type=str, help="Starting URL to crawl (e.g., 'https://example.com')")
    parser.add_argument("--max_pages", type=int, default=None,
                        help="Maximum number of unique pages to crawl. Defaults to no limit.")
    parser.add_argument("--num_crawlers", type=int,
                        default=multiprocessing.cpu_count(), # Default crawlers to total cores
                        help="Number of parallel crawler workers to launch. Defaults to the number of CPU cores.")
    # --- START OF KEY CORRECTION: NEW ARGUMENT FOR ANALYSIS WORKERS ---
    parser.add_argument("--num_analysis_workers", type=int,
                        default=max(1, multiprocessing.cpu_count() - 1), # Default to N-1 cores for analysis, leaving one for sitemap/overhead
                        help="Number of parallel language analysis workers to launch. Defaults to (total cores - 1).")
    # --- END OF KEY CORRECTION ---

    args = parser.parse_args()

    if not args.target_url.startswith(("http://", "https://")):
        print(f"Error: Target URL must start with 'http://' or 'https://'. Received: {args.target_url}")
        sys.exit(1)

    if args.num_crawlers <= 0:
        print("Error: Number of crawlers must be a positive integer.")
        sys.exit(1)

    if args.num_analysis_workers <= 0:
        print("Error: Number of analysis workers must be a positive integer.")
        sys.exit(1)

    # Small warning if analysis workers exceed total CPUs (Ray will manage, but direct mapping might not be 1:1)
    if args.num_analysis_workers > multiprocessing.cpu_count():
        print(f"Warning: --num_analysis_workers ({args.num_analysis_workers}) exceeds total available CPUs ({multiprocessing.cpu_count()}). Ray will manage, but performance might not scale linearly beyond physical cores.")

    try:
        ray.init(ignore_reinit_error=True) # Initialize Ray once
        main(args.project_name, args.target_url, args.max_pages, args.num_crawlers, args.num_analysis_workers) # Pass new arg
    except Exception as e:
        print(f"\nAn unhandled error occurred: {e}")
        if ray.is_initialized():
            ray.shutdown()
        sys.exit(1)