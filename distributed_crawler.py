import ray
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
from typing import Set, Tuple, List, Optional
from ray.util.queue import Queue
import logging # Keep logging import for error/exception handling

# Configure a basic logger for errors/warnings, but not INFO/DEBUG for this module.
# The main.py will handle the high-level status updates.
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# Timeout for HTTP requests
REQUEST_TIMEOUT = 10 # seconds
# Delay between requests to be polite (adjust as needed for target site)
POLITENESS_DELAY = 0.1 # seconds

def _normalize_domain(domain: str) -> str:
    """Removes 'www.' prefix and ensures consistency for domain comparison."""
    if domain.startswith("www."):
        domain = domain[4:]
    if ":" in domain: # Remove port if present
        domain = domain.split(":")[0]
    return domain.lower()

@ray.remote
class VisitedUrlManager:
    """
    A Ray actor to manage a globally shared set of visited URLs.
    Ensures that URLs are not crawled more than once across all workers.
    """
    def __init__(self):
        self.visited_urls: Set[str] = set()
        self.crawled_count: int = 0
        self.max_pages_to_crawl: Optional[int] = None
        self.start_time: float = time.time() # To track overall rate

    def set_max_pages(self, max_pages: Optional[int]):
        self.max_pages_to_crawl = max_pages

    def add_url(self, url: str) -> bool:
        """
        Adds a URL to the visited set.
        Returns True if the URL was new and added, False if already present.
        """
        # Note: Max pages check is done by the worker before calling add_url
        #       but it's good to keep this as a safeguard.
        if self.max_pages_to_crawl is not None and self.crawled_count >= self.max_pages_to_crawl:
            return False

        # Normalize URL before adding to visited set for consistent deduplication
        normalized_url = url.split('#')[0].rstrip('/')
        if normalized_url not in self.visited_urls:
            self.visited_urls.add(normalized_url)
            self.crawled_count += 1
            return True
        return False

    def get_size(self) -> int:
        """Returns the current number of unique URLs visited."""
        return len(self.visited_urls)

    def get_crawled_count(self) -> int:
        """Returns the number of pages whose content was successfully processed/queued for analysis."""
        return self.crawled_count

    def get_all_visited_urls(self) -> List[str]:
        """Returns a list of all URLs that have been marked as visited."""
        return list(self.visited_urls)

    def get_max_pages(self) -> Optional[int]:
        """Returns the maximum number of pages to crawl."""
        return self.max_pages_to_crawl

    def get_crawl_rate(self) -> Tuple[int, float]:
        """Returns (crawled_count, elapsed_time) for rate calculation."""
        return self.crawled_count, time.time() - self.start_time


@ray.remote
class CrawlerWorker:
    """
    A Ray actor that performs the actual web crawling:
    fetches content, extracts internal links, and pushes data to queues.
    """
    def __init__(self,
                 crawl_queue: Queue,
                 analysis_queue: Queue,
                 visited_manager: ray.actor.ActorHandle,
                 base_domain: str):
        self.crawl_queue = crawl_queue
        self.analysis_queue = analysis_queue
        self.visited_manager = visited_manager
        self.base_domain_normalized = _normalize_domain(base_domain)
        self.running = True

    def _get_domain_normalized(self, url: str) -> Optional[str]:
        """Helper to extract and normalize domain from a URL."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            return _normalize_domain(domain) if domain else None
        except Exception as e:
            logging.warning(f"Error parsing domain from URL '{url}': {e}")
            return None

    def _is_internal_link(self, url: str) -> bool:
        """Checks if a URL belongs to the base domain after normalization."""
        link_domain_normalized = self._get_domain_normalized(url)
        return link_domain_normalized == self.base_domain_normalized

    def _fetch_html_content(self, url: str) -> Optional[str]:
        """Fetches HTML content from a given URL."""
        try:
            time.sleep(POLITENESS_DELAY)
            response = requests.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '').lower()
            if 'text/html' in content_type:
                return response.text
            else:
                logging.info(f"Skipping {url}: Not HTML content (Content-Type: {content_type}).")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {url}: {e}")
            return None

    def run(self):
        """
        The main loop for the crawler worker.
        Continuously pulls URLs from the crawl_queue, processes them,
        and pushes new data/links.
        """
        # Removed "CrawlerWorker started." log

        while self.running:
            try:
                url_from_queue = self.crawl_queue.get(timeout=2)
                if url_from_queue == "STOP_SIGNAL":
                    self.running = False
                    self.crawl_queue.put("STOP_SIGNAL")
                    break

                current_max_pages = ray.get(self.visited_manager.get_max_pages.remote())

                if current_max_pages is not None and ray.get(self.visited_manager.get_crawled_count.remote()) >= current_max_pages:
                    # No need to log here, orchestrator will handle overall status
                    self.running = False
                    self.crawl_queue.put("STOP_SIGNAL")
                    continue

                normalized_url_for_processing = url_from_queue.split('#')[0].rstrip('/')

                html_content = self._fetch_html_content(normalized_url_for_processing)

                if html_content:
                    self.analysis_queue.put((normalized_url_for_processing, html_content))
                    # No need to log push to analysis queue

                    soup = BeautifulSoup(html_content, 'html.parser')
                    for link_tag in soup.find_all('a', href=True):
                        href = link_tag['href']
                        full_url = urljoin(normalized_url_for_processing, href).split('#')[0].rstrip('/')

                        if self._is_internal_link(full_url):
                            added_to_visited_and_queued = ray.get(self.visited_manager.add_url.remote(full_url))
                            if added_to_visited_and_queued:
                                self.crawl_queue.put(full_url)
                else:
                    # No need to log "No HTML content fetched"
                    pass # Keep pass statement or remove the else block entirely if desired

            except ray.util.queue.Empty:
                pass
            except Exception as e:
                logging.exception(f"An unexpected error occurred in CrawlerWorker for {url_from_queue if 'url_from_queue' in locals() else 'unknown url'}: {e}")
        # Removed "CrawlerWorker finished." log

    def stop(self):
        """Method to cleanly stop the worker."""
        self.running = False