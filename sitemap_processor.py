import ray
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import gzip # Keep import for potential future use or if a specific edge case requires manual handling, but it won't be used in _fetch_content anymore for direct decompression.
import io
import time
from typing import Set, Tuple, List, Optional
from ray.util.queue import Queue
import logging

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

REQUEST_TIMEOUT = 10 # seconds
POLITENESS_DELAY = 0.1 # seconds

@ray.remote
class SitemapProcessor:
    def __init__(self, analysis_queue: Queue, visited_manager: ray.actor.ActorHandle):
        self.analysis_queue = analysis_queue
        self.visited_manager = visited_manager # To use its add_url for deduplication
        self.processed_sitemaps: Set[str] = set()
        self.total_urls_extracted = 0
        self.running = True

    def _normalize_domain(self, domain: str) -> str:
        """Removes 'www.' prefix and ensures consistency for domain comparison."""
        if domain.startswith("www."):
            domain = domain[4:]
        if ":" in domain: # Remove port if present
            domain = domain.split(":")[0]
        return domain.lower()

    def _fetch_content(self, url: str) -> Optional[bytes]:
        """
        Fetches raw content (bytes) from a URL.
        Relies on requests's automatic decompression for gzipped/deflated content.
        """
        try:
            time.sleep(POLITENESS_DELAY)
            # requests generally handles gzip/deflate decoding automatically based on Content-Encoding header.
            # We explicitly ask for gzip/deflate, but will trust requests to decompress if it gets it.
            headers = {'Accept-Encoding': 'gzip, deflate'}
            response = requests.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True, headers=headers)
            response.raise_for_status()

            # --- CRITICAL CORRECTION HERE ---
            # requests automatically decompresses content if Content-Encoding header is present.
            # So, response.content will already be the decompressed bytes.
            return response.content
            # --- END CRITICAL CORRECTION ---
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching sitemap content from {url}: {e}")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred while fetching sitemap {url}: {e}")
            return None

    def _parse_sitemap_xml(self, xml_content: bytes, sitemap_url: str, base_domain_normalized: str) -> List[str]:
        """Parses an XML sitemap and extracts URLs."""
        urls = []
        try:
            soup = BeautifulSoup(xml_content, 'xml') # Use 'xml' parser for sitemaps

            # Extract URLs from <loc> tags (webpages)
            for loc_tag in soup.find_all('loc'):
                url = loc_tag.text.strip()
                # Simple check for common image/media extensions to filter non-webpage links
                # This can be expanded based on specific needs.
                if url and not any(url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.pdf', '.css', '.js', '.xml', '.gz', '.txt']):
                    normalized_url = url.split('#')[0].rstrip('/')
                    # Ensure it's an internal link before considering
                    parsed_url = urlparse(normalized_url)
                    link_domain = parsed_url.netloc
                    # Use the new method here
                    if self._normalize_domain(link_domain) == base_domain_normalized:
                        urls.append(normalized_url)

            # Extract URLs from <sitemap> tags (nested sitemaps)
            for sitemap_tag in soup.find_all('sitemap'):
                nested_sitemap_url = sitemap_tag.loc.text.strip()
                if nested_sitemap_url and nested_sitemap_url not in self.processed_sitemaps:
                    urls.extend(self._process_sitemap(nested_sitemap_url, base_domain_normalized))

        except Exception as e:
            logging.error(f"Error parsing sitemap XML from {sitemap_url}: {e}")
        return urls

    def _parse_sitemap_txt(self, txt_content: bytes, sitemap_url: str, base_domain_normalized: str) -> List[str]:
        """Parses a plain text sitemap (one URL per line)."""
        urls = []
        try:
            for line in txt_content.decode('utf-8').splitlines():
                url = line.strip()
                if url and not any(url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.pdf', '.css', '.js', '.xml', '.gz', '.txt']):
                    normalized_url = url.split('#')[0].rstrip('/')
                    # Ensure it's an internal link before considering
                    parsed_url = urlparse(normalized_url)
                    link_domain = parsed_url.netloc
                    # Use the new method here
                    if self._normalize_domain(link_domain) == base_domain_normalized:
                        urls.append(normalized_url)
        except Exception as e:
            logging.error(f"Error parsing sitemap TXT from {sitemap_url}: {e}")
        return urls

    def _process_sitemap(self, sitemap_url: str, base_domain_normalized: str) -> List[str]:
        """Recursively processes a sitemap URL."""
        if sitemap_url in self.processed_sitemaps:
            return []

        self.processed_sitemaps.add(sitemap_url)
        logging.info(f"Processing sitemap: {sitemap_url}")

        content = self._fetch_content(sitemap_url)
        if not content:
            return []

        extracted_urls = []
        if sitemap_url.endswith('.xml') or sitemap_url.endswith('.xml.gz'):
            extracted_urls = self._parse_sitemap_xml(content, sitemap_url, base_domain_normalized)
        elif sitemap_url.endswith('.txt'):
            extracted_urls = self._parse_sitemap_txt(content, sitemap_url, base_domain_normalized)
        else:
            logging.warning(f"Unsupported sitemap format for {sitemap_url}. Skipping.")

        return extracted_urls

    def find_and_process_sitemaps(self, base_url: str) -> int:
        """
        Attempts to find sitemaps via robots.txt or common paths and processes them.
        Returns the count of unique webpage URLs pushed to the analysis queue.
        """
        initial_domain = urlparse(base_url).netloc
        initial_domain_normalized = self._normalize_domain(initial_domain)

        sitemap_urls_to_process = set()

        # 1. Check robots.txt for Sitemap-directive
        robots_txt_url = urljoin(base_url, '/robots.txt')
        logging.info(f"Checking robots.txt at: {robots_txt_url}")
        try:
            robots_content = self._fetch_content(robots_txt_url)
            if robots_content:
                for line in robots_content.decode('utf-8').splitlines():
                    if line.lower().startswith('sitemap:'):
                        sitemap_candidate = line[len('sitemap:'):].strip()
                        # Ensure sitemap is on the same base domain
                        sitemap_candidate_domain = urlparse(sitemap_candidate).netloc
                        if self._normalize_domain(sitemap_candidate_domain) == initial_domain_normalized:
                            sitemap_urls_to_process.add(sitemap_candidate)
                            logging.info(f"Found sitemap in robots.txt: {sitemap_candidate}")
        except Exception as e:
            logging.warning(f"Could not fetch or parse robots.txt from {robots_txt_url}: {e}")

        # 2. Check common sitemap locations if none found in robots.txt or directly
        if not sitemap_urls_to_process:
            common_sitemap_paths = [
                '/sitemap.xml',
                '/sitemap_index.xml',
                '/sitemap.gz',
                '/sitemapindex.xml',
                '/sitemap.txt'
            ]
            for path in common_sitemap_paths:
                candidate_url = urljoin(base_url, path)
                # Only try if it's on the same base domain
                candidate_domain = urlparse(candidate_url).netloc
                if self._normalize_domain(candidate_domain) == initial_domain_normalized:
                    try:
                        resp = requests.head(candidate_url, timeout=REQUEST_TIMEOUT)
                        if resp.status_code == 200 and ('xml' in resp.headers.get('Content-Type', '').lower() or 'text' in resp.headers.get('Content-Type', '').lower()):
                            sitemap_urls_to_process.add(candidate_url)
                            logging.info(f"Found sitemap at common path: {candidate_url}")
                            break # Assume one main sitemap for now
                    except requests.exceptions.RequestException:
                        pass # Ignore if HEAD request fails

        if not sitemap_urls_to_process:
            logging.info(f"No sitemaps found for {base_url}.")
            return 0 # No sitemaps found

        total_extracted_webpage_urls = 0
        for sitemap_url in list(sitemap_urls_to_process):
            extracted_from_this_sitemap = self._process_sitemap(sitemap_url, initial_domain_normalized)
            for url in extracted_from_this_sitemap:
                added = ray.get(self.visited_manager.add_url.remote(url))
                if added:
                    self.analysis_queue.put((url, None)) # Put URL and None for content (analyzer will fetch)
                    self.total_urls_extracted += 1
            total_extracted_webpage_urls += len(extracted_from_this_sitemap)

        logging.info(f"Sitemap processing finished. Total unique webpage URLs extracted: {self.total_urls_extracted}")
        return self.total_urls_extracted

    def stop(self):
        self.running = False