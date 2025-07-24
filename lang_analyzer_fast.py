import ray
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
import json
import os
import time
from typing import Dict, Any, Tuple, List, Set, Optional
from ray.util.queue import Queue
from bs4 import BeautifulSoup
from collections import defaultdict
import re
from bs4.element import Comment
import requests # NEW: Import requests for fetching content

# Only import the tokenizer from indicnlp, no langinfo
from indicnlp.tokenize import indic_tokenize

# Ensure consistent language detection results
DetectorFactory.seed = 0

# Compile the Unicode-aware regex for filtering words in Indic scripts
INDIC_WORD_FILTER_REGEX = re.compile(r'[^\W_]+', re.UNICODE)


# --- Configuration for Indian Languages ---
TARGET_LANGUAGES = {
    "hi", "bn", "te", "mr", "ta", "gu", "ur", "kn", "ml", "pa", "or",
    "en"
}

# Explicitly define which of our TARGET_LANGUAGES are considered Indic
HARDCODED_INDIC_LANGUAGES = {
    "hi", "bn", "te", "mr", "ta", "gu", "ur", "kn", "ml", "pa", "or"
}

# Timeout for HTTP requests in analyzer
ANALYSIS_REQUEST_TIMEOUT = 10 # seconds

@ray.remote
class ParallelLangDetectionProcessor:
    def __init__(self, project_name: str):
        self.output_file = f"{project_name.lower().replace(' ', '_')}_language_analysis.json"
        self.language_results: Dict[str, Dict[str, Any]] = {}
        self.running = True
        self.processed_count = 0
        self.site_total_word_counts: defaultdict[str, int] = defaultdict(int)
        self.site_total_pages_processed = 0

        print(f"ParallelLangDetectionProcessor actor initialized. Output file: {self.output_file}")

    def process_data_from_queue(self, data_queue: Queue):
        print("ParallelLangDetectionProcessor actor started processing queue...")
        while self.running:
            try:
                item = data_queue.get(timeout=3)
                if item == "STOP_SIGNAL":
                    print("ParallelLangDetectionProcessor received STOP_SIGNAL. Finishing current tasks and shutting down.")
                    self.running = False
                    data_queue.put("STOP_SIGNAL")
                    break

                url, html_content = item # html_content might be None now

                # --- NEW LOGIC FOR FETCHING CONTENT IF NOT PROVIDED ---
                actual_html_content = html_content
                if actual_html_content is None:
                    try:
                        print(f"Fetching content for analysis: {url}") # Indicate fetching
                        response = requests.get(url, timeout=ANALYSIS_REQUEST_TIMEOUT)
                        response.raise_for_status()
                        content_type = response.headers.get('Content-Type', '').lower()
                        if 'text/html' in content_type:
                            actual_html_content = response.text
                        else:
                            print(f"Skipping analysis for {url}: Not HTML content (Content-Type: {content_type}).", flush=True)
                            continue # Skip to next item in queue
                    except requests.exceptions.RequestException as e:
                        print(f"Error fetching content for analysis for {url}: {e}", flush=True)
                        continue # Skip to next item in queue
                    except Exception as e:
                        print(f"An unexpected error during fetch for {url}: {e}", flush=True)
                        continue # Skip to next item in queue
                # --- END NEW LOGIC ---

                if actual_html_content: # Only proceed if we have content
                    page_lang_stats = self._analyze_page_languages(actual_html_content)
                    self.language_results[url] = {
                        "url": url,
                        "content": page_lang_stats
                    }
                    self.processed_count += 1
                    for lang, count in page_lang_stats.get("word_count_per_language", {}).items():
                        self.site_total_word_counts[lang] += count
                    self.site_total_pages_processed += 1

            except ray.util.queue.Empty:
                pass
            except Exception as e:
                print(f"Error processing item in LangDetectionProcessor for {url if 'url' in locals() else 'unknown url'}: {e}", flush=True)

        self._save_results()
        print(f"ParallelLangDetectionProcessor finished and results saved to {self.output_file}")

    def _analyze_page_languages(self, html_content: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html_content, 'html.parser')
        for script_or_style in soup(['script', 'style', 'noscript', 'meta', 'link', 'img', 'svg']):
            script_or_style.extract()
        for element in soup(text=lambda text: isinstance(text, Comment)):
            element.extract()

        raw_text = soup.get_text(separator=' ', strip=True)
        text_blocks = raw_text.split('\n')
        text_blocks = [block.strip() for block in text_blocks if block.strip() and len(block.strip()) > 10]

        page_word_counts = defaultdict(int)
        page_languages_detected: Set[str] = set()
        total_words_on_page = 0

        for block in text_blocks:
            if not block:
                continue

            detected_lang = 'und'
            try:
                detected_lang = detect(block).split('-')[0]
            except LangDetectException:
                pass
            except Exception as e:
                print(f"Unexpected error during block language detection with langdetect: {e}", flush=True)

            words_in_block = []
            if detected_lang in TARGET_LANGUAGES:
                is_indic = detected_lang in HARDCODED_INDIC_LANGUAGES

                if is_indic:
                    words_in_block = indic_tokenize.trivial_tokenize(block, detected_lang)
                    words_in_block = [
                        INDIC_WORD_FILTER_REGEX.sub('', word).strip()
                        for word in words_in_block
                    ]
                    words_in_block = [word for word in words_in_block if word]
                elif detected_lang == 'en':
                    words_in_block = re.findall(r'\b[a-zA-Z]+\b', block.lower())
                else: # Fallback for other Latin-script languages that langdetect might find
                    words_in_block = re.findall(r'\b\w+\b', block.lower())
            else: # If not in TARGET_LANGUAGES at all
                detected_lang = "other_lang"
                words_in_block = re.findall(r'\b\w+\b', block.lower())

            if words_in_block:
                page_languages_detected.add(detected_lang)
                word_count_in_block = len(words_in_block)
                page_word_counts[detected_lang] += word_count_in_block
                total_words_on_page += word_count_in_block

        percentage_per_language = {}
        if total_words_on_page > 0:
            for lang, count in page_word_counts.items():
                percentage_per_language[lang] = round((count / total_words_on_page) * 100, 2)

        return {
            "languages_used": sorted(list(page_languages_detected)),
            "word_count_per_language": dict(page_word_counts),
            "percentage_per_language": percentage_per_language,
            "total_counted_words": total_words_on_page
        }

    def _save_results(self):
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(self.language_results, f, indent=4)
        except Exception as e:
            print(f"Error saving language analysis results to {self.output_file}: {e}", flush=True)

    def get_site_summary(self) -> Dict[str, Any]:
        total_site_words = sum(self.site_total_word_counts.values())
        site_percentage_per_language = {}
        if total_site_words > 0:
            for lang, count in self.site_total_word_counts.items():
                site_percentage_per_language[lang] = round((count / total_site_words) * 100, 2)
        return {
            "total_pages_processed": self.site_total_pages_processed,
            "site_total_word_counts": dict(self.site_total_word_counts),
            "site_percentage_per_language": site_percentage_per_language,
            "site_total_words": total_site_words
        }

    def wait_for_completion(self):
        while self.running:
            time.sleep(0.5)
        return True