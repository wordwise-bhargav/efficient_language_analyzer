# Distributed Multilingual Web Crawler and Language Analyzer

This project is a highly scalable, parallelized system that crawls websites and performs per-page language analysis, with a focus on Indian and English languages. It uses Ray to manage distributed execution.

## Features

- Distributed web crawling using Ray actors  
- Intelligent sitemap parsing for efficient URL discovery  
- Multilingual language detection using langdetect and indic-nlp-library  
- Per-page and site-wide language statistics  
- Polite crawling behavior (respects delays and handles timeouts)  
- Automatic handling of HTML content filtering  

---

## 📦 Dependencies

It's highly recommended to create a virtual environment before installing dependencies:

For standard `venv`:

```bash
python -m venv .venv
```

```bash  
source .venv/bin/activate     # On Linux/macOS  
```

```bash
.venv\\Scripts\\activate      # On Windows
```

Then install the dependencies:

```bash
pip install -r requirements.txt
```

### Optional: Use `uv` for faster installs

If you have [`uv`](https://github.com/astral-sh/uv) installed:

```bash
uv pip install -r requirements.txt
```

Make sure `indic-nlp-library` and `langdetect` are included in `requirements.txt`, along with `Ray` and `BeautifulSoup`.

---

## 🚀 How to Run

### 1. Start the crawler and analyzer

Run the script using:

```bash
python main.py <project_name> <target_url> [--max_pages MAX] [--num_crawlers N] [--num_analysis_workers M]
```

#### Example:

```bash
python main.py MyProject https://example.com --max_pages 100 --num_crawlers 4 --num_analysis_workers 3
```

- project_name: A name for your project (used in output filenames)  
- target_url: The starting URL (must begin with `http://` or `https://`)  
- --max_pages: (Optional) Max number of unique pages to crawl (no limit by default)  
- --num_crawlers: (Optional) Number of parallel crawler workers (default: CPU cores)  
- --num_analysis_workers: (Optional) Number of parallel language analysis workers (default: CPU cores - 1)  

---

## 🗂️ Output Files

After execution, the following files will be saved in the current directory:

- `<project_name>_language_analysis.json`
  Per-page language analysis with word counts and language percentages.

- `<project_name>_summary.json`
  Site-wide language distribution summary, including total words and percentage per language.

- `<project_name>_crawled_urls.json`  
  List of all successfully crawled URLs.

---

## 🧠 Notes

- If the target site has a sitemap, the system will use it to extract URLs first before falling back to crawling.  
- The system respects polite crawling practices (timeouts and rate-limiting).  
- Uses Indic NLP tokenizer for Indian languages, and standard regex-based methods for English.

---

## ✅ Requirements

- `Python 3.8+`  
- `Ray`  
- `langdetect`  
- `indic-nlp-library`  
- `beautifulsoup4`  
- `requests`  

---

## 🛠 Developer Tips

- You can monitor status in the terminal while the script is running.  
- Press `Ctrl+C` to stop crawling early. Already queued pages will still be processed and saved.  
- Logs and intermediate progress are visible in the console.