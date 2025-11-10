# scraper_logic.py
import time
from datetime import datetime
import json
import urllib.parse
from pathlib import Path
import logging
import configparser
import os
from tkinter import filedialog
import tkinter as tk
import shutil
import threading
import platform

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

logger = logging.getLogger(__name__)

CODE_VALUE = "409573"
PASSWORD_VALUE = "220106"
RUN_HEADLESS = False
TOPICS_FILE = Path("topics.json")
LOGIN_URL = "https://www2.kolhalashon.com/#/login/%2FregularSite%2Fnew"
CONFIG_FILE = Path("config.ini")

# --- FIX: שינוי סדר הפעולות לאיתור הדרייבר ---
def _create_webdriver_standalone(status_callback):
    status_callback("בודק הגדרות דרייבר...")
    logger.info("Attempting to create webdriver.")
    
    config = configparser.ConfigParser()
    driver_path = None
    service = None

    # שלב 1: בדוק אם קיים נתיב שמור ועובד
    if CONFIG_FILE.exists():
        config.read(CONFIG_FILE)
        if 'Paths' in config and 'driver_path' in config['Paths']:
            saved_path = config['Paths']['driver_path']
            if os.path.exists(saved_path):
                driver_path = saved_path
                logger.info(f"Using saved driver path from config.ini: {driver_path}")
                status_callback("משתמש בנתיב דרייבר שמור.")
                service = ChromeService(executable_path=driver_path)

    # שלב 2: אם אין נתיב שמור, נסה את הדרך האוטומטית (דורש אינטרנט)
    if not service:
        try:
            status_callback("מנסה לאתר דרייבר אוטומטית...")
            logger.info("No valid saved path. Trying webdriver-manager.")
            service = ChromeService(ChromeDriverManager().install())
        except Exception as e:
            logger.error(f"Webdriver-manager failed: {e}")
            status_callback("איתור אוטומטי נכשל. יש לבחור קובץ דרייבר ידנית.")
            
            # שלב 3: אם הכל נכשל, בקש מהמשתמש לבחור ידנית
            root = tk.Tk()
            root.withdraw()
            file_types = [("All files", "*.*")] if platform.system() == "Darwin" else [("Executable files", "*.exe")]
            manual_path = filedialog.askopenfilename(title="אנא בחר את קובץ chromedriver", filetypes=file_types)
            root.destroy()

            if manual_path:
                logger.info(f"User selected manual path: {manual_path}")
                config['Paths'] = {'driver_path': manual_path}
                with open(CONFIG_FILE, 'w') as configfile: config.write(configfile)
                logger.info(f"Saved new driver path to {CONFIG_FILE}")
                service = ChromeService(executable_path=manual_path)
            else:
                logger.critical("User did not select a driver. Aborting.")
                status_callback("לא נבחר דרייבר. לא ניתן להמשיך.")
                return None

    status_callback("מפעיל את הדפדפן...")
    chrome_options = ChromeOptions()
    if RUN_HEADLESS:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1400,900")
    chrome_options.add_argument("--disable-features=RendererCodeIntegrity")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    temp_download_path = str(Path.home() / 'Downloads' / 'kol_halashon_temp')
    os.makedirs(temp_download_path, exist_ok=True)
    prefs = {
        "download.default_directory": temp_download_path,
        "profile.default_content_setting_values.automatic_downloads": 1
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver
# ----------------------------------------------------------------

def initial_login(status_callback):
    driver = _create_webdriver_standalone(status_callback)
    if not driver:
        return None

    status_callback("מתחיל תהליך התחברות...")
    logger.info("Navigating to login page.")
    driver.get(LOGIN_URL)
    try:
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input[formcontrolname='code']"))).send_keys(CODE_VALUE)
        password_input = driver.find_element(By.CSS_SELECTOR, "input[formcontrolname='password']")
        password_input.send_keys(PASSWORD_VALUE)
        password_input.send_keys(webdriver.common.keys.Keys.ENTER)
        WebDriverWait(driver, 25).until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".banner-search input, .banner-title input")))
        status_callback("✅ התחברות בוצעה בהצלחה.")
        logger.info("Login successful.")
        return driver
    except Exception as e:
        status_callback("❌ שגיאה: התחברות נכשלה.")
        logger.error(f"Login failed: {e}")
        driver.quit()
        return None

class Scraper:
    def __init__(self, driver, status_callback=None, download_progress_callback=None):
        self.driver = driver
        self.status_callback = status_callback
        self.download_progress_callback = download_progress_callback
        self.topics_data = None
        self.temp_download_path = str(Path.home() / 'Downloads' / 'kol_halashon_temp')
        self.final_download_path = str(Path.home())
        self.download_lock = threading.Lock()
        # Lock to serialize interactions with the selenium webdriver (not thread-safe)
        self.driver_lock = threading.Lock()

    def set_final_download_path(self, path):
        # Ensure a dedicated subfolder 'קול הלשון' under the chosen path
        target = os.path.join(path, "קול הלשון")
        os.makedirs(target, exist_ok=True)
        self.final_download_path = target

        # Clear temp download folder when changing final path to avoid stale files
        try:
            if os.path.exists(self.temp_download_path):
                for name in os.listdir(self.temp_download_path):
                    fp = os.path.join(self.temp_download_path, name)
                    try:
                        if os.path.isfile(fp) or os.path.islink(fp):
                            os.remove(fp)
                        elif os.path.isdir(fp):
                            shutil.rmtree(fp)
                    except Exception as e:
                        logger.warning(f"Failed to remove temp file {fp}: {e}")
        except Exception as e:
            logger.warning(f"Could not clear temp download dir: {e}")

        self._update_status(f"ההורדות הבאות יישמרו ב: {self.final_download_path}")
        logger.info(f"Final download path set to: {self.final_download_path}")

    def _update_status(self, message):
        if self.status_callback:
            self.status_callback(message)
            
    def _update_download_progress(self, download_id, progress, status):
        if self.download_progress_callback:
            self.download_progress_callback(download_id, progress, status)

    def _js_click(self, element):
        self.driver.execute_script("arguments[0].click();", element)

    def _wait_for_file_ready(self, path, timeout=15, poll_interval=0.5):
        """Wait until the file exists, is readable and (optionally) stable.
        Returns True if the file appears ready within timeout, False otherwise.
        """
        start = time.time()
        while time.time() - start < timeout:
            if not os.path.exists(path):
                time.sleep(poll_interval)
                continue
            try:
                # Try to open the file for reading to ensure no exclusive lock by antivirus/OS
                with open(path, 'rb') as fh:
                    fh.read(1)
                return True
            except Exception:
                time.sleep(poll_interval)
                continue
        return False

    def _try_move_file(self, src_path, dest_dir, max_attempts=3, wait_ready_timeout=15, attempt_delay=0.8):
        """Try to move src_path into dest_dir with retries and readiness checks.
        Returns destination_path on success, or None on failure.
        """
        if not os.path.exists(src_path):
            return None
        filename = os.path.basename(src_path)
        dest_path = os.path.join(dest_dir, filename)
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            try:
                # wait until file is readable/unlocked
                if not self._wait_for_file_ready(src_path, timeout=wait_ready_timeout):
                    logger.debug(f"Move attempt {attempt}: file not ready: {src_path}")
                    time.sleep(attempt_delay)
                    continue
                # ensure unique destination
                name, ext = os.path.splitext(filename)
                candidate = dest_path
                counter = 1
                while os.path.exists(candidate):
                    candidate = os.path.join(dest_dir, f"{name} ({counter}){ext}")
                    counter += 1
                shutil.move(src_path, candidate)
                return candidate
            except FileNotFoundError:
                logger.warning(f"Move attempt {attempt}: file disappeared before move: {src_path}")
                return None
            except Exception as e:
                logger.warning(f"Move attempt {attempt} failed for {src_path}: {e}")
                time.sleep(attempt_delay)
                continue
        return None

    def _attempt_post_move(self, file_id, expected_filename, start_time):
        """After a successful primary move, attempt to move any remaining matching mp3s created since start_time."""
        try:
            temp_dir = self.temp_download_path
            candidates = [f for f in os.listdir(temp_dir) if f.lower().endswith('.mp3') and (file_id in f or expected_filename in f) and os.path.getmtime(os.path.join(temp_dir, f)) >= start_time - 0.5]
            if not candidates:
                return
            # sort oldest-first to move in order
            for c in sorted(candidates, key=lambda f: os.path.getmtime(os.path.join(temp_dir, f))):
                full = os.path.join(temp_dir, c)
                dest = self._try_move_file(full, self.final_download_path, max_attempts=4, wait_ready_timeout=20)
                if dest:
                    logger.info(f"Post-move moved extra mp3 {full} -> {dest}")
        except Exception as e:
            logger.warning(f"Post-move attempt failed: {e}")

    def load_topics_from_file(self):
        if self.topics_data: return self.topics_data
        if not TOPICS_FILE.exists(): return None
        with open(TOPICS_FILE, 'r', encoding='utf-8') as f:
            self.topics_data = json.load(f)
        return self.topics_data

    def _get_current_shiurim_and_filters(self):
        self._update_status("טוען שיעורים ומסננים...")
        logger.info("Extracting shiurim and filters from page.")
        try:
            shiurim_list = self.driver.execute_script("""
            let shiurs = [];
            document.querySelectorAll('app-shiurim-display .shiur-container').forEach((el, i) => {
                let title = el.querySelector('.shiurim-title')?.textContent.trim() || '';
                let rav = el.querySelector('.shiurim-rav-name')?.textContent.trim() || '';
                let date = el.querySelector('.shiurim-start-time')?.textContent.trim() || '';
                let link = el.querySelector('a')?.href || '';
                shiurs.push({id: i, title: title, rav: rav, date: date, link: link});
            });
            return shiurs;
            """)
        except Exception as e:
            shiurim_list = []
            self._update_status("אזהרה: שגיאה בקריאת פרטי השיעורים.")
            logger.warning(f"Could not extract shiurim via JS: {e}")
        filters_data = []
        try:
            filter_groups = self.driver.find_elements(By.CSS_SELECTOR, "app-filter-container")
            for group in filter_groups:
                # find header safely
                headers = group.find_elements(By.CSS_SELECTOR, ".filter-header")
                if not headers:
                    continue
                header = headers[0]
                category_title = header.text.strip()
                if not category_title:
                    continue

                # open container if needed (guarded)
                inner_containers = group.find_elements(By.CSS_SELECTOR, ".filter-container")
                inner_container = inner_containers[0] if inner_containers else None
                if inner_container:
                    try:
                        if "opened" not in inner_container.get_attribute("class"):
                            self._js_click(header)
                            time.sleep(0.5)
                    except Exception:
                        # ignore and continue
                        pass

                # try to expand "show more" elements if present
                while True:
                    show_mores = group.find_elements(By.XPATH, ".//div[contains(@class, 'display-more') and contains(normalize-space(), 'הצג עוד')]")
                    if not show_mores:
                        break
                    for sm in show_mores:
                        try:
                            self._js_click(sm)
                            time.sleep(0.5)
                        except Exception:
                            continue

                try:
                    WebDriverWait(self.driver, 5).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "mat-checkbox.filter-option")))
                except Exception:
                    # continue anyway and try to read what exists
                    pass

                time.sleep(0.5)
                options = group.find_elements(By.CSS_SELECTOR, "mat-checkbox.filter-option")
                category_filters = []
                for option in options:
                    label_els = option.find_elements(By.CSS_SELECTOR, ".mat-checkbox-label")
                    if not label_els:
                        continue
                    label_el = label_els[0]
                    try:
                        full_label = self.driver.execute_script("return arguments[0].textContent;", label_el).strip().replace('\n', ' ').replace('  ', ' ')
                        if full_label:
                            category_filters.append(full_label)
                    except Exception:
                        continue
                if category_filters:
                    filters_data.append({'category_name': category_title, 'filters': category_filters})
        except Exception as e:
            # don't raise an exception to the caller; log a concise message
            self._update_status(f"אזהרה: לא ניתן היה לטעון מסננים.")
            logger.warning(f"Could not extract filters: {str(e)}")
        self._update_status(f"נמצאו {len(shiurim_list)} שיעורים ו-{len(filters_data)} קטגוריות סינון.")
        logger.info(f"Found {len(shiurim_list)} shiurim and {len(filters_data)} filter categories.")
        try:
            # append shiurim list to a log file with timestamp, title and link
            self._append_shiurim_log(shiurim_list)
        except Exception as e:
            logger.warning(f"Failed to write shiurim log: {e}")
        return {'type': 'shiurim_and_filters', 'data': {'shiurim': shiurim_list, 'filters': filters_data}}

    def _append_shiurim_log(self, shiurim_list):
        """Append the current list of shiurim to a log file (JSON lines).
        Each line: {timestamp, title, link}
        """
        log_path = Path('shiurim_list.log')
        now = datetime.utcnow().isoformat() + 'Z'
        with open(log_path, 'a', encoding='utf-8') as fh:
            for s in shiurim_list:
                entry = {'timestamp': now, 'title': s.get('title', ''), 'link': s.get('link', '')}
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def _handle_results_page(self):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._update_status(f"ממתין לתוצאות (ניסיון {attempt + 1}/{max_retries})...")
                WebDriverWait(self.driver, 10).until(EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "app-shiurim-display .shiur-container")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".rav-container"))
                ))
                time.sleep(2)
                rav_results = self.driver.find_elements(By.CSS_SELECTOR, ".rav-container")
                if rav_results:
                    try:
                        WebDriverWait(self.driver, 10).until(
                            lambda d: d.find_element(By.CSS_SELECTOR, ".rav-container .rav-name").text.strip() != ""
                        )
                    except TimeoutException:
                        logger.warning("Timed out waiting for rav names to load. Proceeding anyway.")
                    rav_list = self.driver.execute_script("""
                    let ravs = [];
                    document.querySelectorAll('.rav-container').forEach((el, i) => {
                        let name = el.querySelector('.rav-name')?.textContent.trim() || '';
                        let count = el.querySelector('.rav-shiurim-sum')?.textContent.trim() || '';
                        ravs.push({id: i, name: name, count: count});
                    });
                    return ravs;
                    """)
                    if rav_list and rav_list[0]['name']:
                        return {'type': 'rav_selection', 'data': rav_list}
                
                shiurim = self.driver.find_elements(By.CSS_SELECTOR, "app-shiurim-display .shiur-container")
                if shiurim:
                    return self._get_current_shiurim_and_filters()
                
                raise TimeoutException("Content not fully loaded, retrying...")

            except TimeoutException:
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    logger.error("Failed to get results after multiple retries.")
                    return {'type': 'error', 'message': 'לא נמצאו תוצאות לאחר מספר ניסיונות.'}

    def refresh_current_page_content(self):
        self._update_status("טוען מחדש נתונים מהעמוד...")
        logger.info("Re-extracting data from current page (no refresh).")
        return self._handle_results_page()

    def refresh_browser_page(self):
        self._update_status("מרענן את הדף בדפדפן...")
        logger.info("Refreshing browser page.")
        self.driver.refresh()
        return self._handle_results_page()

    def perform_search(self, query: str):
        self._update_status(f"מבצע חיפוש: '{query}'...")
        logger.info(f"Performing search for: '{query}'")
        search_type = "ravSearch" if query.strip().startswith("הרב") else "searchResults"
        encoded_query = urllib.parse.quote(query)
        self.driver.execute_script(f"window.location.hash = '#/regularSite/{search_type}/{encoded_query}';")
        return self._handle_results_page()

    def navigate_to_topic_by_href(self, href: str):
        self._update_status("מנווט לקטגוריה...")
        logger.info(f"Navigating to topic: {href}")
        self.driver.get(href)
        return self._handle_results_page()

    def select_rav_from_results(self, rav_id: int):
        self._update_status("בוחר רב מהרשימה...")
        logger.info(f"Selecting rav with ID: {rav_id}")
        fresh_rav_results = self.driver.find_elements(By.CSS_SELECTOR, ".rav-container")
        if rav_id >= len(fresh_rav_results):
            self._update_status("❌ שגיאה: הרב הנבחר לא נמצא. נסה לרענן.")
            logger.error(f"IndexError: rav_id {rav_id} is out of bounds for results list of size {len(fresh_rav_results)}.")
            return {'type': 'error', 'message': 'הרב לא נמצא'}
        self._js_click(fresh_rav_results[rav_id].find_element(By.CSS_SELECTOR, "a.rav-name"))
        return self._handle_results_page()

    def apply_filter_by_name(self, filter_name: str):
        self._update_status(f"מפעיל מסנן: {filter_name}...")
        logger.info(f"Applying filter: {filter_name}")
        try:
            first_shiur_element = None
            try:
                first_shiur_element = self.driver.find_element(By.CSS_SELECTOR, "app-shiurim-display .shiur-container")
            except NoSuchElementException:
                pass 
            
            click_script = """
            const filterName = arguments[0];
            const checkboxes = document.querySelectorAll('mat-checkbox.filter-option');
            for (const cb of checkboxes) {
                const label = cb.querySelector('.mat-checkbox-label');
                if (label && label.textContent.trim().includes(filterName)) {
                    cb.querySelector('input').click();
                    return true;
                }
            }
            return false;
            """
            clicked = self.driver.execute_script(click_script, filter_name)
            if not clicked:
                raise Exception("לא נמצא מסנן עם השם המבוקש.")

            if first_shiur_element:
                WebDriverWait(self.driver, 20).until(EC.staleness_of(first_shiur_element))
            
            return self._handle_results_page()
        except Exception as e:
            logger.error(f"Error in apply_filter_by_name for '{filter_name}': {e}")
            self._update_status(f"❌ שגיאה בהפעלת המסנן: {filter_name}")
            return {'type': 'error', 'message': f'שגיאה בהפעלת המסנן'}
            
    def download_shiur_by_id(self, shiur_id: int, shiur_title: str):
        """Start a download for the given shiur. This monitors the temp download directory for
        a new file (either a .crdownload that completes or a new .mp3) and moves it to the final
        download path. Only the final move is serialized by a lock so multiple downloads can run
        concurrently.
        """
        download_id = f"{shiur_id}_{int(time.time())}"
        self._update_download_progress(download_id, 0, "starting")

        try:
            self._update_status(f"מתחיל הורדה: {shiur_title}")
            logger.info(f"Initiating download for shiur ID: {shiur_id}")

            # All direct webdriver/DOM access must be serialized to avoid concurrent webdriver calls
            with self.driver_lock:
                shiur_elements = self.driver.find_elements(By.CSS_SELECTOR, "app-shiurim-display .shiur-container")
                phone_button = shiur_elements[shiur_id].find_element(By.XPATH, ".//button[contains(@class, 'click-phone-button')]")
                file_id = phone_button.text.strip()

            # Build expected filename (sanitized) but be tolerant if Chrome names it slightly different
            expected_filename = f"{shiur_title} - {file_id}.mp3"
            invalid_chars = '<>:"/\\|?*'
            for char in invalid_chars:
                expected_filename = expected_filename.replace(char, '')

            temp_dir = self.temp_download_path
            os.makedirs(temp_dir, exist_ok=True)

            # Snapshot existing files so we can detect the new one
            before_files = set(os.listdir(temp_dir))
            logger.debug(f"Download {download_id} before_files: {before_files}")

            # Trigger download in browser --- serialize access to the webdriver
            with self.driver_lock:
                self._js_click(shiur_elements[shiur_id].find_element(By.XPATH, ".//button[.//svg-icon[contains(@src, 'download-i.svg')]]"))
                try:
                    audio_option = WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'download-option')]") ))
                    self._js_click(audio_option)
                except TimeoutException:
                    # Not all downloads present an additional option
                    pass

            self._update_status("מוריד...")
            logger.info(f"Waiting for download (looking in {temp_dir}) for pattern containing '{file_id}' or '{expected_filename}'")

            wait_time = 600
            start_time = time.time()
            completed_path = None

            # Continuously look for any new files (mp3 or .crdownload) that were NOT present before the download started
            processed = set()
            while time.time() - start_time < wait_time:
                current_files = set(os.listdir(temp_dir))
                # candidates = new files that were not in before_files and not already processed
                candidates = [f for f in current_files if f not in before_files and f not in processed and (f.lower().endswith('.mp3') or f.endswith('.crdownload'))]
                logger.debug(f"Download {download_id} candidates (new files): {candidates}")

                for candidate in sorted(candidates, key=lambda f: os.path.getmtime(os.path.join(temp_dir, f))):
                    full = os.path.join(temp_dir, candidate)
                    # Process any new candidate regardless of filename — user requested moving all new files
                    # If it's a .crdownload, wait until it's replaced by final file or disappears
                    if candidate.endswith('.crdownload'):
                        final_name = candidate[:-len('.crdownload')]
                        final_path = os.path.join(temp_dir, final_name)
                        inner_start = time.time()
                        while time.time() - inner_start < wait_time:
                            # Only proceed if final filename matches our expected id/name
                            if os.path.exists(final_path) and (file_id in final_name or expected_filename in final_name):
                                # ready to move
                                with self.download_lock:
                                    destination_filename = os.path.basename(final_path)
                                    destination_path = os.path.join(self.final_download_path, destination_filename)
                                    counter = 1
                                    while os.path.exists(destination_path):
                                        name, ext = os.path.splitext(destination_filename)
                                        destination_path = os.path.join(self.final_download_path, f"{name} ({counter}){ext}")
                                        counter += 1
                                    # Try moving with retries
                                    dest = self._try_move_file(final_path, self.final_download_path, max_attempts=4, wait_ready_timeout=20)
                                    if not dest:
                                        logger.warning(f"Failed to move final file after retries: {final_path}")
                                        continue
                                processed.add(candidate)
                                self._update_download_progress(download_id, 1, "moving")
                                logger.info(f"Moved completed download {final_path} -> {dest}")
                                # attempt to move any remaining matching files
                                try:
                                    self._attempt_post_move(file_id, expected_filename, start_time)
                                except Exception:
                                    pass
                                break
                            # if .crdownload disappeared but final file name different, pick newest mp3
                            if not os.path.exists(full):
                                # consider only mp3 files that match this download's id or expected filename
                                mps = [f for f in os.listdir(temp_dir) if f.lower().endswith('.mp3') and (file_id in f or expected_filename in f)]
                                if mps:
                                    candidates_mp3 = sorted(mps, key=lambda f: os.path.getmtime(os.path.join(temp_dir, f)), reverse=True)
                                    mp3_full = os.path.join(temp_dir, candidates_mp3[0])
                                    # Try moving detected mp3 with retries
                                    dest = self._try_move_file(mp3_full, self.final_download_path, max_attempts=4, wait_ready_timeout=20)
                                    if not dest:
                                        logger.warning(f"Failed to move detected mp3 after retries: {mp3_full}")
                                        continue
                                    processed.add(candidate)
                                    self._update_download_progress(download_id, 1, "moving")
                                    logger.info(f"Moved detected mp3 {mp3_full} -> {dest}")
                                    # attempt post move of any extras
                                    try:
                                        self._attempt_post_move(file_id, expected_filename, start_time)
                                    except Exception:
                                        pass
                                    break
                            time.sleep(0.5)
                    elif candidate.lower().endswith('.mp3'):
                        # ensure this mp3 matches this download
                        if not ((file_id in candidate) or (expected_filename in candidate)):
                            logger.debug(f"Skipping mp3 for download {download_id}: not matching: {candidate}")
                            continue
                        # ensure stable size
                        p = os.path.join(temp_dir, candidate)
                        try:
                            size1 = os.path.getsize(p)
                            time.sleep(0.8)
                            size2 = os.path.getsize(p)
                            if size1 == size2 and size1 > 0:
                                with self.download_lock:
                                    destination_filename = os.path.basename(p)
                                    destination_path = os.path.join(self.final_download_path, destination_filename)
                                    counter = 1
                                    while os.path.exists(destination_path):
                                        name, ext = os.path.splitext(destination_filename)
                                        destination_path = os.path.join(self.final_download_path, f"{name} ({counter}){ext}")
                                        counter += 1
                                    # Try moving mp3 with retries
                                    dest = self._try_move_file(p, self.final_download_path, max_attempts=4, wait_ready_timeout=20)
                                    if not dest:
                                        logger.warning(f"Failed to move mp3 after retries: {p}")
                                        continue
                                processed.add(candidate)
                                self._update_download_progress(download_id, 1, "moving")
                                logger.info(f"Moved mp3 {p} -> {dest}")
                                try:
                                    self._attempt_post_move(file_id, expected_filename, start_time)
                                except Exception:
                                    pass
                        except OSError:
                            pass

                if processed:
                    # mark completed and exit loop
                    self._update_status(f"✅ הורדה הושלמה: {shiur_title}")
                    self._update_download_progress(download_id, 1, "completed")
                    break

                time.sleep(0.5)

            if not processed:
                # final fallback: try to move any mp3 created since start_time that were not present before
                try:
                    candidates_all = [f for f in os.listdir(temp_dir) if f.lower().endswith('.mp3') and f not in before_files and os.path.getmtime(os.path.join(temp_dir, f)) >= start_time - 0.5]
                    logger.warning(f"Download {download_id} timed out strict matching; fallback ALL mp3 candidates (not in before_files): {candidates_all}")
                    if candidates_all:
                        # try moving each candidate (oldest-first) with retries
                        for fname in sorted(candidates_all, key=lambda f: os.path.getmtime(os.path.join(temp_dir, f))):
                            mp3_full = os.path.join(temp_dir, fname)
                            dest = self._try_move_file(mp3_full, self.final_download_path, max_attempts=6, wait_ready_timeout=30, attempt_delay=1.0)
                            if dest:
                                processed.add(fname)
                                self._update_download_progress(download_id, 1, "moving")
                                self._update_status(f"✅ הורדה הושלמה (fallback all): {shiur_title}")
                                logger.info(f"Fallback ALL moved mp3 {mp3_full} -> {dest} for download {download_id}")
                        # after attempting all, also try post-move cleanup for any other matching files
                        try:
                            self._attempt_post_move(file_id, expected_filename, start_time)
                        except Exception:
                            pass
                except Exception as e:
                    logger.error(f"Fallback ALL move failed for download {download_id}: {e}")

            if not processed:
                raise Exception("Download timed out or file not detected in temp directory")

        except Exception as e:
            logger.error(f"Download failed for shiur {shiur_id}: {e}")
            self._update_status(f"❌ הורדה נכשלה: {shiur_title}")
            self._update_download_progress(download_id, 0, "failed")
        
    def navigate_to_next_page(self):
        try:
            self._update_status("עובר לעמוד הבא...")
            logger.info("Navigating to next page.")
            next_button = self.driver.find_element(By.CSS_SELECTOR, "app-pagination-options .next:not(.disabled)")
            self._js_click(next_button)
            return self._handle_results_page()
        except NoSuchElementException:
            self._update_status("אין עמוד הבא.")
            logger.info("No next page button found.")
            return None

    def close_driver(self):
        if self.driver:
            logger.info("Closing webdriver.")
            self.driver.quit()
