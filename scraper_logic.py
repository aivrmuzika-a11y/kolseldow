# scraper_logic.py (FIXED - REMOVED CIRCULAR IMPORT)
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
import queue
import platform
import random
import subprocess
import tempfile

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

logger = logging.getLogger(__name__)

CODE_VALUE = "014510"
PASSWORD_VALUE = "077536"
RUN_HEADLESS = False
# Default: don't override the browser User-Agent unless configured.
# Some sites block specific UAs; prefer leaving the browser default or letting the
# user set a UA in config.ini under [Settings] user_agent = <UA string>
DEFAULT_USER_AGENT = None
# A small pool of realistic desktop user-agents to rotate when none is configured
USER_AGENT_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6340.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]
TOPICS_FILE = Path("topics.json")
LOGIN_URL = "https://www2.kolhalashon.com/#/login/%2FregularSite%2Fnew"
CONFIG_FILE = Path("config.ini")

def _create_webdriver_standalone(status_callback):
    status_callback("בודק הגדרות דרייבר...")
    config = configparser.ConfigParser()
    service = None
    if CONFIG_FILE.exists():
        config.read(CONFIG_FILE)
        if 'Paths' in config and 'driver_path' in config['Paths']:
            saved_path = config['Paths']['driver_path']
            if os.path.exists(saved_path):
                status_callback("משתמש בנתיב דרייבר שמור.")
                service = ChromeService(executable_path=saved_path, log_path=str(Path.cwd() / 'chromedriver.log'))
    if not service:
        try:
            status_callback("מנסה לאתר דרייבר אוטומטית...")
            driver_bin = ChromeDriverManager().install()
            service = ChromeService(executable_path=driver_bin, log_path=str(Path.cwd() / 'chromedriver.log'))
        except Exception as e:
            status_callback("איתור אוטומטי נכשל. יש לבחור קובץ דרייבר ידנית.")
            root = tk.Tk(); root.withdraw()
            file_types = [("All files", "*.*")] if platform.system() == "Darwin" else [("Executable files", "*.exe")]
            manual_path = filedialog.askopenfilename(title="אנא בחר את קובץ chromedriver", filetypes=file_types)
            root.destroy()
            if manual_path:
                config['Paths'] = {'driver_path': manual_path}
                with open(CONFIG_FILE, 'w') as configfile: config.write(configfile)
                service = ChromeService(executable_path=manual_path)
            else:
                status_callback("לא נבחר דרייבר. לא ניתן להמשיך.")
                return None
    status_callback("מפעיל את הדפדפן...")
    chrome_options = ChromeOptions()
    # Optional: allow the user to configure using their real Chrome profile
    # via config.ini under a [Profile] section. Keys:
    #   use_profile = true/false
    #   chrome_user_data_dir = C:\\Users\\you\\AppData\\Local\\Google\\Chrome\\User Data
    #   profile_dir_name = Default
    use_profile = False
    profile_user_data_dir = None
    profile_dir_name = None
    try:
        if CONFIG_FILE.exists():
            config.read(CONFIG_FILE)
            if 'Profile' in config:
                use_profile = config['Profile'].getboolean('use_profile', fallback=False)
                profile_user_data_dir = config['Profile'].get('chrome_user_data_dir', fallback=None)
                profile_dir_name = config['Profile'].get('profile_dir_name', fallback=None)
    except Exception:
        use_profile = False

    # Helper to detect running Chrome on Windows (avoids profile lock/corruption)
    def _is_chrome_running():
        try:
            if platform.system() == 'Windows':
                out = subprocess.check_output('tasklist /FI "IMAGENAME eq chrome.exe"', shell=True, text=True)
                return 'chrome.exe' in out
            else:
                # On non-windows we'll try pgrep
                out = subprocess.check_output(['pgrep', '-f', 'chrome'])
                return bool(out.strip())
        except Exception:
            # If detection fails, assume not running to avoid false positives
            return False

    if use_profile and profile_user_data_dir:
        # Expand environment vars and verify path exists
        profile_user_data_dir = os.path.expandvars(profile_user_data_dir)
        if not os.path.exists(profile_user_data_dir):
            status_callback("הנתיב לפרופיל לא נמצא — משתמש בפרופיל זמני במקום.")
            use_profile = False
        else:
            if _is_chrome_running():
                status_callback("יש לסגור את כל חלונות Chrome לפני שמתחברים לפרופיל האמיתי; סגור Chrome ונסה שוב.")
                return None
            # Attach user data dir and (optionally) select profile directory name
            chrome_options.add_argument(f"--user-data-dir={profile_user_data_dir}")
            if profile_dir_name:
                chrome_options.add_argument(f"--profile-directory={profile_dir_name}")
            status_callback(f"מנסה להשתמש בפרופיל Chrome: {profile_dir_name or 'Default'}")

    if RUN_HEADLESS: chrome_options.add_argument("--headless=new")
    # Allow overriding the user agent from config.ini (section [Settings], key user_agent).
    ua = None
    try:
        if CONFIG_FILE.exists():
            config.read(CONFIG_FILE)
            if 'Settings' in config and 'user_agent' in config['Settings']:
                ua = config['Settings']['user_agent']
    except Exception:
        ua = None
    # Fallback to DEFAULT_USER_AGENT if set; otherwise don't override UA so Chrome's default is used.
    if not ua:
        # pick a UA from pool if not configured and DEFAULT_USER_AGENT not set
        ua = DEFAULT_USER_AGENT or random.choice(USER_AGENT_POOL)
    if ua:
        chrome_options.add_argument(f"--user-agent={ua}")
        status_callback(f"מגדיר User-Agent: {ua[:60]}...")
    else:
        status_callback("לא שונה User-Agent — משתמש בברירת המפעל של Chrome.")
    # Disable some automation flags that make webdriver detectable
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    # Reduce detection surface
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--window-size=1200,900")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    temp_download_path = str(Path.home() / 'Downloads' / 'kol_halashon_temp')
    os.makedirs(temp_download_path, exist_ok=True)
    
    prefs = {
        "download.default_directory": temp_download_path,
        "profile.default_content_setting_values.automatic_downloads": 1
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Script that masks common webdriver fingerprints
    script = '''
    // navigator.webdriver
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    // chrome runtime
    window.chrome = window.chrome || { runtime: {} };
    // languages
    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
    // plugins (non-empty array)
    Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
    // permissions
    try {
        const origQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => {
            if (parameters && parameters.name === 'notifications') {
                return Promise.resolve({ state: Notification.permission });
            }
            return origQuery(parameters);
        };
    } catch (e) {}
    // prevent detection using toString checks
    try {
        const fnToString = Function.prototype.toString;
        const nativeToString = fnToString.call(fnToString);
        Function.prototype.toString = function() { return nativeToString; };
    } catch (e) {}
    '''

    # First try: undetected-chromedriver (if installed). If it fails, fall back to normal Selenium.
    try:
        import undetected_chromedriver as uc
        status_callback("מנסה undetected_chromedriver...")
        uc_options = uc.ChromeOptions()
        if RUN_HEADLESS:
            uc_options.add_argument("--headless=new")
        # If we have a UA chosen above, use it for uc as well
        try:
            if ua:
                uc_options.add_argument(f"--user-agent={ua}")
        except Exception:
            # ua might not be defined in some paths; ignore
            pass
        uc_options.add_argument("--disable-blink-features=AutomationControlled")
        uc_options.add_argument("--window-size=1200,900")
        uc_options.add_argument("--no-sandbox")
        uc_options.add_argument("--disable-dev-shm-usage")
        # If we decided to attach to the real profile above, propagate that to uc as well
        try:
            if use_profile and profile_user_data_dir:
                uc_options.add_argument(f"--user-data-dir={profile_user_data_dir}")
                if profile_dir_name:
                    uc_options.add_argument(f"--profile-directory={profile_dir_name}")
        except Exception:
            pass
        uc_options.add_experimental_option("prefs", prefs)
        driver = uc.Chrome(options=uc_options)
        try:
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': script})
        except Exception:
            pass
        return driver
    except Exception:
        status_callback("undetected_chromedriver לא זמין, ממשיך עם Selenium רגיל...")

    # Fallback: regular Selenium-backed Chrome
    driver = webdriver.Chrome(service=service, options=chrome_options)
    # Apply masking script to new documents
    try:
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': script})
    except Exception:
        pass

    # If we set a UA via chrome_options, also set it via CDP to ensure headers and navigator align
    try:
        if ua:
            try:
                driver.execute_cdp_cmd('Network.enable', {})
                driver.execute_cdp_cmd('Network.setUserAgentOverride', {'userAgent': ua})
            except Exception:
                # ignore failures in UA override
                pass
    except Exception:
        pass

    return driver

def initial_login(status_callback):
    driver = _create_webdriver_standalone(status_callback)
    if not driver: return None
    status_callback("מתחיל תהליך התחברות...")
    # Navigate to login page
    driver.get(LOGIN_URL)
    # small pause so the page can finish scripts/redirects and look more human
    time.sleep(2)
    try:
        # ensure window is focused and input is clickable (helps when the page isn't active)
        try:
            driver.execute_script('window.focus();')
        except Exception:
            pass
        # type the code/password more like a human (per-character)
        def _human_type(element, text, delay_min=0.04, delay_max=0.12):
            for ch in str(text):
                element.send_keys(ch)
                time.sleep(random.uniform(delay_min, delay_max))
        code_input = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input[formcontrolname='code']")))
        try:
            code_input.click()
        except Exception:
            pass
        _human_type(code_input, CODE_VALUE)
        password_input = driver.find_element(By.CSS_SELECTOR, "input[formcontrolname='password']")
        try:
            password_input.click()
        except Exception:
            pass
        _human_type(password_input, PASSWORD_VALUE)
        # Try clicking the page's submit/login button if available (more robust than ENTER)
        try:
            # common patterns: button[type=submit], .login-button, input[type=submit]
            btn = None
            for sel in ["button[type='submit']", "button.login-button", "input[type='submit']"]:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    btn = els[0]
                    break
            if btn:
                try:
                    btn.click()
                except Exception:
                    # fallback to sending ENTER
                    password_input.send_keys(webdriver.common.keys.Keys.ENTER)
            else:
                password_input.send_keys(webdriver.common.keys.Keys.ENTER)
        except Exception:
            password_input.send_keys(webdriver.common.keys.Keys.ENTER)
        WebDriverWait(driver, 25).until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".banner-search input, .banner-title input")))
        status_callback("✅ התחברות בוצעה בהצלחה.")
        return driver
    except Exception as e:
        status_callback(f"❌ שגיאה: התחברות נכשלה. {e}")
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
        self.driver_lock = threading.Lock()

        self.download_queue = queue.Queue()
        # --- NEW: The "message board" to link file IDs to download IDs ---
        self.active_downloads = {}
        self.monitor_lock = threading.Lock()

        self.download_worker_thread = threading.Thread(target=self._download_worker, daemon=True)
        self.file_monitor_thread = threading.Thread(target=self._file_monitor, daemon=True)
        self.download_worker_thread.start()
        self.file_monitor_thread.start()

    def set_final_download_path(self, path):
        target = os.path.join(path, "קול הלשון")
        os.makedirs(target, exist_ok=True)
        self.final_download_path = target
        self._update_status(f"ההורדות יישמרו ב: {self.final_download_path}")

    def _update_status(self, message):
        if self.status_callback: self.status_callback(message)
            
    def _update_download_progress(self, did, prog, stat):
        if self.download_progress_callback: self.download_progress_callback(did, prog, stat)

    def _js_click(self, element):
        self.driver.execute_script("arguments[0].click();", element)

    def _wait_for_file_ready(self, path, timeout=15):
        start_time = time.time()
        while time.time() - start_time < timeout:
            if os.path.exists(path):
                try:
                    with open(path, 'rb') as f: f.read(1)
                    return True
                except (IOError, PermissionError): time.sleep(0.5)
            else: time.sleep(0.5)
        return False

    def _try_move_file(self, src_path, dest_dir, max_attempts=5, wait_timeout=20):
        if not self._wait_for_file_ready(src_path, timeout=wait_timeout):
            logger.warning(f"File never became ready: {src_path}")
            return None
        filename = os.path.basename(src_path)
        for attempt in range(max_attempts):
            try:
                name, ext = os.path.splitext(filename)
                candidate = os.path.join(dest_dir, filename)
                counter = 1
                while os.path.exists(candidate):
                    candidate = os.path.join(dest_dir, f"{name} ({counter}){ext}")
                    counter += 1
                shutil.move(src_path, candidate)
                logger.info(f"Successfully moved {src_path} to {candidate}")
                return candidate
            except Exception as e:
                logger.warning(f"Move attempt {attempt+1} failed for {src_path}: {e}")
                time.sleep(1)
        return None

    def load_topics_from_file(self):
        if self.topics_data: return self.topics_data
        if not TOPICS_FILE.exists(): return None
        with open(TOPICS_FILE, 'r', encoding='utf-8') as f:
            self.topics_data = json.load(f)
        return self.topics_data

    def get_initial_page_data(self):
        self._update_status("טוען נתונים ראשוניים...")
        try:
            script = """
            const shiurim = Array.from(document.querySelectorAll('app-shiurim-display .shiur-container')).map((el, i) => ({
                id: i,
                title: el.querySelector('.shiurim-title')?.textContent.trim() || '',
                rav: el.querySelector('.shiurim-rav-name')?.textContent.trim() || '',
                date: el.querySelector('.shiurim-start-time')?.textContent.trim() || ''
            }));
            const filter_categories = Array.from(document.querySelectorAll('app-filter-container .filter-header'))
                .map(header => header.textContent.trim()).filter(Boolean);
            return { shiurim, filter_categories };
            """
            return self.driver.execute_script(script)
        except Exception as e:
            logger.error(f"Failed to get initial page data: {e}")
            return {'shiurim': [], 'filter_categories': []}

    def expand_and_get_all_filters(self):
        self._update_status("מרחיב מסננים ברקע...")
        try:
            self.driver.execute_script("""
                document.querySelectorAll('app-filter-container .filter-header').forEach(h => {
                    const c = h.closest('app-filter-container').querySelector('.filter-container');
                    if (c && !c.classList.contains('opened')) h.click();
                });
            """)
            time.sleep(0.3)
            for i in range(20):
                self._update_status(f"מרחיב מסננים... (שלב {i+1})")
                clicked_something = self.driver.execute_script("""
                    let clicked = false;
                    const showMoreButtons = Array.from(document.querySelectorAll(".display-more"))
                                                .filter(btn => btn.textContent.includes('הצג עוד') && btn.offsetParent);
                    if (showMoreButtons.length > 0) { showMoreButtons.forEach(btn => btn.click()); clicked = true; }
                    const closedArrowButtons = Array.from(document.querySelectorAll(".nested-filter-container:not(.expanded-nested-filter-container) .icon-nav-arrow, .scroll-container > .nested-filter-container:not(.expanded-nested-filter-container) .icon-nav-arrow"))
                                                   .filter(arrow => arrow.offsetParent);
                    if (closedArrowButtons.length > 0) { closedArrowButtons.forEach(arrow => arrow.click()); clicked = true; }
                    return clicked;
                """)
                if not clicked_something: break
                time.sleep(0.8)
            self._update_status("אוסף את רשימת המסננים...")
            filters_data = self.driver.execute_script("""
            function getElementText(el) {
                const title = el.querySelector('.filter-title')?.textContent.trim() || '';
                const count = el.querySelector('.shiurim-count')?.textContent.trim() || '';
                if (!title && el.classList.contains('mat-checkbox')) { return el.textContent.trim(); }
                return `${title} ${count}`.trim();
            }
            function parseContainer(container, level) {
                let results = [];
                const children = container.querySelectorAll(':scope > .nested-flex-display, :scope > mat-checkbox.filter-option, :scope > .nested-filter-container, :scope > div > mat-checkbox.filter-option');
                children.forEach(child => {
                    if (child.matches('mat-checkbox.filter-option, .nested-flex-display')) {
                        const text = getElementText(child);
                        if (text) results.push({ text: text, level: level });
                    } else if (child.matches('.nested-filter-container')) {
                        results = results.concat(parseContainer(child, level + 1));
                    }
                });
                return results;
            }
            const topLevelContainers = document.querySelectorAll('app-filter-container');
            let allFilters = [];
            topLevelContainers.forEach(topContainer => {
                const categoryName = topContainer.querySelector('.filter-header')?.textContent.trim();
                if (!categoryName) return;
                allFilters.push({ text: categoryName, level: -1 });
                const content = topContainer.querySelector('.filter-content > .scroll-container, .filter-content');
                if (content) allFilters = allFilters.concat(parseContainer(content, 0));
            });
            return allFilters;
            """)
            self._update_status("טעינת המסננים הושלמה.")
            return filters_data
        except Exception as e:
            self._update_status("שגיאה בטעינת המסננים.")
            logger.error(f"Failed to expand and get filters: {e}", exc_info=True)
            return []

    def _handle_results_page(self):
        self._update_status("ממתין לטעינת העמוד...")
        try:
            WebDriverWait(self.driver, 15).until(EC.any_of(
                EC.presence_of_element_located((By.CSS_SELECTOR, "app-shiurim-display .shiur-container")),
                EC.presence_of_element_located((By.CSS_SELECTOR, ".rav-container"))
            ))
            time.sleep(1)
            if self.driver.find_elements(By.CSS_SELECTOR, ".rav-container"):
                return {'type': 'rav_selection', 'data': self.driver.execute_script("""
                    return Array.from(document.querySelectorAll('.rav-container')).map((el, i) => ({
                        id: i, name: el.querySelector('.rav-name')?.textContent.trim(),
                        count: el.querySelector('.rav-shiurim-sum')?.textContent.trim()
                    }));""")}
            if self.driver.find_elements(By.CSS_SELECTOR, "app-shiurim-display .shiur-container"):
                try:
                    WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "app-filter-container")))
                except TimeoutException:
                    logger.warning("Filter container did not appear in time.")
                return {'type': 'initial_data', 'data': self.get_initial_page_data()}
            return {'type': 'error', 'message': 'לא נמצא תוכן מתאים.'}
        except TimeoutException:
            return {'type': 'error', 'message': 'העמוד לא נטען בזמן.'}

    def refresh_browser_page(self):
        self._update_status("מרענן את הדף...")
        self.driver.refresh()
        return self._handle_results_page()

    def refresh_current_page_content(self):
        self._update_status("טוען נתונים מחדש...")
        return self._handle_results_page()

    def perform_search(self, query: str):
        self._update_status(f"חיפוש: '{query}'...")
        search_type = "ravSearch" if query.strip().startswith("הרב") else "searchResults"
        self.driver.get(f"https://www2.kolhalashon.com/#/regularSite/{search_type}/{urllib.parse.quote(query)}")
        return self._handle_results_page()

    def navigate_to_topic_by_href(self, href: str):
        self._update_status("מנווט לקטגוריה...")
        self.driver.get(href)
        return self._handle_results_page()

    def select_rav_from_results(self, rav_id: int):
        self._update_status("בוחר רב...")
        with self.driver_lock:
            rav_links = self.driver.find_elements(By.CSS_SELECTOR, ".rav-container a.rav-name")
            if rav_id < len(rav_links): self._js_click(rav_links[rav_id])
            else: return {'type': 'error', 'message': 'הרב לא נמצא.'}
        return self._handle_results_page()

    def apply_filter_by_name(self, filter_name: str):
        self._update_status(f"מפעיל מסנן: {filter_name}...")
        try:
            with self.driver_lock:
                first_shiur = self.driver.find_element(By.CSS_SELECTOR, "app-shiurim-display .shiur-container")
                self.driver.execute_script("""
                    const filterText = arguments[0];
                    for (const cb of document.querySelectorAll('mat-checkbox')) {
                        const labelContent = Array.from(cb.querySelectorAll('.filter-title, .shiurim-count'))
                                                  .map(el => el.textContent.trim()).join(' ').trim();
                        if (labelContent === filterText || cb.textContent.trim() === filterText) {
                            cb.querySelector('input').click(); return;
                        }
                    }
                """, filter_name)
                WebDriverWait(self.driver, 20).until(EC.staleness_of(first_shiur))
            return self._handle_results_page()
        except Exception as e:
            return {'type': 'error', 'message': f'שגיאה בהפעלת המסנן: {e}'}

    def queue_download(self, shiur_id, title, did):
        self.download_queue.put({'shiur_id': shiur_id, 'title': title, 'did': did})
        self._update_download_progress(did, 0, "starting")

    def _download_worker(self):
        while True:
            task = self.download_queue.get()
            shiur_id, title, did = task['shiur_id'], task['title'], task['did']
            
            self._update_status(f"מתחיל הורדה: {title}")
            try:
                with self.driver_lock:
                    shiur_elements = self.driver.find_elements(By.CSS_SELECTOR, "app-shiurim-display .shiur-container")
                    if shiur_id >= len(shiur_elements):
                        raise IndexError("Shiur ID out of bounds")
                    
                    try:
                        phone_button = shiur_elements[shiur_id].find_element(By.XPATH, ".//button[contains(@class, 'click-phone-button')]")
                        file_id = phone_button.text.strip()
                        if file_id:
                            with self.monitor_lock:
                                self.active_downloads[file_id] = did
                                logger.info(f"Registered download: file_id {file_id} maps to did {did}")
                    except NoSuchElementException:
                        logger.warning(f"Could not find file_id for {title}. UI update will not work for this download.")

                    download_button = shiur_elements[shiur_id].find_element(By.XPATH, ".//button[.//svg-icon[contains(@src, 'download-i.svg')]]")
                    self._js_click(download_button)
                    
                    try:
                        audio_option = WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'download-option')]")))
                        self._js_click(audio_option)
                    except TimeoutException:
                        pass
                    
                    time.sleep(1.5) 
            except Exception as e:
                logger.error(f"Failed to initiate download for {title}: {e}")
                self._update_download_progress(did, 0, "failed")
            
            self.download_queue.task_done()
            time.sleep(1)

    def _file_monitor(self):
        processed_files = set()
        while True:
            try:
                all_files = set(os.listdir(self.temp_download_path))
                completed_files = [f for f in all_files if not f.endswith(('.crdownload', '.tmp')) and f not in processed_files]

                for fname in completed_files:
                    full_path = os.path.join(self.temp_download_path, fname)
                    processed_files.add(fname)
                    
                    did_to_update = None
                    file_id_found = None
                    with self.monitor_lock:
                        for file_id, did in self.active_downloads.items():
                            if file_id in fname:
                                did_to_update = did
                                file_id_found = file_id
                                break
                    
                    if self._try_move_file(full_path, self.final_download_path):
                        logger.info(f"Moved downloaded file: {fname}")
                        if did_to_update:
                            self._update_download_progress(did_to_update, 1, "completed")
                            with self.monitor_lock:
                                del self.active_downloads[file_id_found]
                    else:
                        logger.error(f"Failed to move {fname} from temp folder.")
                        if did_to_update:
                            self._update_download_progress(did_to_update, 0, "failed")
                            with self.monitor_lock:
                                del self.active_downloads[file_id_found]
                
                if len(processed_files) > 100:
                    processed_files.clear()

            except Exception as e:
                logger.error(f"Error in file monitor: {e}")
            
            time.sleep(2)

    def navigate_to_next_page(self):
        try:
            self._update_status("עובר לעמוד הבא...")
            with self.driver_lock:
                next_button = self.driver.find_element(By.CSS_SELECTOR, "app-pagination-options .next:not(.disabled)")
                self._js_click(next_button)
            return self._handle_results_page()
        except NoSuchElementException:
            self._update_status("אין עמוד הבא.")
            return None

    def close_driver(self):
        if self.driver: self.driver.quit()
