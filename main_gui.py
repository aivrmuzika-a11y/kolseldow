# main_gui.py
import customtkinter as ctk
import threading
import time
from scraper_logic import Scraper, initial_login
from tkinter import Menu
import platform
import logging
from pathlib import Path
import os

if platform.system() == "Windows":
    try:
        from win32api import GetLogicalDriveStrings, GetVolumeInformation
    except ImportError:
        pass
    
def rtl_fix(text):
    text_str = str(text)
    if not text_str: return ""
    words = text_str.split()
    reversed_words = words[::-1]
    return " ".join(reversed_words)

def setup_logging():
    log_path = Path.home() / 'kol_halashon_app.log'
    log_formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    try:
        file_handler = logging.FileHandler(str(log_path), encoding='utf-8')
        file_handler.setFormatter(log_formatter)
        root_logger.addHandler(file_handler)
    except Exception as e:
        print(f"Failed to set up file logging: {e}")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_formatter)
    root_logger.addHandler(stream_handler)

logger = logging.getLogger(__name__)

class App(ctk.CTk):
    def __init__(self, driver):
        super().__init__()
        
        ctk.set_appearance_mode("Light")
        ctk.set_default_color_theme("blue")

        self.title("קול הלשון")
        self.geometry("1200x750")

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(1, weight=1)
        
        self.scraper = Scraper(driver, status_callback=self.safe_update_status, download_progress_callback=self.safe_update_download_progress)
        self.is_logged_in = True
        self.topics_data = None
        self.active_filters = set()
        
        self.filter_checkboxes = []
        self.original_checkbox_text = {}
        self.download_widgets = {}

        self.create_widgets()
        
        self.after(100, self.initialize_backend)

    def initialize_backend(self):
        self.start_drive_refresh()
        self.run_in_thread(self.scraper.load_topics_from_file, self.on_topics_loaded)
        self.safe_update_status("מוכן. בצע חיפוש או בחר קטגוריה.")

    def create_widgets(self):
        self.status_bar = ctk.CTkLabel(self, text=rtl_fix("טוען..."), anchor="e", height=25)
        self.status_bar.grid(row=4, column=0, columnspan=2, sticky="ew", padx=10, pady=(0,5))
        
        self.bottom_frame = ctk.CTkFrame(self)
        self.bottom_frame.grid(row=2, column=0, columnspan=2, sticky="ew")
        self.bottom_frame.grid_columnconfigure(1, weight=1)

        self.progress_bar = ctk.CTkProgressBar(self.bottom_frame, orientation="horizontal", mode="indeterminate")

        self.top_frame = ctk.CTkFrame(self, height=50, corner_radius=0)
        self.top_frame.grid(row=0, column=0, columnspan=2, sticky="ew")
        self.top_frame.grid_columnconfigure(4, weight=1)
        self.top_frame.grid_rowconfigure(1, weight=1)

        instruction_label = ctk.CTkLabel(self.top_frame, text=rtl_fix("לחיפוש רבנים יש להוסיף 'הרב' בתחילת החיפוש"), font=ctk.CTkFont(size=11), text_color="gray50")
        instruction_label.grid(row=0, column=4, sticky="se", padx=(10,5), pady=(2,0))

        self.search_entry = ctk.CTkEntry(self.top_frame, placeholder_text=rtl_fix("הזן לחיפוש..."), justify="right")
        self.search_entry.grid(row=1, column=4, padx=(10, 5), pady=(0, 10), sticky="ew")
        self.search_entry.bind("<Return>", self.start_search)
        
        self.search_button = ctk.CTkButton(self.top_frame, text=rtl_fix("חיפוש"), width=100, command=self.start_search)
        self.search_button.grid(row=1, column=3, padx=(5, 5), pady=(0, 10))
        
        self.reload_button = ctk.CTkButton(self.top_frame, text=rtl_fix("רענן דף"), width=100, command=self.start_browser_refresh)
        self.reload_button.grid(row=1, column=2, padx=(5, 5), pady=(0, 10))
        
        self.re_extract_button = ctk.CTkButton(self.top_frame, text=rtl_fix("טען מחדש"), width=100, command=self.start_content_refresh)
        self.re_extract_button.grid(row=1, column=1, padx=(5, 5), pady=(0, 10))
        
        self.categories_button = ctk.CTkButton(self.top_frame, text=rtl_fix("קטגוריות"), width=120)
        self.categories_button.grid(row=1, column=0, padx=(10, 5), pady=(0, 10))
        self.categories_menu = Menu(self.categories_button, tearoff=0)
        self.categories_button.configure(command=self.show_categories_menu)

        self.left_panel = ctk.CTkTabview(self, width=300)
        self.left_panel.grid(row=1, column=0, sticky="ns", padx=10, pady=10)
        self.left_panel.add(rtl_fix("מסננים"))
        self.left_panel.add(rtl_fix("הורדות"))
        
        filters_tab = self.left_panel.tab(rtl_fix("מסננים"))
        filters_tab.grid_rowconfigure(2, weight=1)
        filters_tab.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(filters_tab, text=rtl_fix("מסננים זמינים"), font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, padx=10, pady=(10,5), sticky="ew")
        self.filter_search_entry = ctk.CTkEntry(filters_tab, placeholder_text=rtl_fix("מצא מסנן..."), justify="right")
        self.filter_search_entry.grid(row=1, column=0, padx=10, pady=5, sticky="ew")
        self.filter_search_entry.bind("<KeyRelease>", self.filter_checkbox_list)
        self.filters_scroll_frame = ctk.CTkScrollableFrame(filters_tab, label_text="")
        self.filters_scroll_frame.grid(row=2, column=0, sticky="nsew", padx=10, pady=10)

        self.downloads_tab = self.left_panel.tab(rtl_fix("הורדות"))
        self.downloads_scroll_frame = ctk.CTkScrollableFrame(self.downloads_tab, label_text=rtl_fix("הורדות פעילות"))
        self.downloads_scroll_frame.pack(expand=True, fill="both", padx=5, pady=5)

        self.results_outer_frame = ctk.CTkFrame(self)
        self.results_outer_frame.grid(row=1, column=1, sticky="nsew", padx=(0, 10), pady=5)
        self.results_outer_frame.grid_rowconfigure(1, weight=1)
        self.results_outer_frame.grid_columnconfigure(0, weight=1)
        
        self.active_filters_frame = ctk.CTkFrame(self.results_outer_frame, fg_color="transparent")
        self.active_filters_frame.grid(row=0, column=0, sticky="ew", padx=5, pady=0)
        self.results_frame = ctk.CTkScrollableFrame(self.results_outer_frame, label_text=rtl_fix("תוצאות"))
        self.results_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=0)
        
        self.pagination_frame = ctk.CTkFrame(self.bottom_frame)
        self.pagination_frame.grid(row=0, column=1, sticky="ew", padx=10, pady=(0, 10))
        self.pagination_frame.grid_columnconfigure(1, weight=1)
        
        self.drive_selector_frame = ctk.CTkFrame(self.pagination_frame, fg_color="transparent")
        self.drive_selector_frame.grid(row=0, column=0, padx=10, pady=5)
        ctk.CTkLabel(self.drive_selector_frame, text=rtl_fix("שמור ב:")).pack(side="right")
        self.setup_drive_selector()
        
        self.next_page_button = ctk.CTkButton(self.pagination_frame, text=rtl_fix("העמוד הבא ->"), command=self.go_to_next_page, state="disabled")
        self.next_page_button.grid(row=0, column=1, padx=10, pady=5)

    def get_drives(self):
        drives = []
        drive_map = {}
        home_path = str(Path.home())
        drives.append(home_path)
        drive_map[home_path] = home_path
        if platform.system() == "Windows":
            # Prefer win32api if available (pywin32). Otherwise fall back to ctypes-based detection so
            # the app can list drives without requiring extra packages.
            try:
                try:
                    import win32api
                    drive_str = win32api.GetLogicalDriveStrings()
                    raw_drives = [d for d in drive_str.split('\000') if d]
                    for d in raw_drives:
                        try:
                            volume_name = win32api.GetVolumeInformation(d)[0] or ''
                            display_name = f"{volume_name} ({d.strip()})" if volume_name else d.strip()
                            drives.append(display_name)
                            drive_map[display_name] = d
                        except Exception:
                            drives.append(d)
                            drive_map[d] = d
                except Exception:
                    # ctypes fallback (no pywin32 installed)
                    import ctypes
                    kernel32 = ctypes.windll.kernel32
                    buf = ctypes.create_unicode_buffer(260)
                    res = kernel32.GetLogicalDriveStringsW(260, buf)
                    if res and res > 0:
                        raw_drives = [d for d in buf.value.split('\x00') if d]
                    else:
                        raw_drives = []
                    for d in raw_drives:
                        try:
                            vol_name_buf = ctypes.create_unicode_buffer(260)
                            fs_name_buf = ctypes.create_unicode_buffer(260)
                            serial = ctypes.c_uint()
                            max_comp = ctypes.c_uint()
                            flags = ctypes.c_uint()
                            kernel32.GetVolumeInformationW(ctypes.c_wchar_p(d), vol_name_buf, ctypes.sizeof(vol_name_buf), ctypes.byref(serial), ctypes.byref(max_comp), ctypes.byref(flags), fs_name_buf, ctypes.sizeof(fs_name_buf))
                            volume_name = vol_name_buf.value or ''
                        except Exception:
                            volume_name = ''
                        display_name = f"{volume_name} ({d.strip()})" if volume_name else d.strip()
                        drives.append(display_name)
                        drive_map[display_name] = d
            except Exception as e:
                logger.error(f"Windows drive detection error: {e}")
        elif platform.system() == "Darwin":
            try:
                volumes_path = "/Volumes"
                if os.path.exists(volumes_path):
                    for volume in os.listdir(volumes_path):
                        full_path = os.path.join(volumes_path, volume)
                        if os.path.isdir(full_path) and not os.path.islink(full_path) and volume not in drives:
                            drives.append(volume)
                            drive_map[volume] = full_path
            except Exception as e:
                logger.error(f"macOS volume error: {e}")
        
        return drives, drive_map

    def setup_drive_selector(self):
        self.drive_option_menu = ctk.CTkOptionMenu(self.drive_selector_frame, values=[rtl_fix("טוען...")], command=self.on_drive_selected)
        self.drive_option_menu.pack(side="right", padx=5)

        refresh_drive_button = ctk.CTkButton(self.drive_selector_frame, text="🔄", width=28, height=28, command=self.start_drive_refresh)
        refresh_drive_button.pack(side="right", padx=(0, 5))

    def start_drive_refresh(self):
        threading.Thread(target=self.refresh_drives_async, daemon=True).start()

    def refresh_drives_async(self):
        drives, drive_map = self.get_drives()
        self.after(0, lambda: self.update_drive_menu(drives, drive_map))

    def update_drive_menu(self, drives, drive_map):
        self.drive_map = drive_map
        display_drives = [rtl_fix(d) for d in drives]
        self.display_to_key_map = {rtl_fix(k): k for k in drives}
        
        self.drive_option_menu.configure(values=display_drives)
        if display_drives:
            self.drive_option_menu.set(display_drives[0])
            self.on_drive_selected(display_drives[0])
        self.safe_update_status("רשימת מיקומים עודכנה.")

    def on_drive_selected(self, selected_display_name):
        original_key = self.display_to_key_map.get(selected_display_name)
        if original_key:
            path = self.drive_map.get(original_key)
            if path:
                # Set scraper final path (will create subfolder 'קול הלשון')
                self.scraper.set_final_download_path(path)
                # Reset download UI/state whenever the download path changes
                try:
                    # clear active download widgets UI
                    for w in list(self.downloads_scroll_frame.winfo_children()):
                        w.destroy()
                except Exception:
                    pass
                self.download_widgets.clear()
                # Populate current files in target folder (newest first)
                self.refresh_downloads_folder_display()

    def refresh_downloads_folder_display(self):
        # Show files present in final download folder (newest first)
        try:
            target = getattr(self.scraper, 'final_download_path', None)
            if not target or not os.path.exists(target):
                return
            files = [f for f in os.listdir(target) if os.path.isfile(os.path.join(target, f))]
            # sort by mtime desc (newest first)
            files.sort(key=lambda f: os.path.getmtime(os.path.join(target, f)), reverse=True)

            for fname in files:
                frame = ctk.CTkFrame(self.downloads_scroll_frame)
                frame.pack(fill="x", padx=5, pady=2)
                label = ctk.CTkLabel(frame, text=rtl_fix(fname), font=ctk.CTkFont(size=11))
                label.pack(side="right", padx=5, pady=2)
        except Exception as e:
            logger.error(f"Error refreshing downloads folder display: {e}")

    def _ui_start_loading(self):
        self.progress_bar.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(5,0))
        self.progress_bar.start()
        self.set_ui_state("disabled")

    def _ui_stop_loading(self):
        self.set_ui_state("normal")
        self.progress_bar.stop()
        self.progress_bar.grid_forget()

    def _ui_update_status_text(self, message):
        self.status_bar.configure(text=rtl_fix(message))

    def safe_update_status(self, message):
        logger.info(f"Status: {message}")
        self.after(0, lambda: self._ui_update_status_text(message))

    def safe_update_download_progress(self, download_id, progress, status):
        self.after(0, self.update_download_widget, download_id, progress, status)

    def run_in_thread(self, target_func, callback=None, *args):
        self.after(0, self._ui_start_loading)
        def thread_target():
            try:
                result = target_func(*args)
                if callback:
                    self.after(0, lambda: callback(result))
            except Exception as e:
                logger.error(f"Error in thread: {e}", exc_info=True)
                self.safe_update_status(f"❌ שגיאה. בדוק לוג.")
            finally:
                self.after(0, self._ui_stop_loading)
        threading.Thread(target=thread_target, daemon=True).start()

    def set_ui_state(self, state):
        self.search_button.configure(state=state)
        self.categories_button.configure(state=state)
        self.reload_button.configure(state="normal")
        self.re_extract_button.configure(state="normal")

    def on_topics_loaded(self, topics):
        self.topics_data = topics
        self.build_categories_menu()

    def build_categories_menu(self):
        if not self.topics_data: return
        try:
            self.categories_menu.delete(0, 'end')
        except: pass
        for main_cat_name, sub_cats in self.topics_data.items():
            sub_menu = Menu(self.categories_menu, tearoff=0)
            for sub_cat in sub_cats:
                cmd = lambda h=sub_cat['href']: self.run_in_thread(self.scraper.navigate_to_topic_by_href, self.handle_results, h)
                sub_menu.add_command(label=rtl_fix(sub_cat['name']), command=cmd)
            self.categories_menu.add_cascade(label=rtl_fix(main_cat_name), menu=sub_menu)

    def show_categories_menu(self):
        self.categories_menu.tk_popup(self.categories_button.winfo_rootx(), self.categories_button.winfo_rooty() + self.categories_button.winfo_height())

    def start_search(self, event=None):
        query = self.search_entry.get()
        if query and self.is_logged_in:
            self.run_in_thread(self.scraper.perform_search, self.handle_results, query)

    def start_browser_refresh(self):
        if not self.is_logged_in: return
        self.run_in_thread(self.scraper.refresh_browser_page, self.handle_results)

    def start_content_refresh(self):
        if not self.is_logged_in: return
        self.run_in_thread(self.scraper.refresh_current_page_content, self.handle_results)

    def _clear_ui_elements(self):
        for widget in self.results_frame.winfo_children(): widget.destroy()
        for widget in self.filters_scroll_frame.winfo_children(): widget.destroy()
        self.update_active_filters_display()

    def handle_results(self, result):
        self._clear_ui_elements()
        self.filter_checkboxes = []
        self.original_checkbox_text = {}
        self.next_page_button.configure(state="disabled")

        if not result: return
        
        result_type = result.get('type')
        
        if result_type == 'error':
            msg = result.get('message', 'שגיאה')
            ctk.CTkLabel(self.results_frame, text=rtl_fix(f"שגיאה: {msg}"), text_color="red").pack(pady=10)
            
        elif result_type == 'rav_selection':
            ctk.CTkLabel(self.results_frame, text=rtl_fix("בחר רב:")).pack(pady=5)
            data = result.get('data')
            for rav in data:
                cmd = lambda r=rav['id']: self.run_in_thread(self.scraper.select_rav_from_results, self.handle_results, r)
                txt = f"{rav['name']} ({rav['count']})"
                ctk.CTkButton(self.results_frame, text=rtl_fix(txt), command=cmd).pack(fill="x", padx=10, pady=2)
                
        elif result_type == 'shiurim_and_filters':
            data = result.get('data')
            self.populate_results(data.get('shiurim', []))
            self.populate_filters(data.get('filters', []))

    def populate_results(self, shiurim_list):
        if not shiurim_list:
            ctk.CTkLabel(self.results_frame, text=rtl_fix("לא נמצאו שיעורים.")).pack(pady=20)
            return
            
        self.next_page_button.configure(state="normal")
        
        for shiur in shiurim_list:
            frame = ctk.CTkFrame(self.results_frame)
            frame.pack(fill="x", padx=5, pady=4)
            frame.grid_columnconfigure(0, weight=1)
            
            details = ctk.CTkFrame(frame, fg_color="transparent")
            details.grid(row=0, column=0, sticky="ew", padx=10, pady=5)
            
            ctk.CTkLabel(details, text=rtl_fix(shiur['title']), justify="right", font=ctk.CTkFont(weight="bold")).pack(fill="x", anchor="e")
            ctk.CTkLabel(details, text=rtl_fix(f"{shiur['rav']} | {shiur['date']}"), justify="right", font=ctk.CTkFont(size=11)).pack(fill="x", anchor="e")
            
            dl_cmd = lambda s_id=shiur['id'], s_title=shiur['title']: self.start_download(s_id, s_title)
            ctk.CTkButton(frame, text=rtl_fix("הורדה"), width=80, command=dl_cmd).grid(row=0, column=1, padx=10)

    def start_download(self, shiur_id, shiur_title):
        download_id = f"{shiur_id}_{int(time.time())}"
        
        download_frame = ctk.CTkFrame(self.downloads_scroll_frame)
        download_frame.pack(fill="x", padx=5, pady=3)
        
        label_text = shiur_title[:30] + "..." if len(shiur_title) > 30 else shiur_title
        label = ctk.CTkLabel(download_frame, text=rtl_fix(label_text), font=ctk.CTkFont(size=11))
        label.pack(fill="x", padx=5, pady=(2,0))
        
        progress = ctk.CTkProgressBar(download_frame, orientation="horizontal")
        progress.set(0)
        progress.pack(fill="x", padx=5, pady=(0,2))
        
        self.download_widgets[download_id] = {'frame': download_frame, 'progress': progress, 'label': label}
        
        threading.Thread(target=lambda: self.scraper.download_shiur_by_id(shiur_id, shiur_title), daemon=True).start()
        
        self.left_panel.set(rtl_fix("הורדות"))

    def update_download_widget(self, download_id, progress_value, status):
        if download_id in self.download_widgets:
            widget_info = self.download_widgets[download_id]
            progress_bar = widget_info['progress']
            label = widget_info['label']

            if status == "starting":
                progress_bar.configure(mode="indeterminate")
                progress_bar.start()
            elif status == "moving":
                progress_bar.stop()
                progress_bar.configure(mode="determinate")
                progress_bar.set(1)
            elif status == "completed":
                progress_bar.set(1)
                progress_bar.configure(progress_color="green")
                label.configure(text=f"✅ {label.cget('text')}")
            elif status == "failed":
                progress_bar.stop()
                progress_bar.configure(mode="determinate", progress_color="red")
                progress_bar.set(1)
                label.configure(text=f"❌ {label.cget('text')}")

    def populate_filters(self, filters_data):
        VISIBLE = 5
        if not filters_data:
            ctk.CTkLabel(self.filters_scroll_frame, text=rtl_fix("אין מסננים")).pack(pady=10)
            return
        
        for cat in filters_data:
            ctk.CTkLabel(self.filters_scroll_frame, text=rtl_fix(cat['category_name']), font=ctk.CTkFont(weight="bold")).pack(fill="x", pady=(10, 2), padx=5, anchor="e")
            
            hidden = []
            for i, fname in enumerate(cat['filters']):
                var = ctk.StringVar(value="on" if fname in self.active_filters else "off")
                cmd = lambda n=fname, v=var: self.on_filter_toggled(n, v.get())
                cb = ctk.CTkCheckBox(self.filters_scroll_frame, text=rtl_fix(fname), variable=var, onvalue="on", offvalue="off", command=cmd)
                
                self.filter_checkboxes.append(cb)
                self.original_checkbox_text[cb] = fname
                
                if i < VISIBLE or fname in self.active_filters:
                    cb.pack(fill="x", padx=10, pady=2, anchor="e")
                else:
                    hidden.append(cb)
            
            if hidden:
                btn_txt = rtl_fix("הצג עוד")
                btn = ctk.CTkButton(self.filters_scroll_frame, text=btn_txt, fg_color="transparent", text_color="gray", height=20)
                btn_cmd = lambda h=hidden, b=btn: self.toggle_show_more(h, b)
                btn.configure(command=btn_cmd)
                btn.pack(fill="x", padx=10)

    def toggle_show_more(self, hidden_widgets, button):
        is_showing = rtl_fix("הצג פחות") == button.cget("text")
        if is_showing:
            for w in hidden_widgets: w.pack_forget()
            button.configure(text=rtl_fix("הצג עוד"))
        else:
            for w in hidden_widgets: w.pack(fill="x", padx=10, pady=2, anchor="e", before=button)
            button.configure(text=rtl_fix("הצג פחות"))

    def on_filter_toggled(self, filter_name, var_state):
        if var_state == "on": self.active_filters.add(filter_name)
        else: self.active_filters.discard(filter_name)
        
        self.update_active_filters_display()
        self.run_in_thread(self.scraper.apply_filter_by_name, self.handle_results, filter_name)

    def filter_checkbox_list(self, event=None):
        term = self.filter_search_entry.get()
        for cb in self.filter_checkboxes:
            if term in self.original_checkbox_text[cb]:
                cb.pack(fill="x", padx=10, pady=2, anchor="e")
            else:
                cb.pack_forget()

    def update_active_filters_display(self):
        for w in self.active_filters_frame.winfo_children(): w.destroy()
        if self.active_filters:
            self.active_filters_frame.grid(row=0, column=0, sticky="ew", padx=5, pady=2)
            ctk.CTkLabel(self.active_filters_frame, text=rtl_fix(":מסננים"), font=ctk.CTkFont(size=11, weight="bold")).pack(side="right", padx=5)
            for fname in self.active_filters:
                ctk.CTkLabel(self.active_filters_frame, text=rtl_fix(fname), fg_color="#e0e0e0", corner_radius=4, padx=5).pack(side="right", padx=2)
        else:
            self.active_filters_frame.grid_remove()

    def go_to_next_page(self):
        self.run_in_thread(self.scraper.navigate_to_next_page, self.handle_results)

    def on_closing(self):
        self.safe_update_status("סוגר...")
        threading.Thread(target=self._close_app, daemon=True).start()
        self.destroy()

    def _close_app(self):
        self.scraper.close_driver()

if __name__ == "__main__":
    setup_logging()
    logger.info(f"--- Application Starting on {platform.system()} ---")
    
    print("מפעיל דפדפן ומתחבר...")
    driver = initial_login(print)
    
    if driver:
        print("ההתחברות הצליחה. מפעיל ממשק גרפי...")
        app = App(driver=driver)
        app.protocol("WM_DELETE_WINDOW", app.on_closing)
        app.mainloop()
    else:
        print("ההתחברות נכשלה. התוכנה תיסגר.")
        logger.error("Initial login failed. Exiting application.")
    
    logger.info("--- Application Closed ---")
