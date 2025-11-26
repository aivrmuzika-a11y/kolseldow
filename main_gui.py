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
    try: from win32api import GetLogicalDriveStrings, GetVolumeInformation
    except ImportError: pass
    
def rtl_fix(text):
    return str(text) if text is not None else ""

def setup_logging():
    log_path = Path.home() / 'kol_halashon_app.log'
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(str(log_path), encoding='utf-8'), logging.StreamHandler()])

logger = logging.getLogger(__name__)

class App(ctk.CTk):
    def __init__(self, driver):
        super().__init__()
        ctk.set_appearance_mode("Light")
        self.title("קול הלשון"); self.geometry("1200x750")
        self.grid_columnconfigure(1, weight=1); self.grid_rowconfigure(1, weight=1)
        
        self.scraper = Scraper(driver, self.safe_update_status, self.safe_update_download_progress)
        self.active_filters = set()
        self.filter_checkboxes = {}
        self.download_widgets = {}
        self.create_widgets()
        self.after(100, self.initialize_backend)

    def initialize_backend(self):
        self.start_drive_refresh()
        self.run_in_thread(self.scraper.load_topics_from_file, self.on_topics_loaded)
        self.safe_update_status("מוכן.")

    def create_widgets(self):
        top_frame = ctk.CTkFrame(self, height=50, corner_radius=0)
        top_frame.grid(row=0, column=0, columnspan=2, sticky="ew")
        top_frame.grid_columnconfigure(4, weight=1)
        self.search_entry = ctk.CTkEntry(top_frame, placeholder_text="הזן לחיפוש...", justify="right")
        self.search_entry.grid(row=0, column=4, padx=(10, 5), pady=10, sticky="ew")
        self.search_entry.bind("<Return>", self.start_search)
        self.search_button = ctk.CTkButton(top_frame, text="חיפוש", width=100, command=self.start_search)
        self.search_button.grid(row=0, column=3, padx=5, pady=10)
        
        self.reload_button = ctk.CTkButton(top_frame, text="רענן דף", width=100, command=lambda: self.run_in_thread(self.scraper.refresh_browser_page, self.on_initial_data_loaded))
        self.reload_button.grid(row=0, column=2, padx=5, pady=10)
        self.re_extract_button = ctk.CTkButton(top_frame, text="טען מחדש", width=100, command=lambda: self.run_in_thread(self.scraper.refresh_current_page_content, self.on_initial_data_loaded))
        self.re_extract_button.grid(row=0, column=1, padx=5, pady=10)
        
        self.categories_button = ctk.CTkButton(top_frame, text="קטגוריות", width=120)
        self.categories_button.grid(row=0, column=0, padx=(10, 5), pady=10)
        self.categories_menu = Menu(self.categories_button, tearoff=0)
        self.categories_button.configure(command=lambda: self.categories_menu.tk_popup(self.categories_button.winfo_rootx(), self.categories_button.winfo_rooty() + self.categories_button.winfo_height()))

        left_panel = ctk.CTkTabview(self, width=350)
        left_panel.grid(row=1, column=0, sticky="ns", padx=10, pady=10)
        filters_tab = left_panel.add("מסננים")
        self.downloads_tab = left_panel.add("הורדות")
        filters_tab.grid_rowconfigure(1, weight=1); filters_tab.grid_columnconfigure(0, weight=1)
        self.filter_search_entry = ctk.CTkEntry(filters_tab, placeholder_text="חפש מסנן...", justify="right")
        self.filter_search_entry.grid(row=0, column=0, padx=10, pady=5, sticky="ew")
        self.filter_search_entry.bind("<KeyRelease>", self.filter_checkbox_list)
        self.filters_scroll_frame = ctk.CTkScrollableFrame(filters_tab, label_text="")
        self.filters_scroll_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
        self.downloads_scroll_frame = ctk.CTkScrollableFrame(self.downloads_tab, label_text="הורדות פעילות")
        self.downloads_scroll_frame.pack(expand=True, fill="both", padx=5, pady=5)

        results_outer_frame = ctk.CTkFrame(self)
        results_outer_frame.grid(row=1, column=1, sticky="nsew", padx=(0, 10), pady=10)
        results_outer_frame.grid_rowconfigure(1, weight=1); results_outer_frame.grid_columnconfigure(0, weight=1)
        self.active_filters_frame = ctk.CTkFrame(results_outer_frame, fg_color="transparent")
        self.results_frame = ctk.CTkScrollableFrame(results_outer_frame, label_text="תוצאות")
        self.results_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)

        bottom_frame = ctk.CTkFrame(self); bottom_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=5, padx=10)
        bottom_frame.grid_columnconfigure(1, weight=1)
        self.drive_selector_frame = ctk.CTkFrame(bottom_frame, fg_color="transparent")
        self.drive_selector_frame.grid(row=0, column=0, padx=5)
        ctk.CTkLabel(self.drive_selector_frame, text="שמור ב:").pack(side="right")
        self.setup_drive_selector()
        self.next_page_button = ctk.CTkButton(bottom_frame, text="הבא ->", command=lambda: self.run_in_thread(self.scraper.navigate_to_next_page, self.on_initial_data_loaded))
        self.next_page_button.grid(row=0, column=2, padx=5)
        self.status_bar = ctk.CTkLabel(self, text="", anchor="e", height=25)
        self.status_bar.grid(row=3, column=0, columnspan=2, sticky="ew", padx=10)
        self.progress_bar = ctk.CTkProgressBar(self, orientation="horizontal", mode="indeterminate")

    def setup_drive_selector(self):
        self.drive_option_menu = ctk.CTkOptionMenu(self.drive_selector_frame, values=["..."], command=self.on_drive_selected)
        self.drive_option_menu.pack(side="right", padx=5)
        ctk.CTkButton(self.drive_selector_frame, text="🔄", width=28, height=28, command=self.start_drive_refresh).pack(side="right")

    def start_drive_refresh(self): threading.Thread(target=self.refresh_drives_async, daemon=True).start()
    def refresh_drives_async(self): self.after(0, self.update_drive_menu, *self.get_drives())

    def update_drive_menu(self, drives, drive_map):
        self.drive_map = drive_map
        self.display_to_key_map = {d: d for d in drives}
        self.drive_option_menu.configure(values=drives)
        if drives:
            self.drive_option_menu.set(drives[0])
            self.on_drive_selected(drives[0])

    def on_drive_selected(self, selected_display):
        if path := self.drive_map.get(selected_display):
            self.scraper.set_final_download_path(path)

    def get_drives(self):
        drives, drive_map = [str(Path.home())], {str(Path.home()): str(Path.home())}
        if platform.system() == "Windows":
            try:
                for d in GetLogicalDriveStrings().split('\000')[:-1]:
                    name = GetVolumeInformation(d)[0] or d
                    display = f"{name} ({d})" if name != d else d
                    drives.append(display); drive_map[display] = d
            except Exception as e: logger.error(f"Win drive error: {e}")
        return drives, drive_map

    def run_in_thread(self, target, callback=None, *args, spinner=True):
        if spinner: self.after(0, self.start_loading)
        def target_wrapper():
            try:
                result = target(*args)
                if callback: self.after(0, lambda: callback(result))
            except Exception as e:
                logger.error(f"Thread error: {e}", exc_info=True)
                self.safe_update_status("❌ שגיאה, בדוק לוגים.")
            finally:
                if spinner: self.after(0, self.stop_loading)
        threading.Thread(target=target_wrapper, daemon=True).start()

    def start_loading(self):
        self.progress_bar.grid(row=4, column=0, columnspan=2, sticky="ew", pady=5); self.progress_bar.start()
        self.set_ui_state("disabled")

    def stop_loading(self):
        self.progress_bar.stop(); self.progress_bar.grid_forget()
        self.set_ui_state("normal")

    def set_ui_state(self, state):
        for w in [self.search_button, self.reload_button, self.re_extract_button, self.categories_button, self.next_page_button]:
            w.configure(state=state)

    def safe_update_status(self, msg): self.after(0, lambda: self.status_bar.configure(text=msg))
    def safe_update_download_progress(self, did, prog, stat): self.after(0, self.update_download_widget, did, prog, stat)

    def on_topics_loaded(self, topics):
        if not topics: return
        self.categories_menu.delete(0, 'end')
        for main_cat, sub_cats in topics.items():
            sub_menu = Menu(self.categories_menu, tearoff=0)
            for sub_cat in sub_cats:
                cmd = lambda h=sub_cat['href']: self.run_in_thread(self.scraper.navigate_to_topic_by_href, self.on_initial_data_loaded, h)
                sub_menu.add_command(label=sub_cat['name'], command=cmd)
            self.categories_menu.add_cascade(label=main_cat, menu=sub_menu)

    def start_search(self, event=None):
        if query := self.search_entry.get():
            self.run_in_thread(self.scraper.perform_search, self.on_initial_data_loaded, query)

    def clear_ui(self):
        for w in self.results_frame.winfo_children(): w.destroy()
        for w in self.filters_scroll_frame.winfo_children(): w.destroy()
        self.filter_checkboxes.clear()
        self.next_page_button.configure(state="disabled")
        self.active_filters_frame.grid_forget()

    def on_initial_data_loaded(self, result):
        self.clear_ui()
        if not result or not result.get('data'): return
        
        if result['type'] == 'error':
            ctk.CTkLabel(self.results_frame, text=f"שגיאה: {result['message']}").pack()
        elif result['type'] == 'rav_selection':
            for rav in result['data']:
                cmd = lambda r_id=rav['id']: self.run_in_thread(self.scraper.select_rav_from_results, self.on_initial_data_loaded, r_id)
                ctk.CTkButton(self.results_frame, text=f"{rav['name']} ({rav['count']})", anchor="e").pack(fill="x", padx=5, pady=2)
        elif result['type'] == 'initial_data':
            self.populate_results(result['data']['shiurim'])
            self.populate_filter_placeholders(result['data']['filter_categories'])
            self.run_in_thread(self.scraper.expand_and_get_all_filters, self.on_full_filters_loaded, spinner=False)

    def populate_results(self, shiurim):
        if not shiurim:
            ctk.CTkLabel(self.results_frame, text="לא נמצאו שיעורים.").pack(); return
        self.next_page_button.configure(state="normal")
        for shiur in shiurim:
            frame = ctk.CTkFrame(self.results_frame); frame.pack(fill="x", padx=5, pady=3)
            frame.grid_columnconfigure(0, weight=1)
            details = ctk.CTkFrame(frame, fg_color="transparent")
            details.grid(row=0, column=0, sticky="ew", padx=10, pady=5)
            ctk.CTkLabel(details, text=shiur['title'], font=ctk.CTkFont(weight="bold"), anchor="e").pack(fill="x")
            ctk.CTkLabel(details, text=f"{shiur['rav']} | {shiur['date']}", font=ctk.CTkFont(size=11), anchor="e").pack(fill="x")
            dl_cmd = lambda s_id=shiur['id'], s_title=shiur['title']: self.start_download(s_id, s_title)
            ctk.CTkButton(frame, text="הורדה", width=80, command=dl_cmd).grid(row=0, column=1, padx=10)

    def populate_filter_placeholders(self, categories):
        if not categories:
            ctk.CTkLabel(self.filters_scroll_frame, text="לא נמצאו קטגוריות.").pack(); return
        for cat_name in categories:
            ctk.CTkLabel(self.filters_scroll_frame, text=cat_name, font=ctk.CTkFont(weight="bold"), anchor="e").pack(fill="x", padx=5, pady=(10,0))
            ctk.CTkLabel(self.filters_scroll_frame, text="טוען...", text_color="gray", anchor="e").pack(fill="x", padx=15)

    def on_full_filters_loaded(self, filters_data):
        for w in self.filters_scroll_frame.winfo_children(): w.destroy()
        if not filters_data:
            ctk.CTkLabel(self.filters_scroll_frame, text="לא נמצאו מסננים.").pack(); return
        
        for item in filters_data:
            text, level = item['text'], item['level']
            if level == -1:
                ctk.CTkLabel(self.filters_scroll_frame, text=text, font=ctk.CTkFont(weight="bold"), anchor="e").pack(fill="x", padx=5, pady=(10,2))
            else:
                var = ctk.StringVar(value="on" if text in self.active_filters else "off")
                cmd = lambda n=text, v=var: self.on_filter_toggled(n, v.get())
                cb = ctk.CTkCheckBox(self.filters_scroll_frame, text=text, variable=var, onvalue="on", offvalue="off", command=cmd)
                indent = 10 + (level * 20)
                cb.pack(fill="x", padx=(10, indent), anchor="e")
                self.filter_checkboxes[text] = cb

    def on_filter_toggled(self, name, state):
        if state == "on": self.active_filters.add(name)
        else: self.active_filters.discard(name)
        self.update_active_filters_display()
        self.run_in_thread(self.scraper.apply_filter_by_name, self.on_initial_data_loaded, name)

    def filter_checkbox_list(self, event=None):
        term = self.filter_search_entry.get()
        for name, cb in self.filter_checkboxes.items():
            if term in name: cb.pack(fill="x", padx=cb.cget("padx"), anchor="e")
            else: cb.pack_forget()

    def update_active_filters_display(self):
        for w in self.active_filters_frame.winfo_children(): w.destroy()
        if self.active_filters:
            self.active_filters_frame.grid(row=0, column=0, sticky="ew", padx=5, pady=(0,5))
            ctk.CTkLabel(self.active_filters_frame, text=":מסננים", font=ctk.CTkFont(weight="bold")).pack(side="right")
            for fname in sorted(list(self.active_filters)):
                ctk.CTkLabel(self.active_filters_frame, text=fname, fg_color="#e0e0e0", corner_radius=4).pack(side="right", padx=2)
        else:
            self.active_filters_frame.grid_forget()

    def start_download(self, shiur_id, title):
        did = f"{shiur_id}_{int(time.time())}"
        frame = ctk.CTkFrame(self.downloads_scroll_frame); frame.pack(fill="x", padx=5, pady=2)
        label_text = title[:35] + ("..." if len(title) > 35 else "")
        label = ctk.CTkLabel(frame, text=label_text, anchor="e")
        label.pack(fill="x", expand=True, side="right", padx=5)
        progress = ctk.CTkProgressBar(frame, orientation="horizontal", width=100)
        progress.pack(side="left", padx=5)
        self.download_widgets[did] = {'progress': progress, 'label': label}
        
        self.scraper.queue_download(shiur_id, title, did)
        self.downloads_tab.master.set("הורדות")

    def update_download_widget(self, did, prog, status):
        if did in self.download_widgets:
            widgets = self.download_widgets[did]
            if status == "starting":
                widgets['progress'].configure(mode="indeterminate"); widgets['progress'].start()
            elif status == "completed":
                widgets['progress'].stop(); widgets['progress'].configure(mode="determinate", progress_color="green"); widgets['progress'].set(1)
                widgets['label'].configure(text=f"✅ {widgets['label'].cget('text')}")
            elif status == "failed":
                widgets['progress'].stop(); widgets['progress'].configure(mode="determinate", progress_color="red"); widgets['progress'].set(1)
                widgets['label'].configure(text=f"❌ {widgets['label'].cget('text')}")

    def on_closing(self):
        self.safe_update_status("סוגר..."); self.scraper.close_driver(); self.destroy()

if __name__ == "__main__":
    setup_logging()
    logger.info(f"--- Application Starting on {platform.system()} ---")
    driver = initial_login(print)
    if driver:
        app = App(driver=driver)
        app.protocol("WM_DELETE_WINDOW", app.on_closing)
        app.mainloop()
    else:
        print("ההתחברות נכשלה. התוכנה תיסגר.")
    logger.info("--- Application Closed ---")
