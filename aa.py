
# kol_api.py

import requests
import os
import re
import urllib.parse
from tqdm import tqdm

class KolHalashonAPI:
"""
מחלקה לניהול התקשורת עם ה-API הלא רשמי של קול הלשון.
"""
API_BASE_URL = "https://www2.kolhalashon.com:444/api"
DOWNLOAD_BASE_URL = "https://www.kolhalashon.com/New/Media/PlayShiur.aspx?FileName={file_name}"

def __init__(self, download_folder="KolHaLashon_Shiurim"):
self.download_folder = download_folder
self.headers = {
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
os.makedirs(self.download_folder, exist_ok=True)

def _make_request(self, endpoint):
"""
פונקציית עזר לביצוע בקשות GET ל-API עם טיפול בשגיאות.
"""
url = f"{self.API_BASE_URL}/{endpoint}"
try:
response = requests.get(url, headers=self.headers, timeout=20)
response.raise_for_status() # זורק שגיאה עבור סטטוסים כמו 4xx או 5xx
return response.json()
except requests.exceptions.RequestException as e:
print(f"\nאירעה שגיאת רשת: {e}")
return None
except requests.exceptions.JSONDecodeError:
print("\nשגיאה בפענוח התגובה מהשרת. ייתכן שה-API השתנה או שהכתובת לא נכונה.")
return None

def get_categories(self, parent_id=0):
"""
מחזיר את רשימת הקטגוריות או השיעורים תחת קטגוריית אב מסוימת.
"""
return self._make_request(f"Categories/Website_GetCategories/{parent_id}")

def search_shiurim(self, query, page_size=50):
"""
מחפש שיעורים לפי מחרוזת טקסט.
"""
encoded_query = urllib.parse.quote(query)
return self._make_request(f"Search/WebSite_GetSearchItems/{encoded_query}/-1/1/{page_size}")

@staticmethod
def _clean_filename(filename):
"""
מסיר תווים לא חוקיים משם קובץ.
"""
return re.sub(r'[\\/*?:"<>|]', "", filename)

def download_shiur(self, shiur_data):
"""
מוריד קובץ שיעור בודד עם מד התקדמות.
"""
if 'FileName' not in shiur_data:
print("שגיאה: מידע על קובץ ההורדה לא נמצא.")
return

file_name_from_api = shiur_data['FileName']
download_url = self.DOWNLOAD_BASE_URL.format(file_name=file_name_from_api)

rabbi_name = shiur_data.get('RabbiName', 'UnknownRabbi')
record_title = shiur_data.get('RecordTitle', 'UntitledShiur')
local_filename = self._clean_filename(f"{rabbi_name} - {record_title}.mp3")
file_path = os.path.join(self.download_folder, local_filename)

print(f"\nמתחיל להוריד את: {local_filename}")

try:
with requests.get(download_url, stream=True, timeout=20) as r:
r.raise_for_status()
total_size = int(r.headers.get('content-length', 0))
block_size = 1024 # 1 KB

with tqdm(total=total_size, unit='iB', unit_scale=True, desc="התקדמות") as progress_bar:
with open(file_path, 'wb') as f:
for chunk in r.iter_content(chunk_size=block_size):
progress_bar.update(len(chunk))
f.write(chunk)

if total_size != 0 and progress_bar.n != total_size:
print("שגיאה, ייתכן שההורדה לא הושלמה כראוי.")
else:
print(f"\nההורדה הושלמה בהצלחה!")
print(f"הקובץ נשמר ב: {file_path}")

except requests.exceptions.RequestException as e:
print(f"\nאירעה שגיאה במהלך ההורדה: {e}")
except Exception as e:
print(f"\nאירעה שגיאה לא צפויה: {e}")





# main.py

from kol_api import KolHalashonAPI
import os
import sys

# התקנת ספריות חסרות אם צריך
try:
import requests
import tqdm
except ImportError:
print("ספריות נדרשות (requests, tqdm) חסרות. מנסה להתקין...")
import subprocess
try:
subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "tqdm"])
print("ההתקנה הושלמה. אנא הפעל את התוכנית מחדש.")
except Exception as e:
print(f"ההתקנה נכשלה: {e}")
print("אנא התקן ידנית באמצעות: pip install requests tqdm")
sys.exit()

def clear_screen():
""" מנקה את מסך הטרמינל """
os.system('cls' if os.name == 'nt' else 'clear')

def display_menu_and_get_choice(title, items, name_key='Name'):
"""
פונקציית עזר להצגת תפריט וקבלת בחירה מהמשתמש.
"""
print(f"--- {title} ---")
if not items:
print("לא נמצאו פריטים להצגה.")
return None, None

for i, item in enumerate(items, 1):
print(f"{i}. {item.get(name_key, 'ללא שם')}")
print("-" * (len(title) + 8))

while True:
choice = input("בחר מספר (או הקש '0' לחזור): ")
if choice == '0':
return 'back', None
try:
choice_index = int(choice)
if 1 <= choice_index <= len(items):
return 'selected', items[choice_index - 1]
else:
print("מספר לא חוקי, נסה שוב.")
except ValueError:
print("קלט לא חוקי, אנא הזן מספר.")

def handle_shiurim_selection(api, shiurim):
"""
מטפל בתהליך בחירה והורדה של שיעור מתוך רשימה.
"""
while True:
clear_screen()
action, selected_shiur = display_menu_and_get_choice(
"רשימת שיעורים", shiurim, name_key='RecordTitle'
)

if action == 'back':
return
elif action == 'selected':
api.download_shiur(selected_shiur)
input("\nהקש Enter כדי להמשיך...")
return

def handle_browsing(api):
"""
מנהל את לוגיקת הניווט בקטגוריות.
"""
navigation_stack = [{'Id': 0, 'Name': 'קטגוריות ראשיות'}]

while navigation_stack:
current_category = navigation_stack[-1]
clear_screen()

data = api.get_categories(current_category['Id'])
if not data:
print("לא ניתן היה לטעון את המידע. חוזר אחורה...")
navigation_stack.pop()
input("הקש Enter כדי להמשיך...")
continue
# ה-API מחזיר גם את רשימת השיעורים וגם את רשימת הקטגוריות באותה קריאה
sub_categories = data.get('SubCategories', [])
shiurim = data.get('Shiurim', [])
# אם יש גם קטגוריות וגם שיעורים, נציג קודם את הקטגוריות
if sub_categories:
action, selection = display_menu_and_get_choice(current_category['Name'], sub_categories)
if action == 'back':
navigation_stack.pop()
elif action == 'selected':
navigation_stack.append(selection)
# אם אין קטגוריות אבל יש שיעורים, נציג אותם
elif shiurim:
handle_shiurim_selection(api, shiurim)
# אחרי בחירת שיעור, נחזור אוטומטית אחורה
navigation_stack.pop()
# אם אין כלום, פשוט נחזור
else:
print("בקטגוריה זו אין תת-קטגוריות או שיעורים.")
input("הקש Enter כדי לחזור...")
navigation_stack.pop()

def handle_search(api):
"""
מנהל את לוגיקת החיפוש.
"""
clear_screen()
query = input("הקלד את שם הרב או נושא השיעור לחיפוש: ")
if not query:
return
print("\nמחפש, אנא המתן...")
results = api.search_shiurim(query)

if results:
handle_shiurim_selection(api, results)
else:
print(f"לא נמצאו תוצאות עבור '{query}'.")
input("\nהקש Enter כדי להמשיך...")

def main():
"""
הפונקציה הראשית המנהלת את התפריט הראשי.
"""
api = KolHalashonAPI()

while True:
clear_screen()
print("--- תפריט ראשי - קול הלשון ---")
print("1. דפדוף לפי קטגוריות")
print("2. חיפוש שיעור")
print("3. יציאה")
print("-" * 33)

choice = input("בחר אפשרות: ")

if choice == '1':
handle_browsing(api)
elif choice == '2':
handle_search(api)
elif choice == '3':
print("תודה שהשתמשת. להתראות!")
break
else:
print("בחירה לא חוקית.")
input("הקש Enter כדי להמשיך...")

if __name__ == "__main__":
main()
