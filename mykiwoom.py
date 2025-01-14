import os
import sys
import time
import subprocess
from utility.static import now, strf_time
from utility.setting import system_path

os.system(f'python {system_path}/login/versionupdater.py')
time.sleep(5)

os.system(f'python {system_path}/login/autologin2.py')
time.sleep(5)

subprocess.Popen(f'python {system_path}/collector/window.py')
time.sleep(30)

os.system(f'python {system_path}/login/autologin1.py')
time.sleep(5)

os.system(f'python {system_path}/trader/window.py')
time.sleep(5)

if int(strf_time('%H%M%S')) < 100000:
    os.system('shutdown /s /t 60')
    sys.exit()

if now().weekday() == 5:
    os.system(f'python {system_path}/collector/download_daydata.py')
    time.sleep(5)

os.system(f'python {system_path}/updater/updater_short.py')
time.sleep(5)

os.system(f'python {system_path}/backtester/backtester_tick.py')
time.sleep(5)

os.system('shutdown /s /t 60')
