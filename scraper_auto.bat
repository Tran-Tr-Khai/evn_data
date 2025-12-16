@echo off
cd /d "C:\www\server\scrape_evn\"

:: Trỏ thẳng vào file python.exe nằm trong thư mục venv 
"C:\www\server\scrape_evn\venv\Scripts\python.exe" scraper.py >> system_run.log 2>&1

exit